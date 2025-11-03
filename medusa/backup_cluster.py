# -*- coding: utf-8 -*-
# Copyright 2020- Datastax, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import sys
import uuid
import datetime
import traceback
from typing import Optional
from dataclasses import dataclass

import medusa.config
import medusa.utils
from medusa.orchestration import Orchestration
from medusa.monitoring import Monitoring
from medusa.cassandra_utils import CqlSessionProvider, Cassandra
from medusa.storage import Storage
from medusa.network.hostname_resolver import HostnameResolver


@dataclass
class OrchestrationConfig:
    parallel_snapshots: int
    parallel_uploads: int
    orchestration_snapshots: Optional[Orchestration] = None
    orchestration_uploads: Optional[Orchestration] = None
    keep_snapshot: bool = False
    use_existing_snapshot: bool = False


def orchestrate(config, backup_name_arg, seed_target, stagger, enable_md5_checks, mode, temp_dir,
                orchestration_config: OrchestrationConfig,
                cassandra_config=None, monitoring=None, existing_storage=None, cql_session_provider=None):
    backup = None
    backup_name = backup_name_arg
    monitoring = Monitoring(config=config.monitoring) if monitoring is None else monitoring

    if existing_storage is None:
        storage = Storage(config=config.storage)
    else:
        storage = existing_storage

    with storage as storage:

        try:
            backup_start_time = datetime.datetime.now()
            if not config.storage.fqdn:
                err_msg = "The fqdn was not provided nor calculated properly."
                logging.error(err_msg)
                raise ValueError(err_msg)

            if not temp_dir.is_dir():
                err_msg = '{} is not a directory'.format(temp_dir)
                logging.error(err_msg)
                raise ValueError(err_msg)

            try:
                # Try to get a backup with backup_name. If it exists then we cannot take another backup with that name
                cluster_backup = storage.get_cluster_backup(backup_name)
                if cluster_backup and cluster_backup.name == backup_name:
                    err_msg = 'Backup named {} already exists.'.format(backup_name)
                    logging.error(err_msg)
                    raise ValueError(err_msg)
            except KeyError:
                info_msg = 'Starting backup {}'.format(backup_name)
                logging.info(info_msg)

            backup = BackupJob(config, backup_name, seed_target, stagger, enable_md5_checks, mode, temp_dir,
                               orchestration_config, cassandra_config)
            backup.execute(cql_session_provider)

            backup_end_time = datetime.datetime.now()
            backup_duration = backup_end_time - backup_start_time

            logging.debug('Emitting metrics')

            logging.info('Backup duration: {}'.format(backup_duration.total_seconds()))
            tags = ['medusa-cluster-backup', 'cluster-backup-duration', backup_name]
            monitoring.send(tags, backup_duration.total_seconds())

            tags = ['medusa-cluster-backup', 'cluster-backup-error', backup_name]
            monitoring.send(tags, 0)

            logging.debug('Done emitting metrics.')
            logging.info('Backup of the cluster done.')

        except Exception as e:
            tags = ['medusa-cluster-backup', 'cluster-backup-error', backup_name]
            monitoring.send(tags, 1)

            logging.error('This error happened during the cluster backup: {}'.format(str(e)))
            traceback.print_exc()

            if backup is not None:
                err_msg = 'Something went wrong! Attempting to clean snapshots and exit.'
                logging.error(err_msg)

                delete_snapshot_command = ' '.join(backup.cassandra.delete_snapshot_command(backup.snapshot_tag))
                pssh_run_success_cleanup = backup.orchestration_uploads\
                    .pssh_run(backup.hosts,
                              delete_snapshot_command,
                              hosts_variables={})
                if pssh_run_success_cleanup:
                    info_msg = 'All nodes successfully cleared their snapshot.'
                    logging.info(info_msg)
                else:
                    err_msg_cleanup = 'Some nodes failed to clear the snapshot. Please clean snapshots manually'
                    logging.error(err_msg_cleanup)
            sys.exit(1)


class BackupJob(object):
    def __init__(self, config, backup_name, seed_target, stagger, enable_md5_checks, mode, temp_dir,
                 orchestration_config: OrchestrationConfig, cassandra_config=None):
        self.id = uuid.uuid4()
        self.orchestration_snapshots = (
            Orchestration(config, orchestration_config.parallel_snapshots)
            if orchestration_config.orchestration_snapshots is None
            else orchestration_config.orchestration_snapshots
        )
        self.orchestration_uploads = (
            Orchestration(config, orchestration_config.parallel_uploads)
            if orchestration_config.orchestration_uploads is None
            else orchestration_config.orchestration_uploads
        )
        self.config = config
        self.backup_name = backup_name
        self.stagger = stagger
        self.seed_target = seed_target
        self.enable_md5_checks = enable_md5_checks
        self.mode = mode
        self.temp_dir = temp_dir
        self.keep_snapshot = orchestration_config.keep_snapshot
        self.use_existing_snapshot = orchestration_config.use_existing_snapshot
        self.work_dir = self.temp_dir / 'medusa-job-{id}'.format(id=self.id)
        self.hosts = {}
        self.cassandra = Cassandra(config) if cassandra_config is None else cassandra_config
        self.snapshot_tag = '{}{}'.format(self.cassandra.SNAPSHOT_PREFIX, self.backup_name)
        fqdn_resolver = medusa.utils.evaluate_boolean(self.config.cassandra.resolve_ip_addresses)
        k8s_mode = medusa.utils.evaluate_boolean(config.kubernetes.enabled if config.kubernetes else False)
        self.fqdn_resolver = HostnameResolver(fqdn_resolver, k8s_mode)

    def execute(self, cql_session_provider=None):
        # Two step: Take snapshot everywhere, then upload the backups to the external storage

        # Getting the list of Cassandra nodes.
        seed_target = self.seed_target if self.seed_target is not None else self.config.storage.fqdn
        session_provider = CqlSessionProvider([seed_target],
                                              self.config) \
            if cql_session_provider is None else cql_session_provider
        with session_provider.new_session() as session:
            tokenmap = session.tokenmap()
            self.hosts = list(tokenmap.keys())

        # First let's take a snapshot on all nodes at once, unless we're using an existing one
        # Here we will use parallelism of min(number of nodes, parallel_snapshots)
        if not self.use_existing_snapshot:
            logging.info('Creating snapshots on all nodes')
            self._create_snapshots()
        else:
            logging.info('Using existing snapshots on all nodes')

        # Second
        logging.info('Uploading snapshots from nodes to external storage')
        self._upload_backup()

    def _create_snapshots(self):
        # Run snapshot in parallel on all nodes,
        create_snapshot_command = ' '.join(self.cassandra.create_snapshot_command(self.backup_name))
        pssh_run_success = self.orchestration_snapshots.\
            pssh_run(self.hosts,
                     create_snapshot_command,
                     hosts_variables={})
        if not pssh_run_success:
            # we could implement a retry.
            err_msg = 'Some nodes failed to create the snapshot.'
            logging.error(err_msg)
            raise RuntimeError(err_msg)

        logging.info('A snapshot {} was created on all nodes.'.format(self.snapshot_tag))

    def _upload_backup(self):
        backup_command = self._build_backup_cmd()
        # Run upload in parallel or sequentially according to parallel_uploads defined by the user
        pssh_run_success = self.orchestration_uploads.pssh_run(self.hosts,
                                                               backup_command,
                                                               hosts_variables={})
        if not pssh_run_success:
            # we could implement a retry.
            err_msg = 'Some nodes failed to upload the backup.'
            logging.error(err_msg)
            raise RuntimeError(err_msg)

        logging.info('A new backup {} was created on all nodes.'.format(self.backup_name))

    def _build_backup_cmd(self):
        stagger_option = '--in-stagger {}'.format(self.stagger) if self.stagger else ''
        enable_md5_checks_option = '--enable-md5-checks' if self.enable_md5_checks else ''
        keep_snapshot_option = '--keep-snapshot' if self.keep_snapshot else ''
        use_existing_snapshot_option = '--use-existing-snapshot' if self.use_existing_snapshot else ''

        # Use %s placeholders in the below command to have them replaced by pssh using per host command substitution
        command = 'mkdir -p {work}; cd {work} && medusa-wrapper {sudo} medusa {config} -vvv backup-node ' \
                  '--backup-name {backup_name} {stagger} {enable_md5_checks} --mode {mode} ' \
                  '{keep_snapshot} {use_existing_snapshot}' \
            .format(work=self.work_dir,
                    sudo='sudo' if medusa.utils.evaluate_boolean(self.config.cassandra.use_sudo) else '',
                    config=f'--config-file {self.config.file_path}' if self.config.file_path else '',
                    backup_name=self.backup_name,
                    stagger=stagger_option,
                    enable_md5_checks=enable_md5_checks_option,
                    mode=self.mode,
                    keep_snapshot=keep_snapshot_option,
                    use_existing_snapshot=use_existing_snapshot_option)

        logging.debug('Running backup on all nodes with the following command {}'.format(command))

        return command
