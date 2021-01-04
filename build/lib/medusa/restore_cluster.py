# -*- coding: utf-8 -*-
# Copyright 2020- Datastax, Inc. All rights reserved.
# Copyright 2019 Spotify AB. All rights reserved.
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

import collections
import logging
import sys
import uuid
import datetime
import traceback
import socket

import medusa.config
import medusa.utils
from medusa.monitoring import Monitoring
from medusa.cassandra_utils import CqlSessionProvider, Cassandra
from medusa.orchestration import Orchestration
from medusa.schema import parse_schema
from medusa.storage import Storage
from medusa.verify_restore import verify_restore
from medusa.network.hostname_resolver import HostnameResolver


def orchestrate(config, backup_name, seed_target, temp_dir, host_list, keep_auth, bypass_checks,
                verify, keyspaces, tables, parallel_restores, use_sstableloader=False):
    monitoring = Monitoring(config=config.monitoring)
    try:
        restore_start_time = datetime.datetime.now()
        if seed_target is None and host_list is None:
            # if no target node is provided, nor a host list file, default to the local node as seed target
            hostname_resolver = HostnameResolver(medusa.utils.evaluate_boolean(config.cassandra.resolve_ip_addresses))
            seed_target = hostname_resolver.resolve_fqdn(socket.gethostbyname(socket.getfqdn()))
            logging.warning("Seed target was not provided, using the local hostname: {}".format(seed_target))

        if seed_target is not None and host_list is not None:
            err_msg = 'You must either provide a seed target or a list of host, not both'
            logging.error(err_msg)
            raise Exception(err_msg)

        if not temp_dir.is_dir():
            err_msg = '{} is not a directory'.format(temp_dir)
            logging.error(err_msg)
            raise Exception(err_msg)

        storage = Storage(config=config.storage)

        try:
            cluster_backup = storage.get_cluster_backup(backup_name)
        except KeyError:
            err_msg = 'No such backup --> {}'.format(backup_name)
            logging.error(err_msg)
            raise Exception(err_msg)

        restore = RestoreJob(cluster_backup, config, temp_dir, host_list, seed_target, keep_auth, verify,
                             parallel_restores, keyspaces, tables, bypass_checks, use_sstableloader)
        restore.execute()

        restore_end_time = datetime.datetime.now()
        restore_duration = restore_end_time - restore_start_time

        logging.debug('Emitting metrics')

        logging.info('Restore duration: {}'.format(restore_duration.seconds))
        tags = ['medusa-cluster-restore', 'restore-duration', backup_name]
        monitoring.send(tags, restore_duration.seconds)

        tags = ['medusa-cluster-restore', 'restore-error', backup_name]
        monitoring.send(tags, 0)

        logging.debug('Done emitting metrics')
        logging.info('Successfully restored the cluster')

    except Exception as e:
        tags = ['medusa-cluster-restore', 'restore-error', backup_name]
        monitoring.send(tags, 1)

        logging.error('This error happened during the cluster restore: {}'.format(str(e)))
        traceback.print_exc()
        sys.exit(1)


def expand_repeatable_option(option, values):
    return ' '.join(['--{} {}'.format(option, value) for value in values])


class RestoreJob(object):
    def __init__(self, cluster_backup, config, temp_dir, host_list, seed_target, keep_auth, verify,
                 parallel_restores, keyspaces={}, tables={}, bypass_checks=False, use_sstableloader=False):
        self.id = uuid.uuid4()
        self.ringmap = None
        self.cluster_backup = cluster_backup
        self.session_provider = None
        self.orchestration = Orchestration(config, parallel_restores)
        self.config = config
        self.host_list = host_list
        self.seed_target = seed_target
        self.keep_auth = keep_auth
        self.verify = verify
        self.in_place = None
        self.temp_dir = temp_dir  # temporary files
        self.work_dir = self.temp_dir / 'medusa-job-{id}'.format(id=self.id)
        self.host_map = {}  # Map of backup host/target host for the restore process
        self.keyspaces = keyspaces
        self.tables = tables
        self.bypass_checks = bypass_checks
        self.use_sstableloader = use_sstableloader
        self.pssh_pool_size = parallel_restores
        self.cassandra = Cassandra(config)
        fqdn_resolver = medusa.utils.evaluate_boolean(self.config.cassandra.resolve_ip_addresses)
        self.fqdn_resolver = HostnameResolver(fqdn_resolver)

    def execute(self):
        logging.info('Ensuring the backup is found and is complete')
        if not self.cluster_backup.is_complete():
            raise Exception('Backup is not complete')

        # CASE 1 : We're restoring using a seed target. Source/target mapping will be built based on tokenmap.
        if self.seed_target is not None:
            self.session_provider = CqlSessionProvider([self.seed_target],
                                                       self.config.cassandra)

            with self.session_provider.new_session() as session:
                self._populate_ringmap(self.cluster_backup.tokenmap, session.tokenmap())

        # CASE 2 : We're restoring a backup on a different cluster
        if self.host_list is not None:
            logging.info('Restore will happen on new hardware')
            self.in_place = False
            self._populate_hostmap()
            logging.info('Starting Restore on all the nodes in this list: {}'.format(self.host_list))

        self._restore_data()

    def _validate_ringmap(self, tokenmap, target_tokenmap):
        for host, ring_item in target_tokenmap.items():
            if not ring_item.get('is_up'):
                raise Exception('Target {host} is not up!'.format(host=host))
        if len(target_tokenmap) != len(tokenmap):
            return False
        return True

    def _populate_ringmap(self, tokenmap, target_tokenmap):

        def _tokens_from_ringitem(ringitem):
            return ','.join(map(str, ringitem['tokens']))

        def _token_counts_per_host(tokenmap):
            for host, ringitem in tokenmap.items():
                return len(ringitem['tokens'])

        def _hosts_from_tokenmap(tokenmap):
            hosts = set()
            for host, ringitem in tokenmap.items():
                hosts.add(host)
            return hosts

        def _chunk(my_list, nb_chunks):
            groups = []
            for i in range(nb_chunks):
                groups.append([])
            for i in range(len(my_list)):
                groups[i % nb_chunks].append(my_list[i])
            return groups

        target_tokens = {}
        backup_tokens = {}
        topology_matches = self._validate_ringmap(tokenmap, target_tokenmap)
        self.in_place = self._is_restore_in_place(tokenmap, target_tokenmap)
        if self.in_place:
            logging.info("Restoring on the same cluster that was the backup was taken on (in place fashion)")
            self.keep_auth = False
        else:
            logging.info("Restoring on a different cluster than the backup one (remote fashion)")
            if self.keep_auth:
                logging.info('system_auth keyspace will be left untouched on the target nodes')
            else:
                # ops might not be aware of the underlying behavior towards auth. Let's ask what to do...
                really_keep_auth = None
                while (really_keep_auth != 'Y' and really_keep_auth != 'n') and not self.bypass_checks:
                    really_keep_auth = input('Do you want to skip restoring the system_auth keyspace and keep the'
                                             + ' credentials of the target cluster? (Y/n)')
                self.keep_auth = True if really_keep_auth == 'Y' else False

        if topology_matches:
            target_tokens = {_tokens_from_ringitem(ringitem): host for host, ringitem in target_tokenmap.items()}
            backup_tokens = {_tokens_from_ringitem(ringitem): host for host, ringitem in tokenmap.items()}

            target_tokens_per_host = _token_counts_per_host(tokenmap)
            backup_tokens_per_host = _token_counts_per_host(target_tokenmap)

            # we must have the same number of tokens per host in both vnode and normal clusters
            if target_tokens_per_host != backup_tokens_per_host:
                logging.info('Source/target rings have different number of tokens per node: {}/{}'.format(
                    backup_tokens_per_host,
                    target_tokens_per_host
                ))
                topology_matches = False

            # if not using vnodes, the tokens must match exactly
            if backup_tokens_per_host == 1 and target_tokens.keys() != backup_tokens.keys():
                extras = target_tokens.keys() ^ backup_tokens.keys()
                logging.info('Tokenmap is differently distributed. Extra items: {}'.format(extras))
                topology_matches = False

        if topology_matches:
            # We can associate each restore node with exactly one backup node
            backup_ringmap = collections.defaultdict(list)
            target_ringmap = collections.defaultdict(list)
            for token, host in backup_tokens.items():
                backup_ringmap[token].append(host)
            for token, host in target_tokens.items():
                target_ringmap[token].append(host)

            self.ringmap = backup_ringmap
            i = 0
            for token, hosts in backup_ringmap.items():
                # take the node that has the same token list or pick the one with the same position in the map.
                restore_host = target_ringmap.get(token, list(target_ringmap.values())[i])[0]
                isSeed = True if self.fqdn_resolver.resolve_fqdn(restore_host) in self._get_seeds_fqdn() else False
                self.host_map[restore_host] = {'source': [hosts[0]], 'seed': isSeed}
                i += 1
        else:
            # Topologies are different between backup and restore clusters. Using the sstableloader for restore.
            self.use_sstableloader = True
            backup_hosts = _hosts_from_tokenmap(tokenmap)
            restore_hosts = list(_hosts_from_tokenmap(target_tokenmap))
            if len(backup_hosts) >= len(restore_hosts):
                grouped_backups = _chunk(list(backup_hosts), len(restore_hosts))
            else:
                grouped_backups = _chunk(list(backup_hosts), len(backup_hosts))
            for i in range(min([len(grouped_backups), len(restore_hosts)])):
                # associate one restore host with several backups as we don't have the same number of nodes.
                self.host_map[restore_hosts[i]] = {'source': grouped_backups[i], 'seed': False}

    def _is_restore_in_place(self, backup_tokenmap, target_tokenmap):
        # If at least one node is part of both tokenmaps, then we're restoring in place
        # Otherwise we're restoring a remote cluster
        return len(set(backup_tokenmap.keys()) & set(target_tokenmap.keys())) > 0

    def _get_seeds_fqdn(self):
        seeds = list()
        for seed in self.cassandra.seeds:
            seeds.append(self.fqdn_resolver.resolve_fqdn(seed))
        return seeds

    def _populate_hostmap(self):
        with open(self.host_list, 'r') as f:
            for line in f.readlines():
                seed, target, source = line.replace('\n', '').split(self.config.storage.host_file_separator)
                # in python, bool('False') evaluates to True. Need to test the membership as below
                self.host_map[self.fqdn_resolver.resolve_fqdn(target.strip())] \
                    = {'source': [self.fqdn_resolver.resolve_fqdn(source.strip())], 'seed': seed in ['True']}

    def _restore_data(self):
        # create workdir on each target host
        # Later: distribute a credential
        # construct command for each target host
        # invoke `nohup medusa-wrapper #{command}` on each target host
        # wait for exit on each
        logging.info('Starting cluster restore...')
        logging.info('Working directory for this execution: {}'.format(self.work_dir))
        for target, sources in self.host_map.items():
            logging.info('About to restore on {} using {} as backup source'.format(target, sources))

        logging.info('This will delete all data on the target nodes and replace it with backup {}.'
                     .format(self.cluster_backup.name))

        proceed = None
        while (proceed != 'Y' and proceed != 'n') and not self.bypass_checks:
            proceed = input('Are you sure you want to proceed? (Y/n)')

        if proceed == 'n':
            err_msg = 'Restore manually cancelled'
            logging.error(err_msg)
            raise Exception(err_msg)

        # work out which nodes are seeds in the target cluster
        target_seeds = [t for t, s in self.host_map.items() if s['seed']]
        logging.info("target seeds : {}".format(target_seeds))
        # work out which nodes are seeds in the target cluster
        target_hosts = [host for host in self.host_map.keys()]
        logging.info("target hosts : {}".format(target_hosts))

        if self.use_sstableloader is False:
            # stop all target nodes
            logging.info('Stopping Cassandra on all nodes currently up')

            # Generate a Job ID for this run
            job_id = str(uuid.uuid4())
            logging.debug('Job id is: {}'.format(job_id))
            # Define command to run
            command = self.config.cassandra.stop_cmd
            logging.debug('Command to run is: {}'.format(command))

            self.orchestration.pssh_run(target_hosts, command, hosts_variables={})

        else:
            # we're using the sstableloader, which will require to (re)create the schema and empty the tables
            logging.info("Restoring schema on the target cluster")
            self._restore_schema()

        # trigger restores everywhere at once
        # pass in seed info so that non-seeds can wait for seeds before starting
        # seeds, naturally, don't wait for anything

        # Generate a Job ID for this run
        hosts_variables = []
        for target, source in [(t, s['source']) for t, s in self.host_map.items()]:
            logging.info('Restoring data on {}...'.format(target))
            seeds = '' if target in target_seeds or len(target_seeds) == 0 \
                    else '--seeds {}'.format(','.join(target_seeds))
            hosts_variables.append((','.join(source), seeds))

        command = self._build_restore_cmd()
        pssh_run_success = self.orchestration.pssh_run(target_hosts,
                                                       command,
                                                       hosts_variables=hosts_variables)

        if not pssh_run_success:
            # we could implement a retry.
            err_msg = 'Some nodes failed to restore. Exiting'
            logging.error(err_msg)
            raise Exception(err_msg)

        logging.info('Restore process is complete. The cluster should be up shortly.')

        if self.verify:
            verify_restore(target_hosts, self.config)

    def _build_restore_cmd(self):
        in_place_option = '--in-place' if self.in_place else '--remote'
        keep_auth_option = '--keep-auth' if self.keep_auth else ''
        keyspace_options = expand_repeatable_option('keyspace', self.keyspaces)
        table_options = expand_repeatable_option('table', self.tables)
        # We explicitly set --no-verify since we are doing verification here in this module
        # from the control node
        verify_option = '--no-verify'

        # %s placeholders in the below command will get replaced by pssh using per host command substitution
        command = 'mkdir -p {work}; cd {work} && medusa-wrapper sudo medusa --fqdn=%s -vvv restore-node ' \
                  '{in_place} {keep_auth} %s {verify} --backup-name {backup} --temp-dir {temp_dir} ' \
                  '{use_sstableloader} {keyspaces} {tables}' \
            .format(work=self.work_dir,
                    in_place=in_place_option,
                    keep_auth=keep_auth_option,
                    verify=verify_option,
                    backup=self.cluster_backup.name,
                    temp_dir=self.temp_dir,
                    use_sstableloader='--use-sstableloader' if self.use_sstableloader is True else '',
                    keyspaces=keyspace_options,
                    tables=table_options)

        logging.debug('Preparing to restore on all nodes with the following command {}'.format(command))

        return command

    def _restore_schema(self):
        schema = parse_schema(self.cluster_backup.schema)
        with self.session_provider.new_session() as session:
            for keyspace in schema.keys():
                if keyspace.startswith("system"):
                    continue
                else:
                    self._create_or_recreate_schema_objects(session, keyspace, schema[keyspace])

    def _create_or_recreate_schema_objects(self, session, keyspace, keyspace_schema):
        logging.info("(Re)creating schema for keyspace {}".format(keyspace))
        if (keyspace not in session.cluster.metadata.keyspaces):
            # Keyspace doesn't exist on the target cluster. Got to create it and all the tables as well.
            session.execute(keyspace_schema['create_statement'])
        for mv in keyspace_schema['materialized_views']:
            # MVs need to be dropped before we drop the tables
            logging.debug("Dropping MV {}.{}".format(keyspace, mv[0]))
            session.execute("DROP MATERIALIZED VIEW IF EXISTS {}.{}".format(keyspace, mv[0]))
        for table in keyspace_schema['tables'].items():
            logging.debug("Dropping table {}.{}".format(keyspace, table[0]))
            session.execute("DROP TABLE IF EXISTS {}.{}".format(keyspace, table[0]))
        for udt in keyspace_schema['udt'].items():
            # then custom types as they can be used in tables
            session.execute("DROP TYPE IF EXISTS {}.{}".format(keyspace, udt[0]))
            # Then we create the missing ones
            session.execute(udt[1])
        for table in keyspace_schema['tables'].items():
            logging.debug("Creating table {}.{}".format(keyspace, table[0]))
            # Create the tables
            session.execute(table[1])
        for index in keyspace_schema['indices'].items():
            # indices were dropped with their base tables
            logging.debug("Creating index {}.{}".format(keyspace, index[0]))
            session.execute(index[1])
        for mv in keyspace_schema['materialized_views']:
            # Base tables are created now, we can create the MVs
            logging.debug("Creating MV {}.{}".format(keyspace, mv[0]))
            session.execute(mv[1])