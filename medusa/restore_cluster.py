# -*- coding: utf-8 -*-
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

from pssh.clients.miko import ParallelSSHClient

import collections
import logging
import sys
import uuid
import datetime
import traceback
import paramiko

from medusa.monitoring import Monitoring
from medusa.cassandra_utils import CqlSessionProvider
from medusa.schema import parse_schema
from medusa.storage import Storage
from medusa.verify_restore import verify_restore


def orchestrate(config, backup_name, seed_target, temp_dir, host_list, keep_auth, bypass_checks,
                verify, keyspaces, tables, pssh_pool_size, use_sstableloader=False):
    monitoring = Monitoring(config=config.monitoring)
    try:
        restore_start_time = datetime.datetime.now()
        if seed_target is not None:
            keep_auth = False

        if seed_target is None and host_list is None:
            err_msg = 'You must either provide a seed target or a list of host'
            logging.error(err_msg)
            raise Exception(err_msg)

        if seed_target is not None and host_list is not None:
            err_msg = 'You must either provide a seed target or a list of host, not both'
            logging.error(err_msg)
            raise Exception(err_msg)

        if not temp_dir.is_dir():
            err_msg = '{} is not a directory'.format(temp_dir)
            logging.error(err_msg)
            raise Exception(err_msg)

        if keep_auth:
            logging.info('system_auth keyspace will be left untouched on the target nodes')
        else:
            logging.info('system_auth keyspace will be overwritten with the backup on target nodes')

        storage = Storage(config=config.storage)

        try:
            cluster_backup = storage.get_cluster_backup(backup_name)
        except KeyError:
            err_msg = 'No such backup --> {}'.format(backup_name)
            logging.error(err_msg)
            raise Exception(err_msg)

        restore = RestoreJob(cluster_backup, config, temp_dir, host_list, seed_target, keep_auth, verify,
                             pssh_pool_size, keyspaces, tables, bypass_checks, use_sstableloader)
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
                 pssh_pool_size, keyspaces={}, tables={}, bypass_checks=False, use_sstableloader=False):
        self.id = uuid.uuid4()
        self.ringmap = None
        self.cluster_backup = cluster_backup
        self.session_provider = None
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
        self.pssh_pool_size = pssh_pool_size

    def execute(self):
        logging.info('Ensuring the backup is found and is complete')
        if not self.cluster_backup.is_complete():
            raise Exception('Backup is not complete')

        # CASE 1 : We're restoring in place and a seed target has been provided
        if self.seed_target is not None:
            logging.info('Restore will happen "In-Place", no new hardware is involved')
            self.in_place = True
            self.session_provider = CqlSessionProvider([self.seed_target],
                                                       username=self.config.cassandra.cql_username,
                                                       password=self.config.cassandra.cql_password)

            with self.session_provider.new_session() as session:
                self._populate_ringmap(self.cluster_backup.tokenmap, session.tokenmap())

        # CASE 2 : We're restoring out of place, i.e. doing a restore test
        if self.host_list is not None:
            logging.info('Restore will happen on new hardware')
            self.in_place = False
            self._populate_hostmap()
            logging.info('Starting Restore on all the nodes in this list: {}'.format(self.host_list))

        self._restore_data()

    def _pssh_run(self, hosts, command, hosts_variables=None):
        """
        Runs a command on hosts list using cstar under the hood
        There is no return made, to check the result there is a distinct function
        Return: True (success) or False (error)
        """
        pssh_run_success = False
        username = self.config.ssh.username if self.config.ssh.username != '' else None
        port = self.config.ssh.port
        pkey = None
        if self.config.ssh.key_file is not None and self.config.ssh.key_file != '':
            pkey = paramiko.RSAKey.from_private_key_file(self.config.ssh.key_file, None)

        client = ParallelSSHClient(hosts,
                                   forward_ssh_agent=True,
                                   pool_size=self.pssh_pool_size,
                                   user=username,
                                   port=port,
                                   pkey=pkey)
        logging.info('Executing "{}" on all nodes.'
                     .format(command))
        output = client.run_command(command, host_args=hosts_variables, sudo=True)
        client.join(output)

        success = list(filter(lambda host_output: host_output.exit_code == 0,
                       list(map(lambda host_output: host_output[1], output.items()))))
        error = list(filter(lambda host_output: host_output.exit_code != 0,
                     list(map(lambda host_output: host_output[1], output.items()))))

        # Report on execution status
        if len(success) == len(hosts):
            logging.info('Job executing "{}" ran and finished Successfully on all nodes.'
                         .format(command))
            pssh_run_success = True
        elif len(error) > 0:
            logging.info('Job executing "{}" ran and finished with errors on following nodes: {}'
                         .format(command, sorted(set(map(lambda host_output: host_output.host, error)))))
            self.display_output(error)
        else:
            err_msg = 'Something unexpected happened while running pssh command'
            logging.error(err_msg)
            raise Exception(err_msg)

        return pssh_run_success

    def display_output(self, host_outputs):
        for host_out in host_outputs:
            for line in host_out.stdout:
                logging.info("{}-stdout: {}".format(host_out.host, line))
            for line in host_out.stderr:
                logging.info("{}-stderr: {}".format(host_out.host, line))

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
                yield len(ringitem['tokens'])

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

        topology_matches = self._validate_ringmap(tokenmap, target_tokenmap)

        if topology_matches:
            target_tokens = {_tokens_from_ringitem(ringitem): host for host, ringitem in target_tokenmap.items()}
            backup_tokens = {_tokens_from_ringitem(ringitem): host for host, ringitem in tokenmap.items()}

            target_tokens_per_host = set(_token_counts_per_host(tokenmap))
            backup_tokens_per_host = set(_token_counts_per_host(target_tokenmap))

            # we must have the same number of tokens per host in both vnode and normal clusters
            if target_tokens_per_host != backup_tokens_per_host:
                logging.info('Source/target rings have different number of tokens per node: {}/{}'.format(
                    backup_tokens_per_host,
                    target_tokens_per_host
                ))
                topology_matches = False

            # if not using vnodes, the tokens must match exactly
            if len(backup_tokens_per_host) == 1 and target_tokens.keys() != backup_tokens.keys():
                extras = target_tokens.keys() ^ backup_tokens.keys()
                logging.info('Tokenmap is differently distributed. Extra items: {}'.format(extras))
                topology_matches = False

        if topology_matches:
            # We can associate each restore node with exactly one backup node
            ringmap = collections.defaultdict(list)
            for ring in backup_tokens, target_tokens:
                for token, host in ring.items():
                    ringmap[token].append(host)

            self.ringmap = ringmap
            for token, hosts in ringmap.items():
                self.host_map[hosts[1]] = {'source': [hosts[0]], 'seed': False}
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

    def _populate_hostmap(self):
        with open(self.host_list, 'r') as f:
            for line in f.readlines():
                seed, target, source = line.replace('\n', '').split(self.config.storage.host_file_separator)
                # in python, bool('False') evaluates to True. Need to test the membership as below
                self.host_map[target.strip()] = {'source': [source.strip()], 'seed': seed in ['True']}

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
        target_hosts = self.host_map.keys()

        if self.use_sstableloader is False:
            # stop all target nodes
            logging.info('Stopping Cassandra on all nodes currently up')

            # Generate a Job ID for this run
            job_id = str(uuid.uuid4())
            logging.debug('Job id is: {}'.format(job_id))
            # Define command to run
            command = self.config.cassandra.stop_cmd
            logging.debug('Command to run is: {}'.format(command))

            self._pssh_run(target_hosts, command, hosts_variables={})

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
            command = self._build_restore_cmd(target, source, seeds)

        pssh_run_success = self._pssh_run(target_hosts, command, hosts_variables=hosts_variables)

        if not pssh_run_success:
            # we could implement a retry.
            err_msg = 'Some nodes failed to restore. Exiting'
            logging.error(err_msg)
            raise Exception(err_msg)

        logging.info('Restore process is complete. The cluster should be up shortly.')

        if self.verify:
            verify_restore(target_hosts, self.config)

    def _build_restore_cmd(self, target, source, seeds):
        in_place_option = '--in-place' if self.in_place else ''
        keep_auth_option = '--keep-auth' if self.keep_auth else ''
        keyspace_options = expand_repeatable_option('keyspace', self.keyspaces)
        table_options = expand_repeatable_option('table', self.tables)
        # We explicitly set --no-verify since we are doing verification here in this module
        # from the control node
        verify_option = '--no-verify'

        # %s placeholders in the below command will get replaced by pssh using per host command substitution
        command = 'nohup sh -c "mkdir {work}; cd {work} && medusa-wrapper sudo medusa --fqdn=%s -vvv restore-node ' \
                  '{in_place} {keep_auth} %s {verify} --backup-name {backup} --temp-dir {temp_dir} ' \
                  '{use_sstableloader} {keyspaces} {tables}"' \
            .format(work=self.work_dir,
                    in_place=in_place_option,
                    keep_auth=keep_auth_option,
                    verify=verify_option,
                    backup=self.cluster_backup.name,
                    temp_dir=self.temp_dir,
                    use_sstableloader='--use-sstableloader' if self.use_sstableloader is True else '',
                    keyspaces=keyspace_options,
                    tables=table_options)

        logging.debug('Restoring on node {} with the following command {}'.format(target, command))

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
