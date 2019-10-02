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

import collections
import logging
import sys
import time
import uuid
import datetime
import traceback
import paramiko
import subprocess
import os

from medusa.monitoring import Monitoring

from medusa.cassandra_utils import CqlSessionProvider
from medusa.schema import parse_schema
from medusa.storage import Storage
from medusa.verify_restore import verify_restore


Remote = collections.namedtuple('Remote', ['target', 'connect_args', 'client', 'channel', 'stdout', 'stderr'])
SSH_ADD_KEYS_CMD = 'ssh-add'
SSH_AGENT_CREATE_CMD = 'ssh-agent'
SSH_AGENT_KILL_CMD = 'ssh-agent -k'
SSH_AUTH_SOCK_ENVVAR = 'SSH_AUTH_SOCK'
SSH_AGENT_PID_ENVVAR = 'SSH_AGENT_PID'


def orchestrate(config, backup_name, seed_target, temp_dir, host_list, keep_auth, bypass_checks,
                verify, keyspaces, tables, use_sstableloader=False):
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
                             keyspaces, tables, bypass_checks, use_sstableloader)
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
                 keyspaces={}, tables={}, bypass_checks=False, use_sstableloader=False):
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
        self._ssh_agent_started = False
        self.use_sstableloader = use_sstableloader

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
        if self._ssh_agent_started is True:
            self.ssh_cleanup()

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

        if self.use_sstableloader is False:
            # stop all target nodes
            stop_remotes = []
            logging.info('Stopping Cassandra on all nodes')
            for target, source in [(t, s['source']) for t, s in self.host_map.items()]:
                client, connect_args = self._connect(target)
                if self.check_cassandra_running(target, client, connect_args):
                    logging.info('Cassandra is running on {}. Stopping it...'.format(target))
                    command = 'sh -c "{}"'.format(self.config.cassandra.stop_cmd)
                    stop_remotes.append(self._run(target, client, connect_args, command))
                else:
                    logging.info('Cassandra is not running on {}.'.format(target))

            # wait for all nodes to stop
            logging.info('Waiting for all nodes to stop...')
            finished, broken = self._wait_for(stop_remotes)
            if len(broken) > 0:
                err_msg = 'Some Cassandras failed to stop. Exiting'
                logging.error(err_msg)
                raise Exception(err_msg)
        else:
            # we're using the sstableloader, which will require to (re)create the schema and empty the tables
            logging.info("Restoring schema on the target cluster")
            self._restore_schema()

        # work out which nodes are seeds in the target cluster
        target_seeds = [t for t, s in self.host_map.items() if s['seed']]

        # trigger restores everywhere at once
        # pass in seed info so that non-seeds can wait for seeds before starting
        # seeds, naturally, don't wait for anything
        remotes = []
        for target, source in [(t, s['source']) for t, s in self.host_map.items()]:
            logging.info('Restoring data on {}...'.format(target))
            seeds = None if target in target_seeds else target_seeds
            remote = self._trigger_restore(target, source, seeds=seeds)
            remotes.append(remote)

        # wait for the restores
        logging.info('Starting to wait for the nodes to restore')
        finished, broken = self._wait_for(remotes)
        if len(broken) > 0:
            err_msg = 'Some nodes failed to restore. Exiting'
            logging.error(err_msg)
            raise Exception(err_msg)

        logging.info('Restore process is complete. The cluster should be up shortly.')

        if self.verify:
            hosts = list(map(lambda r: r.target, remotes))
            verify_restore(hosts, self.config)

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
            session.execute("DROP MATERIALIZED VIEW {}.{}".format(keyspace, mv[0]))
        for table in keyspace_schema['tables'].items():
            logging.debug("Dropping table {}.{}".format(keyspace, table[0]))
            if table[0] in session.cluster.metadata.keyspaces[keyspace].tables.keys():
                # table already exists, drop it first
                session.execute("DROP TABLE {}.{}".format(keyspace, table[0]))
        for udt in keyspace_schema['udt'].items():
            # then custom types as they can be used in tables
            if udt[0] in session.cluster.metadata.keyspaces[keyspace].user_types.keys():
                # UDT already exists, drop it first
                session.execute("DROP TYPE {}.{}".format(keyspace, udt[0]))
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

    def _trigger_restore(self, target, source, seeds=None):
        client, connect_args = self._connect(target)

        # TODO: If this command fails, the node is currently still marked as finished and not as broken.
        in_place_option = '--in-place' if self.in_place else ''
        keep_auth_option = '--keep-auth' if self.keep_auth else ''
        seeds_option = '--seeds {}'.format(','.join(seeds)) if seeds else ''
        keyspace_options = expand_repeatable_option('keyspace', self.keyspaces)
        table_options = expand_repeatable_option('table', self.tables)
        # We explicitly set --no-verify since we are doing verification here in this module
        # from the control node
        verify_option = '--no-verify'

        command = 'nohup sh -c "cd {work} && medusa-wrapper sudo medusa --fqdn={fqdn} -vvv restore-node ' \
                  '{in_place} {keep_auth} {seeds} {verify} --backup-name {backup} {use_sstableloader} ' \
                  '{keyspaces} {tables}"' \
            .format(work=self.work_dir,
                    fqdn=','.join(source),
                    in_place=in_place_option,
                    keep_auth=keep_auth_option,
                    seeds=seeds_option,
                    verify=verify_option,
                    backup=self.cluster_backup.name,
                    use_sstableloader='--use-sstableloader' if self.use_sstableloader is True else '',
                    keyspaces=keyspace_options,
                    tables=table_options)

        logging.debug('Restoring on node {} with the following command {}'.format(target, command))
        return self._run(target, client, connect_args, command)

    def _wait_for(self, remotes):
        finished, broken = [], []

        while True:
            time.sleep(5)  # TODO: configure sleep

            if len(remotes) == len(finished) + len(broken):
                # TODO: make a nicer exit condition
                logging.info('Exiting because all jobs are done.')
                break

            for i, remote in enumerate(remotes):

                if remote in broken or remote in finished:
                    continue

                # If the remote does not set an exit status and the channel closes
                # the exit_status is negative.
                logging.debug('remote.channel.exit_status: {}'.format(remote.channel.exit_status))
                if remote.channel.exit_status_ready and remote.channel.exit_status >= 0:
                    if remote.channel.exit_status == 0:
                        finished.append(remote)
                        logging.info('Command succeeded on {}'.format(remote.target))
                    else:
                        broken.append(remote)
                        logging.error('Command failed on {} : '.format(remote.target))
                        logging.error('Output : {}'.format(remote.stdout.readlines()))
                        logging.error('Err output : {}'.format(remote.stderr.readlines()))
                        try:
                            stderr = self.read_file(remote, self.work_dir / 'stderr')
                        except IOError:
                            stderr = 'There was no stderr file'
                        logging.error(stderr)
                    # We got an exit code that does not indicate an error, but not necessarily
                    # success. Cleanup channel and move to next remote.
                    remote.channel.close()
                    # also close the client. this will free file descriptors
                    # in case we start re-using remotes this close will need to go away
                    remote.client.close()
                    continue

                if remote.client.get_transport().is_alive() and not remote.channel.closed:
                    # Send an ignored packet for keep alive and later noticing a broken connection
                    logging.debug('Keeping {} alive.'.format(remote.target))
                    remote.client.get_transport().send_ignore()
                else:
                    client = paramiko.client.SSHClient()
                    client.load_system_host_keys()
                    client.connect(**remote.connect_args)

                    # TODO: check pid to exist before assuming medusa-wrapper to pick it up
                    command = 'cd {work}; medusa-wrapper'.format(work=self.work_dir)
                    remotes[i] = self._run(remote.target, client, remote.connect_args, command)

        if len(broken) > 0:
            logging.info('Command failed on the following nodes:')
            for remote in broken:
                logging.info(remote.target)
        else:
            logging.info('Commands succeeded on all nodes')

        return finished, broken

    def _connect(self, target):
        logging.debug('Connecting to {}'.format(target))

        pkey = None
        if self.config.ssh.key_file is not None and self.config.ssh.key_file != '':
            pkey = paramiko.RSAKey.from_private_key_file(self.config.ssh.key_file, None)
            if self._ssh_agent_started is False:
                self.create_agent()
                add_key_cmd = '{} {}'.format(SSH_ADD_KEYS_CMD, self.config.ssh.key_file)
                subprocess.check_output(add_key_cmd, universal_newlines=True, shell=True)
                self._ssh_agent_started = True

        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.client.AutoAddPolicy())
        connect_args = {
            'hostname': target,
            'username': self.config.ssh.username,
            'pkey': pkey,
            'compress': True,
            'password': None
        }
        client.connect(**connect_args)

        logging.debug('Successfully connected to {}'.format(target))
        sftp = client.open_sftp()
        try:
            sftp.mkdir(str(self.work_dir))
        except OSError:
            err_msg = 'Working directory {} on {} failed.' \
                      'Folder might exist already, ignoring exception'.format(str(self.work_dir), target)
            logging.debug(err_msg)
        except Exception as ex:
            err_msg = 'Creating working directory on {} failed: {}'.format(target, str(ex))
            logging.error(err_msg)
            raise Exception(err_msg)
        finally:
            sftp.close()

        return client, connect_args

    def _run(self, target, client, connect_args, command):
        transport = client.get_transport()
        session = transport.open_session()
        session.get_pty()
        paramiko.agent.AgentRequestHandler(session)
        session.exec_command(command.replace('sudo', 'sudo -S'))
        bufsize = -1
        stdout = session.makefile('r', bufsize)
        stderr = session.makefile_stderr('r', bufsize)
        logging.debug('Running \'{}\' remotely on {}'.format(command, connect_args['hostname']))
        return Remote(target, connect_args, client, stdout.channel, stdout, stderr)

    def read_file(self, remote, remotepath):
        with remote.client.open_sftp() as ftp_client:
            with ftp_client.file(remotepath.as_posix(), 'r') as f:
                return str(f.read(), 'utf-8')

    def check_cassandra_running(self, host, client, connect_args):
        command = 'sh -c "{}"'.format(self.config.cassandra.check_running)
        remote = self._run(host, client, connect_args, command)
        return remote.channel.recv_exit_status() == 0

    def create_agent(self):
        """
        Function that creates the agent and sets the environment variables.
        """
        output = subprocess.check_output(SSH_AGENT_CREATE_CMD, universal_newlines=True, shell=True)
        if output:
            output = output.strip().split('\n')
            for item in output[0:2]:
                envvar, val = item.split(';')[0].split('=')
                logging.debug('Setting environment variable: {}={}'.format(envvar, val))
                os.environ[envvar] = val

    def ssh_cleanup(self):
        """
        Function that kills the agents created so that there aren't too many agents lying around eating up resources.
        """
        # Kill the agent
        subprocess.check_output(SSH_AGENT_KILL_CMD, universal_newlines=True, shell=True)
        # Reset these values so that other function
        os.environ[SSH_AUTH_SOCK_ENVVAR] = ''
        os.environ[SSH_AGENT_PID_ENVVAR] = ''
