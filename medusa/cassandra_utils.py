# -*- coding: utf-8 -*-
# Copyright 2020- Datastax, Inc. All rights reserved.
# Copyright 2020 Spotify Inc. All rights reserved.
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


import fileinput
import itertools
import logging
import pathlib
import shlex
import socket
import subprocess
import time
import yaml

from subprocess import PIPE
from retrying import retry
from cassandra.cluster import Cluster, ExecutionProfile
from cassandra.policies import WhiteListRoundRobinPolicy
from cassandra.auth import PlainTextAuthProvider
from ssl import SSLContext, PROTOCOL_TLSv1, CERT_REQUIRED
from medusa.network.hostname_resolver import HostnameResolver
from medusa.service.snapshot import SnapshotService
from medusa.nodetool import Nodetool


class SnapshotPath(object):

    def __init__(self, path, keyspace, table):
        self.path = path
        self.keyspace = keyspace
        self.columnfamily = table

    def list_files(self):
        # important to use the _r_glob() to recursively descend into subdirs if there are any
        return filter(lambda p: p.is_file(), self.path.rglob('*'))


class CqlSessionProvider(object):

    def __init__(self, ip_addresses, cassandra_config):
        self._ip_addresses = ip_addresses
        self._auth_provider = None
        self._ssl_context = None
        self._cassandra_config = cassandra_config

        if null_if_empty(cassandra_config.cql_username) and null_if_empty(cassandra_config.cql_password):
            auth_provider = PlainTextAuthProvider(username=cassandra_config.cql_username,
                                                  password=cassandra_config.cql_password)
            self._auth_provider = auth_provider

        if cassandra_config.certfile is not None and cassandra_config.usercert is not None and \
           cassandra_config.userkey is not None:
            ssl_context = SSLContext(PROTOCOL_TLSv1)
            ssl_context.load_verify_locations(cassandra_config.certfile)
            ssl_context.verify_mode = CERT_REQUIRED
            ssl_context.load_cert_chain(
                certfile=cassandra_config.usercert,
                keyfile=cassandra_config.userkey)
            self._ssl_context = ssl_context

        load_balancing_policy = WhiteListRoundRobinPolicy(ip_addresses)
        self._execution_profiles = {
            'local': ExecutionProfile(load_balancing_policy=load_balancing_policy)
        }

    def new_session(self, retry=False):
        """
        Creates a new CQL session. If retry is True then attempt to create a CQL session with retry logic. The max
        number of retries is currently hard coded at 5 and the delay between attempts is also hard coded at 5 sec. If
        no session can be created after the max retries is reached, an exception is raised.
         """

        cluster = Cluster(contact_points=self._ip_addresses,
                          auth_provider=self._auth_provider,
                          execution_profiles=self._execution_profiles,
                          ssl_context=self._ssl_context)

        if retry:
            max_retries = 5
            attempts = 0

            while attempts < max_retries:
                try:
                    session = cluster.connect()
                    return CqlSession(session, self._cassandra_config.resolve_ip_addresses)
                except Exception as e:
                    logging.debug('Failed to create session', exc_info=e)
                delay = 5 * (2 ** (attempts + 1))
                time.sleep(delay)
                attempts = attempts + 1
            raise Exception('Could not establish CQL session after {attempts}'.format(attempts=attempts))
        else:
            session = cluster.connect()
            return CqlSession(session, self._cassandra_config.resolve_ip_addresses)


class CqlSession(object):
    EXCLUDED_KEYSPACES = ['system_traces']

    def __init__(self, session, resolve_ip_addresses=True):
        self._session = session
        self.hostname_resolver = HostnameResolver(resolve_ip_addresses)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.shutdown()

    def shutdown(self):
        self.session.shutdown()
        self.cluster.shutdown()

    @property
    def cluster(self):
        return self._session.cluster

    @property
    def session(self):
        return self._session

    def token(self):
        listen_address = self.cluster.contact_points[0]
        token_map = self.cluster.metadata.token_map
        for token, host in token_map.token_to_host_owner.items():
            if host.address == listen_address:
                return token.value
        raise RuntimeError('Unable to get current token')

    def datacenter(self):
        logging.debug('Checking datacenter...')
        listen_address = socket.gethostbyname(self.cluster.contact_points[0])
        token_map = self.cluster.metadata.token_map

        for host in token_map.token_to_host_owner.values():
            socket_host = self.hostname_resolver.resolve_fqdn(listen_address)
            logging.debug('Checking host {} against {}/{}'.format(host.address, listen_address, socket_host))
            if host.address == listen_address or self.hostname_resolver.resolve_fqdn(host.address) == socket_host:
                return host.datacenter

        raise RuntimeError('Unable to current datacenter')

    def tokenmap(self):
        token_map = self.cluster.metadata.token_map
        datacenter = self.datacenter()

        def get_host(host_token_pair):
            return host_token_pair[0]

        def get_host_address(host_token_pair):
            return host_token_pair[0].address

        def get_token(host_token_pair):
            return host_token_pair[1]

        host_token_pairs = sorted(
            [(host, token.value) for token, host in token_map.token_to_host_owner.items()],
            key=get_host_address
        )
        host_tokens_groups = itertools.groupby(host_token_pairs, key=get_host)
        host_tokens_pairs = [(host, list(map(get_token, tokens))) for host, tokens in host_tokens_groups]

        return {
            self.hostname_resolver.resolve_fqdn(host.address): {
                'tokens': tokens,
                'is_up': host.is_up
            }
            for host, tokens in host_tokens_pairs
            if host.datacenter == datacenter
        }

    def dump_schema(self):
        keyspaces = self.session.cluster.metadata.keyspaces
        return '\n\n'.join(metadata.export_as_string()
                           for keyspace, metadata in keyspaces.items()
                           if keyspace not in self.EXCLUDED_KEYSPACES)

    def schema_path_mapping(self):
        query = 'SELECT keyspace_name, columnfamily_name, cf_id FROM system.schema_columnfamilies'

        return (row for row in self.session.execute(query)
                if row.keyspace_name not in self.EXCLUDED_KEYSPACES)

    def execute(self, query):
        return self.session.execute(query)


class CassandraConfigReader(object):

    DEFAULT_CASSANDRA_CONFIG = '/etc/cassandra/cassandra.yaml'

    def __init__(self, cassandra_config=None):
        config_file = pathlib.Path(cassandra_config or self.DEFAULT_CASSANDRA_CONFIG)
        if not config_file.is_file():
            raise RuntimeError('{} is not a file'.format(config_file))
        with open(config_file, 'r') as f:
            self._config = yaml.load(f, Loader=yaml.BaseLoader)

    @property
    def root(self):
        data_file_directories = self._config.get('data_file_directories', ['/var/lib/cassandra/data'])
        if not data_file_directories:
            raise RuntimeError('data_file_directories must be properly configured')
        if len(data_file_directories) > 1:
            raise RuntimeError('Medusa only supports one data directory')
        return pathlib.Path(data_file_directories[0])

    @property
    def commitlog_directory(self):
        commitlog_directory = self._config.get('commitlog_directory', '/var/lib/cassandra/commitlog')
        if not commitlog_directory:
            raise RuntimeError('commitlog_directory must be properly configured')
        return pathlib.Path(commitlog_directory)

    @property
    def saved_caches_directory(self):
        saved_caches_directory = self._config.get('saved_caches_directory', '/var/lib/cassandra/saved_caches')
        if not saved_caches_directory:
            raise RuntimeError('saved_caches_directory must be properly configured')
        return pathlib.Path(saved_caches_directory)

    @property
    def listen_address(self):
        if 'listen_address' in self._config:
            if self._config['listen_address']:
                return self._config['listen_address']

        return socket.gethostbyname(socket.getfqdn())

    @property
    def storage_port(self):
        if 'storage_port' in self._config:
            if self._config['storage_port']:
                return self._config['storage_port']
        return "7000"

    @property
    def native_port(self):
        if 'native_transport_port' in self._config:
            if self._config['native_transport_port']:
                return self._config['native_transport_port']
        return "9042"

    @property
    def rpc_port(self):
        if 'rpc_port' in self._config:
            if self._config['rpc_port']:
                return self._config['rpc_port']
        return "9160"

    @property
    def seeds(self):
        seeds = list()
        if 'seed_provider' in self._config:
            if self._config['seed_provider']:
                if self._config['seed_provider'][0]['class_name'].endswith('SimpleSeedProvider'):
                    return self._config.get('seed_provider')[0]['parameters'][0]['seeds'].replace(' ', '').split(',')
        return seeds


class Cassandra(object):

    SNAPSHOT_PATTERN = '*/*/snapshots/{}'
    SNAPSHOT_PREFIX = 'medusa-'

    def __init__(self, config, contact_point=None):
        cassandra_config = config.cassandra
        self._start_cmd = shlex.split(cassandra_config.start_cmd)
        self._stop_cmd = shlex.split(cassandra_config.stop_cmd)
        self._is_ccm = int(shlex.split(cassandra_config.is_ccm)[0])
        self._os_has_systemd = self._has_systemd()
        self._nodetool = Nodetool(cassandra_config)
        logging.warning('is ccm : {}'.format(self._is_ccm))

        config_reader = CassandraConfigReader(cassandra_config.config_file)
        self._cassandra_config_file = cassandra_config.config_file
        self._root = config_reader.root
        self._commitlog_path = config_reader.commitlog_directory
        self._saved_caches_path = config_reader.saved_caches_directory
        self._hostname = contact_point if contact_point is not None else config_reader.listen_address
        self._cql_session_provider = CqlSessionProvider(
            [self._hostname],
            cassandra_config)

        self._storage_port = config_reader.storage_port
        self._native_port = config_reader.native_port
        self._rpc_port = config_reader.rpc_port
        self.seeds = config_reader.seeds

        self.grpc_config = config.grpc
        self.kubernetes_config = config.kubernetes
        self.snapshot_service = SnapshotService(config=config).snapshot_service

    def _has_systemd(self):
        try:
            result = subprocess.run(['systemctl', '--version'], stdout=PIPE, stderr=PIPE)
            logging.debug('This server has systemd: {}'.format(result.returncode == 0))
            return result.returncode == 0
        except (AttributeError, FileNotFoundError):
            # AttributeError is thrown when subprocess.run is not found, which happens on Trusty
            # Trusty doesn't have systemd, so the semantics of this code still hold
            logging.debug('This server has systemd: False')
            return False

    def new_session(self):
        return self._cql_session_provider.new_session()

    @property
    def root(self):
        return self._root

    @property
    def commit_logs_path(self):
        return self._commitlog_path

    @property
    def saved_caches_path(self):
        return self._saved_caches_path

    @property
    def hostname(self):
        return self._hostname

    @property
    def storage_port(self):
        return self._storage_port

    @property
    def native_port(self):
        return self._native_port

    @property
    def rpc_port(self):
        return self._rpc_port

    class Snapshot(object):
        def __init__(self, parent, tag):
            self._parent = parent
            self._tag = tag

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            logging.debug('Cleaning up snapshot')
            self.delete()

        @property
        def cassandra(self):
            return self._parent

        @property
        def tag(self):
            return self._tag

        @property
        def root(self):
            return self._parent.root

        def find_dirs(self):
            return [
                SnapshotPath(
                    pathlib.Path(snapshot_dir),
                    *snapshot_dir.relative_to(self.root).parts[:2]
                )
                for snapshot_dir in self.root.glob(
                    Cassandra.SNAPSHOT_PATTERN.format(self._tag)
                )
                if (snapshot_dir.is_dir() and snapshot_dir.parts[-4]
                    not in CqlSession.EXCLUDED_KEYSPACES)
            ]

        def delete(self):
            self._parent.delete_snapshot(self._tag)

        def __repr__(self):
            return '{}<{}>'.format(self.__class__.__qualname__, self._tag)

    def create_snapshot(self, backup_name):
        tag = "{}{}".format(self.SNAPSHOT_PREFIX, backup_name)
        if not self.snapshot_exists(tag):
            self.snapshot_service.create_snapshot(tag=tag)

        return Cassandra.Snapshot(self, tag)

    def delete_snapshot(self, tag):
        if self.snapshot_exists(tag):
            self.snapshot_service.delete_snapshot(tag=tag)

    def list_snapshotnames(self):
        return {
            snapshot.name
            for snapshot in self.root.glob(self.SNAPSHOT_PATTERN.format('*'))
            if snapshot.is_dir()
        }

    def get_snapshot(self, tag):
        if any(self.root.glob(self.SNAPSHOT_PATTERN.format(tag))):
            return Cassandra.Snapshot(self, tag)

        raise KeyError('Snapshot {} does not exist'.format(tag))

    def snapshot_exists(self, tag):
        for snapshot in self.root.glob(self.SNAPSHOT_PATTERN.format('*')):
            if snapshot.is_dir() and snapshot.name == tag:
                return True
        return False

    def create_snapshot_command(self, backup_name):
        """
        :param backup_name: string name of the medusa backup
        :return: Array representation of a command to create a snapshot
        """
        tag = '{}{}'.format(self.SNAPSHOT_PREFIX, backup_name)
        if self._is_ccm == 1:
            cmd = 'ccm node1 nodetool \"snapshot -t {}\"'.format(tag)
        else:
            cmd = self._nodetool.nodetool + ['snapshot', '-t', tag]
        return cmd

    def delete_snapshot_command(self, tag):
        """
        :param tag: string snapshot name
        :return: Array repesentation of a command to delete a snapshot
        """
        if self._is_ccm == 1:
            cmd = 'ccm node1 nodetool \"clearsnapshot -t {}\"'.format(tag)
        else:
            cmd = self._nodetool.nodetool + ['clearsnapshot', '-t', tag]
        return cmd

    def _columnfamily_path(self, keyspace_name, columnfamily_name, cf_id):
        root = pathlib.Path(self._root)
        keyspace_path = root / keyspace_name / columnfamily_name

        if keyspace_path.exists() and keyspace_path.is_dir():
            return keyspace_path
        else:
            # Notice: Cassandra use dashes in the cf_id in the system table,
            # but not in the directory names
            directory_postfix = str(cf_id).replace('-', '')
            return keyspace_path.with_name('{}-{}'.format(
                columnfamily_name,
                directory_postfix
            ))

    def _full_columnfamily_name(self, keyspace_name, columnfamily_name, cf_id):
        root = pathlib.Path(self._root)
        keyspace_path = root / keyspace_name / columnfamily_name

        if keyspace_path.exists() and keyspace_path.is_dir():
            return columnfamily_name
        else:
            # Notice: Cassandra use dashes in the cf_id in the system table,
            # but not in the directory names
            directory_postfix = str(cf_id).replace('-', '')
            return '{}-{}'.format(columnfamily_name, directory_postfix)

    def schema_path_mapping(self):

        def _full_cf_name(row):
            return self._full_columnfamily_name(row.keyspace_name, row.columnfamily_name, row.cf_id)

        def _full_cf_path(row):
            return self._columnfamily_path(row.keyspace_name, row.columnfamily_name, row.cf_id)

        with self._cql_session_provider.new_session() as session:
            return {
                (row.keyspace_name, _full_cf_name(row)): _full_cf_path(row)
                for row in session.schema_path_mapping()
            }

    def shutdown(self):
        try:
            subprocess.check_output(self._stop_cmd)
        except subprocess.CalledProcessError:
            logging.debug('Cassandra is already down on {}'.format(self._hostname))
            return

    def start_with_implicit_token(self):
        cmd = self._start_cmd
        logging.debug('Starting Cassandra with {}'.format(cmd))
        subprocess.check_output(cmd)

    def start(self, token_list):
        if self._is_ccm == 0:
            self.replaceTokensInCassandraYamlAndDisableBootstrap(token_list)
            cmd = '{}'.format(' '.join(shlex.quote(x) for x in self._start_cmd))
            logging.debug('Starting Cassandra with {}'.format(cmd))
            # run the command using 'shell=True' option
            # to interpret the string command well
            subprocess.check_output(cmd, shell=True)
        else:
            subprocess.check_output(self._start_cmd, shell=True)

    def replaceTokensInCassandraYamlAndDisableBootstrap(self, token_list):
        initial_token_line_found = False
        auto_bootstrap_line_found = False
        for line in fileinput.input(self._cassandra_config_file, inplace=True):
            if (line.startswith("initial_token:")):
                initial_token_line_found = True
                print('initial_token: {}'.format(','.join(token_list)), end='\n')
            elif (line.startswith("num_tokens:")):
                print('num_tokens: {}'.format(len(token_list)), end='\n')
            elif (line.startswith("auto_bootstrap:")):
                auto_bootstrap_line_found = True
                print('auto_bootstrap: false', end='\n')
            else:
                print('{}'.format(line), end='')

        if (not initial_token_line_found):
            with open(self._cassandra_config_file, "a") as cassandra_yaml:
                cassandra_yaml.write('\ninitial_token: {}'.format(','.join(token_list)))

        if (not auto_bootstrap_line_found):
            with open(self._cassandra_config_file, "a") as cassandra_yaml:
                cassandra_yaml.write('\nauto_bootstrap: false')


@retry(stop_max_attempt_number=7, wait_exponential_multiplier=5000, wait_exponential_max=120000)
def wait_for_node_to_come_up(config, host):
    logging.info('Waiting for Cassandra to come up on {}'.format(host))

    if not is_node_up(config, host):
        raise CassandraNodeNotUpError(host)
    else:
        logging.info('Cassandra is up on {}'.format(host))
        return True


@retry(stop_max_attempt_number=7, wait_exponential_multiplier=5000, wait_exponential_max=120000)
def wait_for_node_to_go_down(config, host):
    logging.info('Waiting for Cassandra to go down on {}'.format(host))

    if is_node_up(config, host):
        # TODO can do a kill here
        raise CassandraNodeNotDownError(host)
    else:
        logging.info('Cassandra is down {}'.format(host))
        return True


def is_node_up(config, host):
    """
    Calls nodetool statusbinary, nodetool statusthrift or both. This function checks the output returned from nodetool
    and not the return code. There could be a normal return code of zero when the node is an unhealthy state and not
    accepting requests.

    :param health_check: Supported values are cql, thrift, and all. The latter will perform both checks. Defaults to
    cql.
    :param host: The target host on which to perform the check
    :return: True if the node is accepting requests, False otherwise. If both cql and thrift are checked, then the node
    must be ready to accept requests for both in order for the health check to be successful.
    """

    health_check = config.checks.health_check
    if int(config.cassandra.is_ccm) == 1:
        args = ['ccm', 'node1', 'nodetool']
        if health_check == 'thrift':
            return is_ccm_up(args, 'statusthrift')
        elif health_check == 'all':
            return is_ccm_up(list(args), 'statusbinary') and is_ccm_up(list(args), 'statusthrift')
        else:
            return is_ccm_up(args, 'statusbinary')
    else:
        cassandra = Cassandra(config)
        native_port = cassandra.native_port
        rpc_port = cassandra.rpc_port
        if health_check == 'thrift':
            return is_cassandra_up(host, rpc_port)
        elif health_check == 'all':
            return is_cassandra_up(host, rpc_port) and is_cassandra_up(host, native_port)
        else:
            # cql only
            return is_cassandra_up(host, native_port)


def is_ccm_up(args, nodetool_command):
    try:
        args.append(nodetool_command)
        output = subprocess.check_output(args, stderr=subprocess.STDOUT, universal_newlines=True)
        # ccm returns either 'running' or 'not running'.
        # Testing on finding 'running' is not enough!
        if output.find('not running') == -1 and output.find('running') >= 0:
            logging.debug('CCM native transport is now up')
            return True
        else:
            logging.debug('CCM native transport is not up yet')
            return False
    except subprocess.CalledProcessError as e:
        logging.debug('CCM native transport is not up yet', exc_info=e)
        return False


def is_open(host, port):
    timeout = 3
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout)
    is_accessible = False
    try:
        s.connect((host, port))
        s.shutdown(socket.SHUT_RDWR)
        is_accessible = True
    except socket.error as e:
        logging.debug('Port {} close on host {}'.format(port, host), exc_info=e)
        is_accessible = False
    finally:
        s.close()
        return is_accessible


@retry(stop_max_attempt_number=5, wait_exponential_multiplier=5000, wait_exponential_max=120000)
def is_cassandra_up(host, port):
    if is_open(host, port):
        return True
    else:
        logging.debug('The node {} is not up yet...'.format(host))


class CassandraNodeNotUpError(Exception):
    """
    Raised when it cannot be verified that a node is up by checking either nodetool statusbinary and/or
    nodetool statusthrift

    Attributes:
        host -- the hostname or ip address of the node
        attempts -- the number of times the check was performed
    """

    def __init(self, host):
        msg = 'Cassandra node {} is still down...'.format(host)
        super(CassandraNodeNotUpError, self).__init__(msg)


class CassandraNodeNotDownError(Exception):
    """
    Raised when we give up waiting on a node to go down, meaning nodetool statusbinary and/or statusthrift keep
    reporting open ports.
    """
    def __init(self, host):
        msg = 'Cassandra node {} is still up...'.format(host)
        super(CassandraNodeNotDownError, self).__init__(msg)
