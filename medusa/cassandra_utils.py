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
import os
import pathlib
import shutil
import shlex
import socket
import subprocess
import time
from ssl import SSLContext, PROTOCOL_TLSv1_2, CERT_REQUIRED
from subprocess import PIPE

import yaml
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, ExecutionProfile
from cassandra.policies import WhiteListRoundRobinPolicy
from cassandra.util import Version
from retrying import retry

from medusa.host_man import HostMan
from medusa.network.hostname_resolver import HostnameResolver
from medusa.network.hostname_resolver import resolve_name
from medusa.nodetool import Nodetool
from medusa.service.snapshot import SnapshotService
from medusa.utils import null_if_empty, evaluate_boolean


class SnapshotPath(object):

    def __init__(self, path, keyspace, table):
        self.path = path
        self.keyspace = keyspace
        self.columnfamily = table

    def list_files(self):
        # important to use the _r_glob() to recursively descend into subdirs if there are any
        return filter(lambda p: p.is_file(), self.path.rglob('*'))

    def __repr__(self):
        return "SnapshotPath(path={}, keyspace={}, table={})".format(self.path, self.keyspace, self.columnfamily)


class CqlSessionProvider(object):

    def __init__(self, ip_addresses, config):
        self._ip_addresses = ip_addresses
        self._auth_provider = None
        self._ssl_context = None
        self._cassandra_config = config.cassandra
        self._config = config
        self._native_port = CassandraConfigReader(self._cassandra_config.config_file).native_port

        if null_if_empty(self._cassandra_config.cql_username) and null_if_empty(self._cassandra_config.cql_password):
            auth_provider = PlainTextAuthProvider(username=self._cassandra_config.cql_username,
                                                  password=self._cassandra_config.cql_password)
            self._auth_provider = auth_provider

        if self._cassandra_config.certfile is not None:
            ssl_context = SSLContext(PROTOCOL_TLSv1_2)
            ssl_context.load_verify_locations(self._cassandra_config.certfile)
            ssl_context.verify_mode = CERT_REQUIRED
            if self._cassandra_config.usercert is not None and self._cassandra_config.userkey is not None:
                ssl_context.load_cert_chain(
                    certfile=self._cassandra_config.usercert,
                    keyfile=self._cassandra_config.userkey)
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
                          port=int(self._native_port),
                          auth_provider=self._auth_provider,
                          execution_profiles=self._execution_profiles,
                          ssl_context=self._ssl_context)

        if retry:
            max_retries = 5
            attempts = 0

            while attempts < max_retries:
                try:
                    session = cluster.connect()
                    return CqlSession(session,
                                      evaluate_boolean(self._cassandra_config.resolve_ip_addresses),
                                      evaluate_boolean(
                                          self._config.kubernetes.enabled if self._config.kubernetes else False))
                except Exception as e:
                    logging.warning('Failed to create session', exc_info=e)
                delay = 5 * (2 ** (attempts + 1))
                time.sleep(delay)
                attempts = attempts + 1
            raise CassandraCqlSessionException('Could not establish CQL session '
                                               'after {attempts}'.format(attempts=attempts))
        else:
            session = cluster.connect()
            return CqlSession(session,
                              evaluate_boolean(self._cassandra_config.resolve_ip_addresses),
                              evaluate_boolean(self._config.kubernetes.enabled if self._config.kubernetes else False))


class CqlSession(object):
    EXCLUDED_KEYSPACES = ['system_traces']

    def __init__(self, session, resolve_ip_addresses=True, k8s_mode=False):
        self._session = session
        self.hostname_resolver = HostnameResolver(resolve_ip_addresses, k8s_mode)

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

    def placement(self):
        logging.debug('Checking placement using dc and rack...')
        listen_address = resolve_name(self.cluster.contact_points[0])
        token_map = self.cluster.metadata.token_map

        for host in token_map.token_to_host_owner.values():
            socket_host = self.hostname_resolver.resolve_fqdn(listen_address)
            logging.debug('Checking host {} against {}/{}'.format(host.address, listen_address, socket_host))
            if host.address == listen_address or self.hostname_resolver.resolve_fqdn(host.address) == socket_host:
                return host.datacenter, host.rack

        raise RuntimeError('Unable to get current placement')

    def tokenmap(self):
        token_map = self.cluster.metadata.token_map
        dc_rack_pair = self.placement()

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
                'is_up': host.is_up,
                'rack': host.rack,
                'dc': host.datacenter
            }
            for host, tokens in host_tokens_pairs
            if host.datacenter == dc_rack_pair[0]
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

    def get_server_type_and_release_version(self):
        server_type = 'cassandra'
        release_version = 'None'
        rows = self.session.execute("SELECT * FROM system.local")
        for row in rows:
            if hasattr(row, 'dse_version'):
                server_type = 'dse'
            if hasattr(row, 'release_version'):
                release_version = row.release_version
        return server_type, release_version


class CassandraConfigReader(object):
    DEFAULT_CASSANDRA_CONFIG = '/etc/cassandra/cassandra.yaml'

    def __init__(self, cassandra_config=None, release_version=None):
        self._release_version = release_version
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
        if 'listen_address' in self._config and self._config['listen_address']:
            return self._config['listen_address']

        return resolve_name(socket.getfqdn())

    @property
    def storage_port(self):
        """
        SSL port, for legacy encrypted communication. The ssl_storage_port is unused unless enabled in
        server_encryption_options. As of cassandra 4.0, this property is deprecated
        as a single port can be used for either/both secure and insecure connections.
        """
        if 'server_encryption_options' in self._config and \
                self._config['server_encryption_options']['internode_encryption'] is not None and \
                self._config['server_encryption_options']['internode_encryption'] != "none":

            # Secure connections, ssl_storage_port is specified.
            if 'ssl_storage_port' in self._config and self._config['ssl_storage_port'] is not None:
                logging.warning("ssl_storage_port is deprecated as of Apache Cassandra 4.x")
                return self._config['ssl_storage_port']
            else:
                # ssl_storage_port not specified, and found a version of c* 4+
                if self._release_version is not None and Version(self._release_version) >= Version('4-a') and \
                        'storage_port' in self._config and self._config['storage_port'] is not None:
                    return self._config['storage_port']
                return "7001"

        # Insecure connection handling of storage_port for any version of c*
        if 'storage_port' in self._config and self._config['storage_port'] is not None:
            return self._config['storage_port']
        return "7000"

    @property
    def native_port(self):
        """
        Condition for client encryption enabled.
         When setting native_transport_port_ssl, expecting that non-encrypted will still be over existing
         native_transport_port. The encrypted will be over the native_transport_port_ssl.
         When not setting an alternate native_transport_port_ssl , the encrypted will be over
         the existing native_transport_port
        """
        # Conditions for client encryption enabled, default encrypted port.
        if 'client_encryption_options' in self._config and \
                self._config['client_encryption_options']['enabled'] is not None and \
                self._config['client_encryption_options']['enabled'] == "true":
            if 'native_transport_port_ssl' in self._config and \
                    self._config['native_transport_port_ssl'] is not None:
                return self._config['native_transport_port_ssl']
            elif 'native_transport_port' in self._config and \
                    self._config['native_transport_port'] is not None:
                return self._config['native_transport_port']
            return "9142"

        if 'native_transport_port' in self._config and self._config['native_transport_port']:
            return self._config['native_transport_port']
        return "9042"

    @property
    def rpc_port(self):
        if 'rpc_port' in self._config and self._config['rpc_port']:
            return self._config['rpc_port']
        return "9160"

    @property
    def seeds(self):
        seeds = []
        if 'seed_provider' in self._config and self._config['seed_provider'] and \
                self._config['seed_provider'][0]['class_name'].endswith('SimpleSeedProvider'):
            return self._config.get('seed_provider')[0]['parameters'][0]['seeds'].replace(' ', '').split(',')
        return seeds


class Cassandra(object):
    SNAPSHOT_PATTERN = '*/*/snapshots/{}'
    DSE_SNAPSHOT_PATTERN = '*/snapshots/{}'
    SNAPSHOT_PREFIX = 'medusa-'

    def __init__(self, config, contact_point=None, release_version=None):
        self._release_version = release_version
        cassandra_config = config.cassandra
        self._start_cmd = shlex.split(cassandra_config.start_cmd)
        self._stop_cmd = shlex.split(cassandra_config.stop_cmd)
        self._is_ccm = int(shlex.split(cassandra_config.is_ccm)[0])
        self._os_has_systemd = self._has_systemd()
        self._nodetool = Nodetool(cassandra_config)
        config_reader = CassandraConfigReader(cassandra_config.config_file, release_version)
        self._cassandra_config_file = cassandra_config.config_file
        self._root = config_reader.root
        self._dse_root = self._root.parent
        self._dse_metadata_folder = 'metadata'
        self._commitlog_path = config_reader.commitlog_directory
        self._saved_caches_path = config_reader.saved_caches_directory
        self._hostname = contact_point if contact_point is not None else config_reader.listen_address
        self._storage_port = config_reader.storage_port
        self._native_port = config_reader.native_port
        self._cql_session_provider = CqlSessionProvider(
            [self._hostname],
            config)
        self._rpc_port = config_reader.rpc_port
        self.seeds = config_reader.seeds

        self.grpc_config = config.grpc
        self.kubernetes_config = config.kubernetes
        self.snapshot_service = SnapshotService(config=config).snapshot_service

    @staticmethod
    def _has_systemd():
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
        return int(self._storage_port)

    @property
    def native_port(self):
        return int(self._native_port)

    @property
    def rpc_port(self):
        return int(self._rpc_port)

    @property
    def release_version(self):
        return self._release_version

    @property
    def dse_metadata_path(self):
        return self._dse_root / self._dse_metadata_folder

    @property
    def dse_search_path(self):
        # the DSE Search files are next to regular keyspace folders, but are not a real keyspace
        return self._root / 'solr.data'

    def rebuild_search_index(self):
        logging.debug('Opening new session to restore DSE indexes')
        with self._cql_session_provider.new_session() as session:
            rows = session.execute("SELECT core_name FROM solr_admin.solr_resources")
            fqtns_with_index = {r.core_name for r in rows}
            for fqtn in fqtns_with_index:
                logging.debug(f'Rebuilding search index for {fqtn}')
                session.execute(f"REBUILD SEARCH INDEX ON {fqtn}")

    @staticmethod
    def _ignore_snapshots(folder, contents):
        ignored = set()
        if folder.endswith('metadata/snapshots'):
            logging.info(f'Ignoring {contents} in folder {folder}')
            for c in contents:
                ignored.add(c)
        return ignored

    def create_dse_snapshot(self, backup_name):
        """
        There is no good way of making snapshot of DSE files
        They are not SSTables, so we cannot just hard-link them and get immutable files to work with
        So we have a poor-mans alternative of just copying them into a folder
        That folder is nested in the parent folder, just like for regular tables
        This way, we can reuse a lot of code later on
        """

        tag = "{}{}".format(self.SNAPSHOT_PREFIX, backup_name)
        if not self.dse_snapshot_exists(tag):
            src_path = self._dse_root / self._dse_metadata_folder
            dst_path = self._dse_root / self._dse_metadata_folder / 'snapshots' / tag
            shutil.copytree(src_path, dst_path, ignore=Cassandra._ignore_snapshots)

        return Cassandra.DseSnapshot(self, tag)

    class DseSnapshot(object):

        def __init__(self, parent, tag):
            self._parent = parent
            self._tag = tag

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            logging.debug('Cleaning up DSE snapshot')
            self.delete()

        def find_dirs(self):
            dse_folder = self._parent._dse_metadata_folder
            return [
                SnapshotPath(
                    pathlib.Path(self._parent._dse_root) / dse_folder / 'snapshots', 'dse', dse_folder
                )
            ]

        def delete(self):
            dse_folder = self._parent._dse_metadata_folder
            dse_folder_path = self._parent._dse_root / dse_folder / 'snapshots' / self._tag
            shutil.rmtree(dse_folder_path)

        def __repr__(self):
            return '{}<{}>'.format(self.__class__.__qualname__, self._tag)

    class Snapshot(object):
        def __init__(self, parent, tag, keep_snapshot=False):
            self._parent = parent
            self._tag = tag
            self._keep_snapshot = keep_snapshot

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            if not self._keep_snapshot:
                logging.debug('Cleaning up Cassandra snapshot')
                self.delete()
            else:
                logging.debug('Keeping snapshot {}'.format(self._tag))

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

    def create_snapshot(self, backup_name, keep_snapshot=False):
        tag = "{}{}".format(self.SNAPSHOT_PREFIX, backup_name)
        if not self.snapshot_exists(tag):
            self.snapshot_service.create_snapshot(tag=tag)

        return Cassandra.Snapshot(self, tag, keep_snapshot)

    def delete_snapshot(self, tag):
        if self.snapshot_exists(tag):
            self.snapshot_service.delete_snapshot(tag=tag)

    def list_snapshotnames(self):
        return {
            snapshot.name
            for snapshot in self.root.glob(self.SNAPSHOT_PATTERN.format('*'))
            if snapshot.is_dir()
        }

    def get_snapshot(self, tag, keep_snapshot=False):
        if any(self.root.glob(self.SNAPSHOT_PATTERN.format(tag))):
            return Cassandra.Snapshot(self, tag, keep_snapshot)

        raise KeyError('Snapshot {} does not exist'.format(tag))

    def snapshot_exists(self, tag):
        for snapshot in self.root.glob(self.SNAPSHOT_PATTERN.format('*')):
            if snapshot.is_dir() and snapshot.name == tag:
                return True
        return False

    def dse_snapshot_exists(self, tag):
        # dse files live one directory up from the data folder
        # the root field should point to the data directory as defined in the cassandra.yaml
        for snapshot in self._dse_root.glob(self.DSE_SNAPSHOT_PATTERN.format('*')):
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
            cmd = f'ccm node1 nodetool -- -Dcom.sun.jndi.rmiURLParsing=legacy \"snapshot -t {tag}\"'
        else:
            cmd = self._nodetool.nodetool + ['snapshot', '-t', tag]
        return cmd

    def delete_snapshot_command(self, tag):
        """
        :param tag: string snapshot name
        :return: Array repesentation of a command to delete a snapshot
        """
        if self._is_ccm == 1:
            cmd = f'ccm node1 nodetool -- -Dcom.sun.jndi.rmiURLParsing=legacy \"clearsnapshot -t {tag}\"'
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
            self.replace_tokens_in_cassandra_yaml_and_disable_bootstrap(token_list)
            cmd = '{}'.format(' '.join(shlex.quote(x) for x in self._start_cmd))
            logging.debug('Starting Cassandra with {}'.format(cmd))
            # run the command using 'shell=True' option
            # to interpret the string command well
            subprocess.check_output(cmd, shell=True)
        else:
            subprocess.check_output(self._start_cmd, shell=True)

    def replace_tokens_in_cassandra_yaml_and_disable_bootstrap(self, token_list):
        initial_token_line_found = False
        auto_bootstrap_line_found = False
        for line in fileinput.input(self._cassandra_config_file, inplace=True):
            if line.startswith("initial_token:"):
                initial_token_line_found = True
                print('initial_token: {}'.format(','.join(token_list)), end='\n')
            elif line.startswith("num_tokens:"):
                print('num_tokens: {}'.format(len(token_list)), end='\n')
            elif line.startswith("auto_bootstrap:"):
                auto_bootstrap_line_found = True
                print('auto_bootstrap: false', end='\n')
            else:
                print('{}'.format(line), end='')

        if not initial_token_line_found:
            with open(self._cassandra_config_file, "a") as cassandra_yaml:
                cassandra_yaml.write('\ninitial_token: {}'.format(','.join(token_list)))

        if not auto_bootstrap_line_found:
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
    If not configured with ccm, proceed with direct health check for Cassandra.
    """
    try:
        check_type = config.checks.health_check
        logging.info('Verifying node state for host {} using check type {}'.format(host, check_type))
        if int(config.cassandra.is_ccm) == 1:
            logging.debug('Checking ccm health')
            return is_ccm_healthy(check_type)

        return is_cassandra_healthy(check_type, Cassandra(config, release_version=HostMan.get_release_version()),
                                    host)
    except Exception as e:
        err_msg = 'Unable to determine if node is up for host: {}'.format(host)
        logging.debug(err_msg, exc_info=e)
        return False


def is_ccm_healthy(check_type):
    """
    Calls nodetool statusbinary, nodetool statusthrift or both. This function checks the output returned from nodetool
    and not the return code. There could be a normal return code of zero when the node is an unhealthy state and not
    accepting requests.
    :param check_type: Supported values are cql, thrift, and all. The latter will perform both checks. Defaults to
    cql.
    :return: True if the node is accepting requests, False otherwise. If both cql and thrift are checked, then the node
    must be ready to accept requests for both in order for the health check to be successful.
    """
    try:
        args = ['ccm', 'node1', 'nodetool', '--', '-Dcom.sun.jndi.rmiURLParsing=legacy']

        if check_type == 'thrift':
            return is_ccm_up(args, 'statusthrift')
        elif check_type == 'all':
            return is_ccm_up(list(args), 'statusbinary') and is_ccm_up(list(args), 'statusthrift')
        else:
            return is_ccm_up(args, 'statusbinary')

    except Exception as e:
        logging.debug('CCM not found to be healthy during check', exc_info=e)

    return False


def is_cassandra_healthy(check_type, cassandra, host):
    """
    Utilizes knowledge of an enabled encrypt conn. or not-enabled encrypt conn. to use ports from Cassandra config.
    Both native and storage(gossip) ports can vary based on the encrypt conn. enablement.
    The specific port knowledge is assigned at the CassandraConfigReader object level.
    The invoked is_cassandra_up includes retry logic.
    """

    try:
        if not cassandra or not host:
            return False

        native_port = cassandra.native_port
        storage_port = cassandra.storage_port
        rpc_port = cassandra.rpc_port
        logging.debug('Checking Cassandra health type: {} for host: {} '
                      'release_ver: {} native_port: {} storage_port: {}, rpc_port: {} '
                      .format(check_type, host, cassandra.release_version, native_port, storage_port, rpc_port))

        if check_type == 'thrift':
            return is_cassandra_up(host, rpc_port)
        elif check_type == 'all':
            # Port checks to include all of: rpc, native, and storage(gossip) health.
            return \
                is_cassandra_up(host, rpc_port) and \
                is_cassandra_up(host, native_port) and is_cassandra_up(host, storage_port)
        else:
            # Default port checks for native OR storage(gossip).
            return is_cassandra_up(host, native_port) or is_cassandra_up(host, storage_port)

    except Exception as e:
        logging.debug('Cassandra not found to be healthy during check', exc_info=e)

    return False


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
    except Exception as e:
        logging.debug('CCM native transport is not up yet', exc_info=e)
        return False


def is_open(host, port):
    is_accessible = False
    timeout = 3
    s = None
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(timeout)
        s.connect((host, port))
        s.shutdown(socket.SHUT_RDWR)
        is_accessible = True

    # If cassandra is not running but the host is up, host may choose to silently drop inbound connections to the
    #   closed port or may respond with a RST indicating that the connection was refused.
    # ConnectionRefusedError: [Errno 111] Connection refused
    except ConnectionRefusedError:
        logging.debug("Port '{port}' is closed, assuming '{host}' is down.".format(host=host, port=port))
    except socket.error:
        logging.debug("Could not open socket to port '{port}' on '{host}'. Assuming host is down."
                      .format(host=host, port=port))
    finally:
        try:
            if s:
                s.close()
        except os.error:
            logging.debug("Socket used for Port '{port}' check failed to close for host '{host}'. Assuming host is down"
                          .format(host=host, port=port))
    return is_accessible


@retry(stop_max_attempt_number=5, wait_exponential_multiplier=5000, wait_exponential_max=120000)
def is_cassandra_up(host, port):
    if is_open(host, port):
        return True
    else:
        logging.debug('The node {} is not up yet...'.format(host))
    return False


class CassandraNodeNotUpError(Exception):
    """
    Raised when it cannot be verified that a node is up by checking either nodetool statusbinary and/or
    nodetool statusthrift

    Attributes:
        host -- the hostname or ip address of the node
        attempts -- the number of times the check was performed
    """

    def __init__(self, host):
        msg = 'Cassandra node {} is still down...'.format(host)
        super(CassandraNodeNotUpError, self).__init__(msg)


class CassandraNodeNotDownError(Exception):
    """
    Raised when we give up waiting on a node to go down, meaning nodetool statusbinary and/or statusthrift keep
    reporting open ports.
    """

    def __init__(self, host):
        msg = 'Cassandra node {} is still up...'.format(host)
        super(CassandraNodeNotDownError, self).__init__(msg)


class CassandraCqlSessionException(Exception):
    """
    Raised when we detect an issue establishing a cql session.
    """

    def __init__(self, host):
        msg = 'Cassandra node {} cql session issue...'.format(host)
        super(Exception, self).__init__(msg)
