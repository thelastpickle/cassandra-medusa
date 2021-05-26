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
import _socket
import configparser
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock
from unittest.mock import Mock

import yaml
from cassandra.metadata import Murmur3Token

import medusa.cassandra_utils
from medusa.cassandra_utils import CqlSession, SnapshotPath, Cassandra, is_cassandra_healthy
from medusa.config import MedusaConfig, StorageConfig, CassandraConfig, GrpcConfig, _namedtuple_from_dict, \
    KubernetesConfig, ChecksConfig
from medusa.host_man import HostMan
from medusa.nodetool import Nodetool


class CassandraUtilsTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def setUp(self):
        config = configparser.ConfigParser(interpolation=None)
        config['storage'] = {
            'host_file_separator': ','
        }
        config['cassandra'] = {
            'resolve_ip_addresses': 'False'
        }
        config["grpc"] = {
            "enabled": "0"
        }
        config['kubernetes'] = {
            "enabled": "0"
        }
        self.config = MedusaConfig(
            file_path=None,
            storage=_namedtuple_from_dict(StorageConfig, config['storage']),
            monitoring={},
            cassandra=_namedtuple_from_dict(CassandraConfig, config['cassandra']),
            ssh=None,
            checks=None,
            logging=None,
            grpc=_namedtuple_from_dict(GrpcConfig, config['grpc']),
            kubernetes=_namedtuple_from_dict(KubernetesConfig, config['kubernetes']),
        )

    def test_tokenmap_one_token(self):
        host = Mock()
        host.is_up = True
        host.address = '127.0.0.1'
        session = Mock()
        session.cluster = Mock()
        session.cluster.contact_points = ["localhost"]
        session.cluster.metadata.token_map.token_to_host_owner = {
            Murmur3Token(-9): host
        }
        s = CqlSession(session, resolve_ip_addresses=self.config.cassandra.resolve_ip_addresses)
        token_map = s.tokenmap()
        self.assertEqual(
            {'127.0.0.1': {'is_up': True, 'tokens': [-9]}},
            token_map
        )

    def test_tokenmap_vnodes(self):
        host = Mock()
        host.is_up = True
        host.address = '127.0.0.1'
        session = Mock()
        session.cluster = Mock()
        session.cluster.contact_points = ["localhost"]
        session.cluster.metadata.token_map.token_to_host_owner = {
            Murmur3Token(-9): host,
            Murmur3Token(-6): host,
            Murmur3Token(0): host
        }
        s = CqlSession(session, resolve_ip_addresses=False)
        token_map = s.tokenmap()
        self.assertEqual(True, token_map["127.0.0.1"]["is_up"])
        self.assertEqual([-9, -6, 0], sorted(token_map["127.0.0.1"]["tokens"]))

    def test_tokenmap_two_dc(self):
        host_a = Mock()
        host_a.is_up = True
        host_a.address = '127.0.0.1'
        host_a.datacenter = "dcA"

        host_b = Mock()
        host_b.is_up = True
        host_b.address = '127.0.0.2'
        host_b.datacenter = "dcB"

        session = Mock()
        session.cluster = Mock()
        session.cluster.contact_points = ["127.0.0.1"]
        session.cluster.metadata.token_map.token_to_host_owner = {
            Murmur3Token(-6): host_a,
            Murmur3Token(6): host_b
        }
        s = CqlSession(session, resolve_ip_addresses=False)
        token_map = s.tokenmap()
        self.assertEqual(
            {'127.0.0.1': {'is_up': True, 'tokens': [-6]}},
            token_map
        )

    def test_snapshot_path_lists_hidden_files(self):
        with tempfile.TemporaryDirectory() as root:
            snapshot_path = root / Path('ks') / 't' / 'snapshot' / 'snapshot_tag'
            index_path = snapshot_path / '.t_idx'
            sstable_path = snapshot_path / 'xx-20-Data.lb'
            index_sstable_path = index_path / 'xx-21-Data.lb'
            index_path.mkdir(parents=True)  # create the directory structure
            sstable_path.touch()  # create a fake SSTable file
            index_sstable_path.touch()  # create a fake index SSTable file

            # create a new SnapshotPath and see if it returns both normal and index SSTables
            sp = SnapshotPath(Path(snapshot_path), 'ks', 't')
            all_files = list(sp.list_files())
            self.assertEqual(
                2,
                len(all_files)
            )
            self.assertTrue(sstable_path in all_files)
            self.assertTrue(index_sstable_path in all_files)

    def test_nodetool_command_without_parameter(self):
        config = configparser.ConfigParser(interpolation=None)
        config['cassandra'] = {
        }
        config["grpc"] = {
            "enabled": "0"
        }
        config['kubernetes'] = {
            "enabled": "0"
        }
        medusa_config = MedusaConfig(
            file_path=None,
            storage=None,
            monitoring=None,
            cassandra=_namedtuple_from_dict(CassandraConfig, config['cassandra']),
            ssh=None,
            checks=None,
            logging=None,
            grpc=_namedtuple_from_dict(GrpcConfig, config['grpc']),
            kubernetes=_namedtuple_from_dict(KubernetesConfig, config['kubernetes']),
        )
        n = Nodetool(medusa_config.cassandra).nodetool
        self.assertEqual(n, ['nodetool'])

    def test_nodetool_command_with_parameters(self):
        config = configparser.ConfigParser(interpolation=None)
        config['cassandra'] = {
            'nodetool_ssl': 'true',
            'nodetool_username': 'cassandra',
            'nodetool_password': 'password',
            'nodetool_password_file_path': '/etc/cassandra/jmx.password',
            'nodetool_host': '127.0.0.1',
            'nodetool_port': '7199'
        }
        config["grpc"] = {
            "enabled": "0"
        }
        config['kubernetes'] = {
            "enabled": "0"
        }
        medusa_config = MedusaConfig(
            file_path=None,
            storage=None,
            monitoring=None,
            cassandra=_namedtuple_from_dict(CassandraConfig, config['cassandra']),
            ssh=None,
            checks=None,
            logging=None,
            grpc=_namedtuple_from_dict(GrpcConfig, config['grpc']),
            kubernetes=_namedtuple_from_dict(KubernetesConfig, config['kubernetes']),
        )
        n = Nodetool(medusa_config.cassandra).nodetool
        expected = ['nodetool', '--ssl', '-u', 'cassandra', '-pw', 'password', '-pwf', '/etc/cassandra/jmx.password',
                    '-h', '127.0.0.1', '-p', '7199']
        self.assertEqual(n, expected)

    def test_nodetool_command_with_ssl_false(self):
        config = configparser.ConfigParser(interpolation=None)
        config['cassandra'] = {
            'nodetool_ssl': 'false',
            'nodetool_username': 'cassandra',
            'nodetool_password': 'password',
            'nodetool_password_file_path': '/etc/cassandra/jmx.password',
            'nodetool_host': '127.0.0.1',
            'nodetool_port': '7199'
        }
        config["grpc"] = {
            "enabled": "0"
        }
        config['kubernetes'] = {
            "enabled": "0"
        }
        medusa_config = MedusaConfig(
            file_path=None,
            storage=None,
            monitoring=None,
            cassandra=_namedtuple_from_dict(CassandraConfig, config['cassandra']),
            ssh=None,
            checks=None,
            logging=None,
            grpc=_namedtuple_from_dict(GrpcConfig, config['grpc']),
            kubernetes=_namedtuple_from_dict(KubernetesConfig, config['kubernetes']),
        )
        n = Nodetool(medusa_config.cassandra).nodetool
        expected = ['nodetool', '-u', 'cassandra', '-pw', 'password', '-pwf', '/etc/cassandra/jmx.password',
                    '-h', '127.0.0.1', '-p', '7199']
        self.assertEqual(n, expected)

    def test_yaml_token_enforcement_no_tokens(self):
        with open('tests/resources/yaml/original/cassandra_no_tokens.yaml', 'r') as f:
            shutil.copyfile('tests/resources/yaml/original/cassandra_no_tokens.yaml',
                            'tests/resources/yaml/work/cassandra_no_tokens.yaml')
        config = configparser.ConfigParser(interpolation=None)
        config['cassandra'] = {
            'config_file': os.path.join(os.path.dirname(__file__), 'resources/yaml/work/cassandra_no_tokens.yaml'),
            'start_cmd': '/etc/init.d/cassandra start',
            'stop_cmd': '/etc/init.d/cassandra stop',
            'is_ccm': '1'
        }
        config["grpc"] = {
            "enabled": "0"
        }
        config['kubernetes'] = {
            "enabled": "0"
        }
        medusa_config = MedusaConfig(
            file_path=None,
            storage=None,
            monitoring=None,
            cassandra=_namedtuple_from_dict(CassandraConfig, config['cassandra']),
            ssh=None,
            checks=None,
            logging=None,
            grpc=_namedtuple_from_dict(GrpcConfig, config['grpc']),
            kubernetes=_namedtuple_from_dict(KubernetesConfig, config['kubernetes']),
        )

        cassandra = Cassandra(medusa_config)
        tokens = ['1', '2', '3']
        cassandra.replace_tokens_in_cassandra_yaml_and_disable_bootstrap(tokens)

        with open('tests/resources/yaml/work/cassandra_no_tokens.yaml', 'r') as f:
            modified_yaml = yaml.load(f, Loader=yaml.BaseLoader)
            self.assertEqual(modified_yaml.get('num_tokens'), '3')
            self.assertEqual(modified_yaml.get('initial_token'), '1,2,3')
            self.assertEqual(modified_yaml.get('auto_bootstrap'), 'false')

    def test_yaml_token_enforcement_with_tokens(self):
        with open('tests/resources/yaml/original/cassandra_with_tokens.yaml', 'r') as f:
            shutil.copyfile('tests/resources/yaml/original/cassandra_with_tokens.yaml',
                            'tests/resources/yaml/work/cassandra_with_tokens.yaml')
        config = configparser.ConfigParser(interpolation=None)
        config['cassandra'] = {
            'config_file': os.path.join(os.path.dirname(__file__), 'resources/yaml/work/cassandra_with_tokens.yaml'),
            'start_cmd': '/etc/init.d/cassandra start',
            'stop_cmd': '/etc/init.d/cassandra stop',
            'is_ccm': '1'
        }
        config["grpc"] = {
            "enabled": "0"
        }
        config['kubernetes'] = {
            "enabled": "0"
        }
        medusa_config = MedusaConfig(
            file_path=None,
            storage=None,
            monitoring=None,
            cassandra=_namedtuple_from_dict(CassandraConfig, config['cassandra']),
            ssh=None,
            checks=None,
            logging=None,
            grpc=_namedtuple_from_dict(GrpcConfig, config['grpc']),
            kubernetes=_namedtuple_from_dict(KubernetesConfig, config['kubernetes']),
        )

        cassandra = Cassandra(medusa_config)
        tokens = ['1', '2', '3']
        cassandra.replace_tokens_in_cassandra_yaml_and_disable_bootstrap(tokens)

        with open('tests/resources/yaml/work/cassandra_with_tokens.yaml', 'r') as f:
            modified_yaml = yaml.load(f, Loader=yaml.BaseLoader)
            self.assertEqual(modified_yaml.get('num_tokens'), '3')
            self.assertEqual(modified_yaml.get('initial_token'), '1,2,3')
            self.assertEqual(modified_yaml.get('auto_bootstrap'), 'false')

    def test_yaml_token_enforcement_with_tokens_and_autobootstrap(self):
        with open('tests/resources/yaml/original/cassandra_with_tokens.yaml', 'r') as f:
            shutil.copyfile('tests/resources/yaml/original/cassandra_with_tokens_and_autobootstrap.yaml',
                            'tests/resources/yaml/work/cassandra_with_tokens_and_autobootstrap.yaml')
        config = configparser.ConfigParser(interpolation=None)
        config['cassandra'] = {
            'config_file': os.path.join(os.path.dirname(__file__),
                                        'resources/yaml/work/cassandra_with_tokens_and_autobootstrap.yaml'),
            'start_cmd': '/etc/init.d/cassandra start',
            'stop_cmd': '/etc/init.d/cassandra stop',
            'is_ccm': '1'
        }
        config["grpc"] = {
            "enabled": "0"
        }
        config['kubernetes'] = {
            "enabled": "0"
        }
        medusa_config = MedusaConfig(
            file_path=None,
            storage=None,
            monitoring=None,
            cassandra=_namedtuple_from_dict(CassandraConfig, config['cassandra']),
            ssh=None,
            checks=None,
            logging=None,
            grpc=_namedtuple_from_dict(GrpcConfig, config['grpc']),
            kubernetes=_namedtuple_from_dict(KubernetesConfig, config['kubernetes']),
        )

        cassandra = Cassandra(medusa_config)
        tokens = ['1', '2', '3']
        cassandra.replace_tokens_in_cassandra_yaml_and_disable_bootstrap(tokens)

        with open('tests/resources/yaml/work/cassandra_with_tokens_and_autobootstrap.yaml', 'r') as f:
            modified_yaml = yaml.load(f, Loader=yaml.BaseLoader)
            self.assertEqual(modified_yaml.get('num_tokens'), '3')
            self.assertEqual(modified_yaml.get('initial_token'), '1,2,3')
            self.assertEqual(modified_yaml.get('auto_bootstrap'), 'false')

    def test_seed_parsing(self):
        shutil.copyfile('tests/resources/yaml/original/cassandra_with_tokens_and_autobootstrap.yaml',
                        'tests/resources/yaml/work/cassandra_with_tokens_and_autobootstrap.yaml')
        config = configparser.ConfigParser(interpolation=None)
        config['cassandra'] = {
            'config_file': os.path.join(os.path.dirname(__file__),
                                        'resources/yaml/work/cassandra_with_tokens_and_autobootstrap.yaml'),
            'start_cmd': '/etc/init.d/cassandra start',
            'stop_cmd': '/etc/init.d/cassandra stop',
            'is_ccm': '1'
        }
        config["grpc"] = {
            "enabled": "0"
        }
        config['kubernetes'] = {
            "enabled": "0"
        }
        medusa_config = MedusaConfig(
            file_path=None,
            storage=None,
            monitoring=None,
            cassandra=_namedtuple_from_dict(CassandraConfig, config['cassandra']),
            ssh=None,
            checks=None,
            logging=None,
            grpc=_namedtuple_from_dict(GrpcConfig, config['grpc']),
            kubernetes=_namedtuple_from_dict(KubernetesConfig, config['kubernetes']),
        )

        cassandra = Cassandra(medusa_config)
        self.assertEqual(["127.0.0.1", "127.0.0.2"], sorted(cassandra.seeds))

    def test_parsing_custom_seed_provider(self):
        # patch a sample yaml to have a custom seed provider
        with open('tests/resources/yaml/original/cassandra_with_tokens.yaml', 'r') as fi:
            yaml_dict = yaml.load(fi, Loader=yaml.FullLoader)
            yaml_dict['seed_provider'] = [
                {'class_name': 'org.foo.bar.CustomSeedProvider'}
            ]
            with open('tests/resources/yaml/work/cassandra_with_custom_seedprovider.yaml', 'w') as fo:
                yaml.safe_dump(yaml_dict, fo)

        # pass the patched yaml to cassandra config
        config = configparser.ConfigParser(interpolation=None)
        config['cassandra'] = {
            'config_file': os.path.join(os.path.dirname(__file__),
                                        'resources/yaml/work/cassandra_with_custom_seedprovider.yaml'),
            'start_cmd': '/etc/init.d/cassandra start',
            'stop_cmd': '/etc/init.d/cassandra stop',
            'is_ccm': '1'
        }
        config["grpc"] = {
            "enabled": "0"
        }
        config['kubernetes'] = {
            "enabled": "0"
        }
        medusa_config = MedusaConfig(
            file_path=None,
            storage=None,
            monitoring=None,
            cassandra=_namedtuple_from_dict(CassandraConfig, config['cassandra']),
            ssh=None,
            checks=None,
            logging=None,
            grpc=_namedtuple_from_dict(GrpcConfig, config['grpc']),
            kubernetes=_namedtuple_from_dict(KubernetesConfig, config['kubernetes']),
        )
        cassandra = Cassandra(medusa_config)
        self.assertEqual([], sorted(cassandra.seeds))

    def test_cassandra_non_encrypt_comm_ports(self):
        c = Cassandra(self.get_simple_medusa_config('resources/yaml/original/cassandra-no-encrypt.yaml'),
                      release_version="4")

        # no encryption, use provided from storage_port even though having ssl_storage_port as well.
        self.assertEqual(c.storage_port, 15000)

        self.assertEqual(c.native_port, 9777)
        self.assertEqual(c.rpc_port, 9160)

    def test_cassandra_internode_encryption_v3_ports(self):
        c = Cassandra(self.get_simple_medusa_config('resources/yaml/original/'
                                                    'cassandra-internode-encrypt.yaml'), release_version="3")

        # Uses ssl_storage_port value
        self.assertEqual(c.storage_port, 10001)
        self.assertEqual(c.native_port, 9042)
        self.assertEqual(c.rpc_port, 9160)

    def test_cassandra_internode_encryption_v4_ports(self):
        c = Cassandra(self.get_simple_medusa_config('resources/yaml/original/'
                                                    'cassandra-internode-encrypt.yaml'), release_version="4")

        # Uses ssl_storage_port value
        self.assertEqual(c.storage_port, 10001)
        self.assertEqual(c.native_port, 9042)
        self.assertEqual(c.rpc_port, 9160)

    def test_cassandra_internode_encrypt_v3_default_ports(self):
        c = Cassandra(self.get_simple_medusa_config('resources/yaml/original/'
                                                    'cassandra-internode-encrypt-default.yaml'), release_version="3")

        # Secure connection desired.
        # Both ssl_storage_port and storage_port are not defined.
        # Based v3 release version, should default to 7001.
        self.assertEqual(c.storage_port, 7001)
        self.assertEqual(c.native_port, 9042)
        self.assertEqual(c.rpc_port, 9160)

    def test_cassandra_internode_encrypt_v4_default_ports(self):
        c = Cassandra(self.get_simple_medusa_config('resources/yaml/original/'
                                                    'cassandra-internode-encrypt-default.yaml'), release_version="4")

        # Secure connection desired.
        # Both ssl_storage_port and storage_port are not defined.
        # Based on v4 release version, should default to 7001.
        self.assertEqual(c.storage_port, 7001)
        self.assertEqual(c.native_port, 9042)
        self.assertEqual(c.rpc_port, 9160)

    def test_cassandra_internode_encrypt_nossl_v3_default_ports(self):
        c = Cassandra(self.get_simple_medusa_config('resources/yaml/original/'
                                                    'cassandra-internode-encrypt-nossl-default.yaml'),
                      release_version="3")

        # Secure connection desired.
        # The ssl_storage_port is not defined.
        # Based on not being c* v4, this will use default port for encryption.
        self.assertEqual(c.storage_port, 7001)
        self.assertEqual(c.native_port, 9042)
        self.assertEqual(c.rpc_port, 9160)

    def test_cassandra_internode_encrypt_nossl_v4_default_ports(self):
        c = Cassandra(self.get_simple_medusa_config('resources/yaml/original/'
                                                    'cassandra-internode-encrypt-nossl-default.yaml'),
                      release_version="4")

        # Secure connection desired.
        # The ssl_storage_port is not defined.
        # Based on being c* v4, this will use the value of the specified storage_port
        self.assertEqual(c.storage_port, 8675)
        self.assertEqual(c.native_port, 9042)
        self.assertEqual(c.rpc_port, 9160)

    def test_cassandra_client_encryption_enabled_default_port(self):
        c = Cassandra(self.get_simple_medusa_config('resources/yaml/original/cassandra-client-encrypt-default.yaml'))

        # Both ports are not assigned, using default
        self.assertEqual(c.native_port, 9142)
        self.assertEqual(c.rpc_port, 9160)
        self.assertEqual(c.storage_port, 7000)

    def test_cassandra_client_encryption_enabled_reuse_port(self):
        c = Cassandra(self.get_simple_medusa_config('resources/yaml/original/cassandra-client-encrypt.yaml'))

        # Expecting with client_encryption_options enabled, and not defining value for native_transport_port_ssl
        # use native_transport_port value as defined.
        self.assertEqual(c.native_port, 9123)
        self.assertEqual(c.rpc_port, 9160)
        self.assertEqual(c.storage_port, 7000)

    def test_cassandra_client_encryption_enabled_ssl_port(self):
        c = Cassandra(self.get_simple_medusa_config('resources/yaml/original/cassandra-client-encrypt-sslport.yaml'))

        # server_encryption_options /internode set to'all', with both
        # ports native_transport_port_ssl AND native_transport_port
        # defined.  Expected to use native_transport_port_ssl
        self.assertEqual(c.storage_port, 7001)
        self.assertEqual(c.native_port, 18675)
        self.assertEqual(c.rpc_port, 9160)

    def test_cassandra_missing_native_port(self):
        c = Cassandra(self.get_simple_medusa_config('resources/yaml/original/cassandra-missing-native-port.yaml'))

        # Case where cient_encryption_options enabled, but no native ports defined.
        self.assertEqual(c.native_port, 9142)

    @staticmethod
    def get_simple_medusa_config(yaml_file='resources/yaml/original/default-c4.yaml', is_ccm_active='1',
                                 config_checks=None):

        config = configparser.ConfigParser(interpolation=None)
        config['cassandra'] = {
            'config_file': os.path.join(os.path.dirname(__file__),
                                        yaml_file),
            'start_cmd': '/etc/init.d/cassandra start',
            'stop_cmd': '/etc/init.d/cassandra stop',
            'is_ccm': is_ccm_active
        }
        config["grpc"] = {
            "enabled": "0"
        }
        config['kubernetes'] = {
            "enabled": "0"
        }

        checks_set = None
        if config_checks:
            config["checks"] = config_checks
            checks_set = _namedtuple_from_dict(ChecksConfig, config['checks'])

        medusa_config = MedusaConfig(
            file_path=None,
            storage=None,
            monitoring=None,
            cassandra=_namedtuple_from_dict(CassandraConfig, config['cassandra']),
            grpc=_namedtuple_from_dict(GrpcConfig, config['grpc']),
            kubernetes=_namedtuple_from_dict(KubernetesConfig, config['kubernetes']),
            ssh=None,
            checks=checks_set,
            logging=None,
        )
        return medusa_config

    @mock.patch.object(medusa.cassandra_utils, "is_ccm_healthy")
    @mock.patch.object(medusa.cassandra_utils, "is_cassandra_healthy")
    def test_is_node_up(self, fm_cass, fm_ccm):
        fm_ccm.return_value = True
        fm_cass.return_value = True

        host = "h1"
        HostMan.set_release_version("1.2.3")
        medusa_config_v4 = self.get_simple_medusa_config(is_ccm_active="1",
                                                         yaml_file='resources/yaml/original/default-c4.yaml',
                                                         config_checks={"health_check": "all"})

        # TODO Setup the hostman to properly associate for this host to resolve to a release version
        # When ccm is active, expect ccm health check
        self.assertTrue(medusa.cassandra_utils.is_node_up(medusa_config_v4, host))

        assert fm_cass.call_count == 0
        assert fm_ccm.call_count == 1

        # reset call counts
        fm_ccm.call_count = 0
        fm_cass.call_count = 0

        # And, when ccm is not active, expect a cassandra health check.
        medusa_config_v4 = self.get_simple_medusa_config(is_ccm_active="0",
                                                         yaml_file='resources/yaml/original/default-c4.yaml',
                                                         config_checks={"health_check": "all"})

        self.assertTrue(medusa.cassandra_utils.is_node_up(medusa_config_v4, host))
        assert fm_cass.call_count == 1
        assert fm_ccm.call_count == 0

        # generate exception as side-effect
        fm_cass.side_effect = Exception("Not good.")

        # And, when an exception occurs during the check, expect a return of False
        self.assertFalse(medusa.cassandra_utils.is_node_up(medusa_config_v4, host))

    @mock.patch.object(medusa.cassandra_utils, "is_ccm_up")
    def test_is_ccm_healthy(self, fm):
        fm.return_value = True
        self.assertTrue(medusa.cassandra_utils.is_ccm_healthy("thrift"))
        assert fm.call_count == 1
        fm.call_count = 0

        self.assertTrue(medusa.cassandra_utils.is_ccm_healthy("all"))
        assert fm.call_count == 2
        fm.call_count = 0

        self.assertTrue(medusa.cassandra_utils.is_ccm_healthy("whatever"))
        assert fm.call_count == 1
        fm.call_count = 0

        fm.side_effect = Exception("Not good.")
        self.assertFalse(medusa.cassandra_utils.is_ccm_healthy("all"))

    @mock.patch.object(medusa.cassandra_utils, "is_cassandra_up")
    def test_is_cassandra_v2_healthy(self, fm):
        fm.return_value = True
        host = Mock()

        medusa_config_v2 = self.get_simple_medusa_config(is_ccm_active="0",
                                                         yaml_file='resources/yaml/original/default-c2.yaml')
        cassandra_v2 = Cassandra(medusa_config_v2, release_version="2")

        # When c* version 2 is used, check for the port values.
        self.assertTrue(is_cassandra_healthy("all", cassandra_v2, host))
        assert fm.call_count == 3

    @mock.patch.object(medusa.cassandra_utils, "is_cassandra_up")
    def test_is_cassandra_v3_healthy(self, fm):
        fm.return_value = True
        host = Mock()

        # Not using ccm, directing check for cassandra health.
        medusa_config_v3 = self.get_simple_medusa_config(is_ccm_active="0",
                                                         yaml_file='resources/yaml/original/default-c3.yaml')
        cassandra_v3 = Cassandra(medusa_config_v3, release_version="3")

        # When c* version 3 is used, check for the port values.
        self.assertTrue(is_cassandra_healthy("all", cassandra_v3, host))
        assert fm.call_count == 3

    @mock.patch.object(medusa.cassandra_utils, "is_cassandra_up")
    def test_is_cassandra_v4_healthy(self, fm):
        fm.return_value = True
        host = Mock()

        # Not using ccm, directing check for cassandra health.
        medusa_config_v4 = self.get_simple_medusa_config(is_ccm_active="0",
                                                         yaml_file='resources/yaml/original/default-c4.yaml')

        cassandra_v4 = Cassandra(medusa_config_v4, release_version="4")

        # When c* version 4 is used, check for the port values.
        self.assertTrue(is_cassandra_healthy("all", cassandra_v4, host))
        assert fm.call_count == 3

    @mock.patch.object(medusa.cassandra_utils, "is_cassandra_up")
    def test_is_cassandra_healthy_check_types(self, fm):

        host = Mock()

        # Not using ccm, directing check for cassandra health.
        medusa_config_v4 = self.get_simple_medusa_config(is_ccm_active="0",
                                                         yaml_file='resources/yaml/original/default-c4.yaml')

        cassandra_v4 = Cassandra(medusa_config_v4, release_version="4")

        # When c* version 4 is used, check for the port values.
        fm.return_value = True
        self.assertTrue(is_cassandra_healthy("all", cassandra_v4, host))
        self.assertTrue(is_cassandra_healthy("thrift", cassandra_v4, host))
        self.assertTrue(is_cassandra_healthy("unknown", cassandra_v4, host))

    @mock.patch.object(medusa.cassandra_utils, "is_cassandra_up")
    def test_is_cassandra_healthy_check_type_unknown2(self, is_cassandra_up_mock):
        host = Mock()
        # Not using ccm, directing check for cassandra health.
        medusa_config_v4 = self.get_simple_medusa_config(is_ccm_active="0",
                                                         yaml_file='resources/yaml/original/default-c4.yaml')
        cassandra_v4 = Cassandra(medusa_config_v4, release_version="4")

        is_cassandra_up_mock.return_value = True
        self.assertTrue(is_cassandra_healthy("not-thrift-and-not-all-check", cassandra_v4, host))
        assert is_cassandra_up_mock.call_count == 1

        is_cassandra_up_mock.call_count = 0
        is_cassandra_up_mock.return_value = False
        self.assertFalse(is_cassandra_healthy("not-thrift-and-not-all-check", cassandra_v4, host))
        assert is_cassandra_up_mock.call_count == 2

    @mock.patch.object(medusa.cassandra_utils, "is_open")
    def test_is_cassandra_healthy_check_type_unknown(self, is_open):
        host = Mock()
        # Not using ccm, directing check for cassandra health.
        medusa_config_v4 = self.get_simple_medusa_config(is_ccm_active="0",
                                                         yaml_file='resources/yaml/original/default-c4.yaml')
        cassandra_v4 = Cassandra(medusa_config_v4, release_version="4")

        is_open.return_value = True
        self.assertTrue(is_cassandra_healthy("not-thrift-and-not-all-check", cassandra_v4, host))
        assert is_open.call_count == 1

        is_open.call_count = 0
        is_open.return_value = False
        self.assertFalse(is_cassandra_healthy("not-thrift-and-not-all-check", cassandra_v4, host))
        assert is_open.call_count == 2

    def test_is_cassandra_healthy_invalid_input(self):
        host = Mock()
        # Not using ccm, directing check for cassandra health.
        medusa_config_v4 = self.get_simple_medusa_config(is_ccm_active="0",
                                                         yaml_file='resources/yaml/original/default-c4.yaml')
        cassandra_v4 = Cassandra(medusa_config_v4, release_version="4")

        # invalid cassandra input
        self.assertFalse(is_cassandra_healthy("all", {}, host))

        # invalid host input
        self.assertFalse(is_cassandra_healthy("all", cassandra_v4, {}))

    def test_is_cass_default_ports_valid(self):
        # Not using ccm, directing check for cassandra health.
        medusa_config_v2 = self.get_simple_medusa_config(is_ccm_active="0",
                                                         yaml_file='resources/yaml/original/default-c2.yaml')

        # Not using ccm, directing check for cassandra health.
        medusa_config_v3 = self.get_simple_medusa_config(is_ccm_active="0",
                                                         yaml_file='resources/yaml/original/default-c3.yaml')
        # Not using ccm, directing check for cassandra health.
        medusa_config_v4 = self.get_simple_medusa_config(is_ccm_active="0",
                                                         yaml_file='resources/yaml/original/default-c4.yaml')

        cassandra_v2 = Cassandra(medusa_config_v2, release_version="2")
        cassandra_v3 = Cassandra(medusa_config_v3, release_version="3")
        cassandra_v4 = Cassandra(medusa_config_v4, release_version="4")

        assert cassandra_v2.rpc_port == 9160
        assert cassandra_v2.native_port == 9042
        assert cassandra_v2.storage_port == 7000

        assert cassandra_v3.rpc_port == 9160
        assert cassandra_v3.native_port == 9042
        assert cassandra_v3.storage_port == 7000

        assert cassandra_v4.rpc_port == 9160
        assert cassandra_v4.native_port == 9042
        assert cassandra_v4.storage_port == 7000

    @mock.patch.object(medusa.cassandra_utils.Cassandra, "replace_tokens_in_cassandra_yaml_and_disable_bootstrap")
    @mock.patch.object(subprocess, "check_output")
    def test_cassandra_start(self, subproc_mock, replace_tokens_mock):

        subproc_mock.return_value = None
        replace_tokens_mock.return_value = None
        subproc_mock.call_count = 0
        replace_tokens_mock.call_count = 0

        medusa_config_v4 = self.get_simple_medusa_config(is_ccm_active="0",
                                                         yaml_file='resources/yaml/original/default-c4.yaml')
        cassandra_v4 = Cassandra(medusa_config_v4, release_version="4")
        cassandra_v4.start(['test-token'])

        # When start invoked with ccm active, expect both check_output and replace tokens are
        # invoked.
        assert subproc_mock.call_count == 1 and replace_tokens_mock.call_count == 1

        # And, when start invoked with ccm active, expect that only check_output is called
        # as there is no need to replace tokens in c* yaml and disabling bootstrap.
        subproc_mock.call_count = 0
        replace_tokens_mock.call_count = 0

        medusa_config_v4 = self.get_simple_medusa_config(is_ccm_active="1",
                                                         yaml_file='resources/yaml/original/default-c4.yaml')
        cassandra_v4 = Cassandra(medusa_config_v4, release_version="4")
        cassandra_v4.start(['test-token'])
        assert subproc_mock.call_count == 1 and replace_tokens_mock.call_count == 0

    @mock.patch.object(subprocess, "check_output")
    def test_is_ccm_up(self, check_mock):

        check_mock.return_value = "running"

        self.assertTrue(medusa.cassandra_utils.is_ccm_up([], "test-nt-cmd"))

        check_mock.return_value = "not running"
        self.assertFalse(medusa.cassandra_utils.is_ccm_up([], "test-nt-cmd"))

        check_mock.side_effect = Exception("unknown issue reported")
        self.assertFalse(medusa.cassandra_utils.is_ccm_up([], "test-nt-cmd"))

    @staticmethod
    def socket_connect_exception():
        return _socket.error("socket connect not good")

    @staticmethod
    def socket_close_exception():
        return _socket.error("socket close not good")

    @mock.patch('socket.socket')
    def test_is_open_valid(self, mock_socket):

        mock_instance = mock_socket.return_value
        mock_instance.socket.side_effect = None
        mock_instance.connect.side_effect = None
        mock_instance.shutdown.side_effect = None
        mock_instance.close.side_effect = None

        self.assertTrue(medusa.cassandra_utils.is_open("test-host", 2001))

        assert mock_socket.call_count == 1
        assert mock_instance.connect.call_count == 1
        assert mock_instance.shutdown.call_count == 1
        assert mock_instance.close.call_count == 1

    @mock.patch('socket.socket')
    def test_is_open_failed_connect(self, mock_socket):

        mock_instance = mock_socket.return_value
        mock_instance.socket.side_effect = None
        mock_instance.connect.side_effect = self.socket_connect_exception()
        mock_instance.shutdown.side_effect = None
        mock_instance.close.side_effect = None

        self.assertFalse(medusa.cassandra_utils.is_open("test-host", 2001))

        assert mock_socket.call_count == 1
        assert mock_instance.connect.call_count == 1
        assert mock_instance.shutdown.call_count == 0
        assert mock_instance.close.call_count == 1

    @mock.patch('socket.socket')
    def test_is_open_failed_close(self, mock_socket):

        mock_instance = mock_socket.return_value
        mock_instance.socket.side_effect = None
        mock_instance.connect.side_effect = None
        mock_instance.shutdown.side_effect = None
        mock_instance.close.side_effect = self.socket_close_exception()

        # Expect we get a good is_open status even though socket close failed.
        self.assertTrue(medusa.cassandra_utils.is_open("test-host", 2001))

        assert mock_socket.call_count == 1
        assert mock_instance.connect.call_count == 1
        assert mock_instance.shutdown.call_count == 1
        assert mock_instance.close.call_count == 1

    if __name__ == '__main__':
        unittest.main()
