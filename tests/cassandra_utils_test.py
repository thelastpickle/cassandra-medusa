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

import configparser
import os
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

import yaml
from cassandra.metadata import Murmur3Token

from medusa.cassandra_utils import CqlSession, SnapshotPath, Cassandra
from medusa.config import MedusaConfig, StorageConfig, CassandraConfig, GrpcConfig, _namedtuple_from_dict, \
    KubernetesConfig
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
            'resolve_ip_addresses': False
        }
        config["grpc"] = {
            "enabled": "0"
        }
        config['kubernetes'] = {
            "enabled": "0"
        }
        self.config = MedusaConfig(
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
        hostA = Mock()
        hostA.is_up = True
        hostA.address = '127.0.0.1'
        hostA.datacenter = "dcA"

        hostB = Mock()
        hostB.is_up = True
        hostB.address = '127.0.0.2'
        hostB.datacenter = "dcB"

        session = Mock()
        session.cluster = Mock()
        session.cluster.contact_points = ["127.0.0.1"]
        session.cluster.metadata.token_map.token_to_host_owner = {
            Murmur3Token(-6): hostA,
            Murmur3Token(6): hostB
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
        config = configparser.ConfigParser(interpolation=None)
        config['cassandra'] = {
            'config_file': os.path.join(os.path.dirname(__file__), 'resources/yaml/original/cassandra-no-encrypt.yaml'),
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
            storage=None,
            monitoring=None,
            cassandra=_namedtuple_from_dict(CassandraConfig, config['cassandra']),
            grpc=_namedtuple_from_dict(GrpcConfig, config['grpc']),
            kubernetes=_namedtuple_from_dict(KubernetesConfig, config['kubernetes']),
            ssh=None,
            checks=None,
            logging=None,
        )

        c = Cassandra(medusa_config)

        # no encryption, use provided from storage_port even though having ssl_storage_port as well.
        self.assertEqual(c.storage_port, 15000)

        self.assertEqual(c.native_port, 9777)
        self.assertEqual(c.rpc_port, 9160)

    def test_cassandra_internode_encryption_v3_ports(self):
        config = configparser.ConfigParser(interpolation=None)
        config['cassandra'] = {
            'config_file': os.path.join(os.path.dirname(__file__),
                                        'resources/yaml/original/cassandra-internode-encrypt.yaml'),
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
            storage=None,
            monitoring=None,
            cassandra=_namedtuple_from_dict(CassandraConfig, config['cassandra']),
            grpc=_namedtuple_from_dict(GrpcConfig, config['grpc']),
            kubernetes=_namedtuple_from_dict(KubernetesConfig, config['kubernetes']),
            ssh=None,
            checks=None,
            logging=None,
        )

        c = Cassandra(medusa_config, release_version="3")

        # Uses ssl_storage_port value
        self.assertEqual(c.storage_port, 10001)

        self.assertEqual(c.native_port, 9042)
        self.assertEqual(c.rpc_port, 9160)

    def test_cassandra_internode_encryption_v4_ports(self):
        config = configparser.ConfigParser(interpolation=None)
        config['cassandra'] = {
            'config_file': os.path.join(os.path.dirname(__file__),
                                        'resources/yaml/original/cassandra-internode-encrypt.yaml'),
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
            storage=None,
            monitoring=None,
            cassandra=_namedtuple_from_dict(CassandraConfig, config['cassandra']),
            grpc=_namedtuple_from_dict(GrpcConfig, config['grpc']),
            kubernetes=_namedtuple_from_dict(KubernetesConfig, config['kubernetes']),
            ssh=None,
            checks=None,
            logging=None,
        )

        c = Cassandra(medusa_config, release_version="4")

        # Uses ssl_storage_port value
        self.assertEqual(c.storage_port, 10001)

        self.assertEqual(c.native_port, 9042)
        self.assertEqual(c.rpc_port, 9160)

    def test_cassandra_internode_encrypt_v3_default_ports(self):
        config = configparser.ConfigParser(interpolation=None)
        config['cassandra'] = {
            'config_file': os.path.join(os.path.dirname(__file__),
                                        'resources/yaml/original/cassandra-internode-encrypt-default.yaml'),
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
            storage=None,
            monitoring=None,
            cassandra=_namedtuple_from_dict(CassandraConfig, config['cassandra']),
            grpc=_namedtuple_from_dict(GrpcConfig, config['grpc']),
            kubernetes=_namedtuple_from_dict(KubernetesConfig, config['kubernetes']),
            ssh=None,
            checks=None,
            logging=None,
        )

        c = Cassandra(medusa_config, release_version="3")

        # Secure connection desired.
        # Both ssl_storage_port and storage_port are not defined.
        # Based v3 release version, should default to 7001.
        self.assertEqual(c.storage_port, 7001)

        self.assertEqual(c.native_port, 9042)
        self.assertEqual(c.rpc_port, 9160)

    def test_cassandra_internode_encrypt_v4_default_ports(self):
        config = configparser.ConfigParser(interpolation=None)
        config['cassandra'] = {
            'config_file': os.path.join(os.path.dirname(__file__),
                                        'resources/yaml/original/cassandra-internode-encrypt-default.yaml'),
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
            storage=None,
            monitoring=None,
            cassandra=_namedtuple_from_dict(CassandraConfig, config['cassandra']),
            grpc=_namedtuple_from_dict(GrpcConfig, config['grpc']),
            kubernetes=_namedtuple_from_dict(KubernetesConfig, config['kubernetes']),
            ssh=None,
            checks=None,
            logging=None,
        )

        c = Cassandra(medusa_config, release_version="4")

        # Secure connection desired.
        # Both ssl_storage_port and storage_port are not defined.
        # Based on v4 release version, should default to 7001.
        self.assertEqual(c.storage_port, 7001)

        self.assertEqual(c.native_port, 9042)
        self.assertEqual(c.rpc_port, 9160)

    def test_cassandra_internode_encrypt_nossl_v3_default_ports(self):
        config = configparser.ConfigParser(interpolation=None)
        config['cassandra'] = {
            'config_file': os.path.join(os.path.dirname(__file__),
                                        'resources/yaml/original/cassandra-internode-encrypt-nossl-default.yaml'),
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
            storage=None,
            monitoring=None,
            cassandra=_namedtuple_from_dict(CassandraConfig, config['cassandra']),
            grpc=_namedtuple_from_dict(GrpcConfig, config['grpc']),
            kubernetes=_namedtuple_from_dict(KubernetesConfig, config['kubernetes']),
            ssh=None,
            checks=None,
            logging=None,
        )

        c = Cassandra(medusa_config, release_version="3")

        # Secure connection desired.
        # The ssl_storage_port is not defined.
        # Based on not being c* v4, this will use default port for encryption.
        self.assertEqual(c.storage_port, 7001)

        self.assertEqual(c.native_port, 9042)
        self.assertEqual(c.rpc_port, 9160)

    def test_cassandra_internode_encrypt_nossl_v4_default_ports(self):
        config = configparser.ConfigParser(interpolation=None)
        config['cassandra'] = {
            'config_file': os.path.join(os.path.dirname(__file__),
                                        'resources/yaml/original/cassandra-internode-encrypt-nossl-default.yaml'),
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
            storage=None,
            monitoring=None,
            cassandra=_namedtuple_from_dict(CassandraConfig, config['cassandra']),
            grpc=_namedtuple_from_dict(GrpcConfig, config['grpc']),
            kubernetes=_namedtuple_from_dict(KubernetesConfig, config['kubernetes']),
            ssh=None,
            checks=None,
            logging=None,
        )

        c = Cassandra(medusa_config, release_version="4")

        # Secure connection desired.
        # The ssl_storage_port is not defined.
        # Based on being c* v4, this will use the value of the specified storage_port
        self.assertEqual(c.storage_port, 8675)

        self.assertEqual(c.native_port, 9042)
        self.assertEqual(c.rpc_port, 9160)

    def test_cassandra_client_encryption_enabled_default_port(self):
        config = configparser.ConfigParser(interpolation=None)
        config['cassandra'] = {
            'config_file': os.path.join(os.path.dirname(__file__),
                                        'resources/yaml/original/cassandra-client-encrypt-default.yaml'),
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
            storage=None,
            monitoring=None,
            cassandra=_namedtuple_from_dict(CassandraConfig, config['cassandra']),
            grpc=_namedtuple_from_dict(GrpcConfig, config['grpc']),
            kubernetes=_namedtuple_from_dict(KubernetesConfig, config['kubernetes']),
            ssh=None,
            checks=None,
            logging=None,
        )

        c = Cassandra(medusa_config)

        # Both ports are not assigned, using default
        self.assertEqual(c.native_port, 9142)
        self.assertEqual(c.rpc_port, 9160)

        self.assertEqual(c.storage_port, 7000)

    def test_cassandra_client_encryption_enabled_reuse_port(self):
        config = configparser.ConfigParser(interpolation=None)
        config['cassandra'] = {
            'config_file': os.path.join(os.path.dirname(__file__),
                                        'resources/yaml/original/cassandra-client-encrypt.yaml'),
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
            storage=None,
            monitoring=None,
            cassandra=_namedtuple_from_dict(CassandraConfig, config['cassandra']),
            grpc=_namedtuple_from_dict(GrpcConfig, config['grpc']),
            kubernetes=_namedtuple_from_dict(KubernetesConfig, config['kubernetes']),
            ssh=None,
            checks=None,
            logging=None,
        )

        c = Cassandra(medusa_config)

        # Expecting with client_encryption_options enabled, and not defining value for native_transport_port_ssl
        # use native_transport_port value as defined.
        self.assertEqual(c.native_port, 9123)
        self.assertEqual(c.rpc_port, 9160)
        self.assertEqual(c.storage_port, 7000)

    def test_cassandra_client_encryption_enabled_ssl_port(self):
        config = configparser.ConfigParser(interpolation=None)
        config['cassandra'] = {
            'config_file': os.path.join(os.path.dirname(__file__),
                                        'resources/yaml/original/cassandra-client-encrypt-sslport.yaml'),
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
            storage=None,
            monitoring=None,
            cassandra=_namedtuple_from_dict(CassandraConfig, config['cassandra']),
            grpc=_namedtuple_from_dict(GrpcConfig, config['grpc']),
            kubernetes=_namedtuple_from_dict(KubernetesConfig, config['kubernetes']),
            ssh=None,
            checks=None,
            logging=None,
        )

        c = Cassandra(medusa_config)

        # server_encryption_options /internode set to'all', with both
        # ports native_transport_port_ssl AND native_transport_port
        # defined.  Expected to use native_transport_port_ssl
        self.assertEqual(c.storage_port, 7001)

        self.assertEqual(c.native_port, 18675)
        self.assertEqual(c.rpc_port, 9160)

    def test_cassandra_missing_native_port(self):
        config = configparser.ConfigParser(interpolation=None)
        config['cassandra'] = {
            'config_file': os.path.join(os.path.dirname(__file__),
                                        'resources/yaml/original/cassandra-missing-native-port.yaml'),
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
            storage=None,
            monitoring=None,
            cassandra=_namedtuple_from_dict(CassandraConfig, config['cassandra']),
            grpc=_namedtuple_from_dict(GrpcConfig, config['grpc']),
            kubernetes=_namedtuple_from_dict(KubernetesConfig, config['kubernetes']),
            ssh=None,
            checks=None,
            logging=None,
        )
        c = Cassandra(medusa_config)

        # Case where cient_encryption_options enabled, but no native ports defined.
        self.assertEqual(c.native_port, 9142)


if __name__ == '__main__':
    unittest.main()
