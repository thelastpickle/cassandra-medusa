# -*- coding: utf-8 -*-
# Copyright 2021 DataStax, Inc.
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

import os
import pathlib
import unittest
from unittest.mock import patch
import socket
import tempfile

import medusa.config
import medusa.utils


class ConfigTest(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def setUp(self):
        self.medusa_config_file = pathlib.Path(__file__).parent / "resources/config/medusa.ini"

    def test_no_auth_env_variables(self):
        """
        Ensure that CQL credentials, Nodetool credentials, and keystore/truststore passwords
        in config file are honored
        """
        for cass_cred in [
            'CQL_USERNAME',
            'CQL_PASSWORD',
            'NODETOOL_USERNAME',
            'NODETOOL_PASSWORD',
            'SSTABLELOADER_TSPW',
            'SSTABLELOADER_KSPW'
        ]:
            # We need to check if the deprecated CQL_USERNAME and CQL_PASSWORD appear in the envs
            if cass_cred in os.environ:
                os.environ.pop(cass_cred)
            if 'MEDUSA_{}'.format(cass_cred) in os.environ:
                os.environ.pop('MEDUSA_{}'.format(cass_cred))

        args = {}
        config = medusa.config.load_config(args, self.medusa_config_file)
        assert config.cassandra.cql_username == 'test_cql_username'
        assert config.cassandra.cql_password == 'test_cql_password'
        assert config.cassandra.nodetool_username == 'test_nodetool_username'
        assert config.cassandra.nodetool_password == 'test_nodetool_password'
        assert config.cassandra.sstableloader_tspw == 'test_ts_password'
        assert config.cassandra.sstableloader_kspw == 'test_ks_password'

    def test_different_auth_env_variables(self):
        """
        Ensure that CQL credentials, Nodetool credentials, and keystore/truststore passwords env vars
        have a higher priority than config
        """
        for cass_cred in [
            'CQL_USERNAME',
            'CQL_PASSWORD',
            'NODETOOL_USERNAME',
            'NODETOOL_PASSWORD',
            'SSTABLELOADER_TSPW',
            'SSTABLELOADER_KSPW'
        ]:
            os.environ['MEDUSA_{}'.format(cass_cred)] = 'different_{}'.format(cass_cred.lower())

        args = {}
        config = medusa.config.load_config(args, self.medusa_config_file)
        assert config.cassandra.cql_username == 'different_cql_username'
        assert config.cassandra.cql_password == 'different_cql_password'
        assert config.cassandra.nodetool_username == 'different_nodetool_username'
        assert config.cassandra.nodetool_password == 'different_nodetool_password'
        assert config.cassandra.sstableloader_tspw == 'different_sstableloader_tspw'
        assert config.cassandra.sstableloader_kspw == 'different_sstableloader_kspw'

    def test_new_env_variables_override_deprecated_ones(self):
        """
        Ensure that CQL credentials stored in the new env vars have a higher priority than the deprecated env vars
        """
        for cql_cred in ['CQL_USERNAME', 'CQL_PASSWORD']:
            # new env vars prefixed with MEDUSA_*; MEDUSA_CQL_USERNAME and MEDUSA_CQL_PASSWORD
            os.environ['MEDUSA_{}'.format(cql_cred)] = 'new_{}'.format(cql_cred.lower())
            # old env vars; CQL_USERNAME and CQL_PASSWORD
            os.environ[cql_cred] = 'deprecated_{}'.format(cql_cred.lower())

        args = {}
        config = medusa.config.load_config(args, self.medusa_config_file)
        assert config.cassandra.cql_username == 'new_cql_username'
        assert config.cassandra.cql_password == 'new_cql_password'

    def test_cql_k8s_secrets_path_override(self):
        """
        Ensure that CQL credentials stored in a path following k8s convention override the default vars.
        """
        tmpdir = tempfile.mkdtemp()
        os.environ['MEDUSA_CQL_K8S_SECRETS_PATH'] = tmpdir
        # Write k8s_username and k8s_password in /tmpdir/username and /tmpdir/password
        for k8s_cred in ['username', 'password']:
            with open(os.path.join(tmpdir, k8s_cred), 'w') as f:
                f.write('k8s_{}'.format(k8s_cred))

        args = {}
        config = medusa.config.load_config(args, self.medusa_config_file)
        assert config.cassandra.cql_username == 'k8s_username'
        assert config.cassandra.cql_password == 'k8s_password'

        # Cleanup
        os.environ.pop('MEDUSA_CQL_K8S_SECRETS_PATH', None)

    def test_nodetool_k8s_secrets_path_override(self):
        """
        Ensure that nodetool credentials stored in a path following k8s convention override the default vars.
        """
        tmpdir = tempfile.mkdtemp()
        os.environ['MEDUSA_NODETOOL_K8S_SECRETS_PATH'] = tmpdir
        # Write nodetool_username and nodetool_password in /tmpdir/username and /tmpdir/password
        for k8s_cred in ['username', 'password']:
            with open(os.path.join(tmpdir, k8s_cred), 'w') as f:
                f.write('k8s_{}'.format(k8s_cred))

        args = {}
        config = medusa.config.load_config(args, self.medusa_config_file)
        assert config.cassandra.nodetool_username == 'k8s_username'
        assert config.cassandra.nodetool_password == 'k8s_password'

        # Cleanup
        os.environ.pop('MEDUSA_NODETOOL_K8S_SECRETS_PATH', None)

    def test_args_settings_override(self):
        """Ensure that each config file's section settings can be overridden with command line options"""
        args = {
            'bucket_name': 'Hector',
            'cql_username': 'Priam',
            'enabled': 'True',  # FIXME collision: grpc or kubernetes
            'file': 'hera.log',
            'monitoring_provider': 'local',
            'query': 'SELECT * FROM greek_mythology',
            'use_mgmt_api': 'True',
            'username': 'Zeus',
            'fqdn': 'localhost',
        }
        config = medusa.config.load_config(args, self.medusa_config_file)
        assert config.storage.bucket_name == 'Hector'
        assert config.cassandra.cql_username == 'Priam'
        assert medusa.utils.evaluate_boolean(config.grpc.enabled)  # FIXME collision
        assert medusa.utils.evaluate_boolean(
            config.kubernetes.enabled if config.kubernetes else False)  # FIXME collision
        assert config.logging.file == 'hera.log'
        assert config.monitoring.monitoring_provider == 'local'
        assert config.checks.query == 'SELECT * FROM greek_mythology'
        assert medusa.utils.evaluate_boolean(config.kubernetes.use_mgmt_api)
        assert config.ssh.username == 'Zeus'

    def test_use_sudo_default(self):
        """Ensure that, by default, use_sudo is enabled and kubernetes disabled"""
        args = {}
        config = medusa.config.load_config(args, self.medusa_config_file)
        assert medusa.utils.evaluate_boolean(config.cassandra.use_sudo)
        # Kubernetes must be disabled by default so use_sudo can be honored
        assert not medusa.utils.evaluate_boolean(config.kubernetes.enabled if config.kubernetes else False)

    def test_use_sudo_kubernetes_disabled(self):
        """Ensure that use_sudo is honored when Kubernetes mode is disabled (default)"""
        args = {'use_sudo': 'True'}
        config = medusa.config.parse_config(args, self.medusa_config_file)
        assert config['cassandra']['use_sudo'] == 'True', 'sudo should be used because Kubernetes mode is not enabled'

        args = {'use_sudo': 'False'}
        config = medusa.config.parse_config(args, self.medusa_config_file)
        assert config['cassandra']['use_sudo'] == 'False', 'sudo should not be used as explicitly required'

    def test_use_sudo_kubernetes_enabled(self):
        """Ensure that use_sudo is disabled when Kubernetes mode is enabled"""
        args = {'use_sudo': 'true'}
        medusa_k8s_config = pathlib.Path(__file__).parent / "resources/config/medusa-kubernetes.ini"
        config = medusa.config.parse_config(args, medusa_k8s_config)
        assert config['cassandra']['use_sudo'] == 'False'

    def test_use_sudo_kubernetes_enabled_without_config_file(self):
        kubernetes_args = {
            "k8s_enabled": 'True',
            "cassandra_url": 'https://foo:8080',
            "use_mgmt_api": 'True'
        }
        args = {**kubernetes_args}
        medusa_basic_config = pathlib.Path(__file__).parent / "resources/config/medusa.ini"
        config = medusa.config.parse_config(args, medusa_basic_config)
        assert config['kubernetes']['enabled'] == 'True'
        assert config['kubernetes']['cassandra_url'] == 'https://foo:8080'
        assert config['kubernetes']['use_mgmt_api'] == 'True'
        assert config['cassandra']['use_sudo'] == 'False'

    def test_overridden_fqdn(self):
        """Ensure that a overridden fqdn in config is honored"""
        args = {'fqdn': 'overridden-fqdn'}
        config = medusa.config.parse_config(args, self.medusa_config_file)
        assert config['storage']['fqdn'] == 'overridden-fqdn'

    def test_fqdn_with_resolve_ip_addresses_enabled(self):
        """Ensure that explicitly defined fqdn is untouched when DNS resolving is enabled"""
        with patch('medusa.network.hostname_resolver.socket') as mock_socket_resolver:
            with patch('medusa.config.socket') as mock_socket_config:
                mock_socket_resolver.getfqdn.return_value = "localhost"
                mock_socket_config.getfqdn.return_value = "localhost"
                args = {
                    'fqdn': 'localhost',
                    'resolve_ip_addresses': 'True'
                }
                config = medusa.config.parse_config(args, self.medusa_config_file)
                assert config['storage']['fqdn'] == 'localhost'

    def test_fqdn_with_resolve_ip_addresses_disabled(self):
        """Ensure that fqdn is an IP address when DNS resolving is disabled"""
        args = {
            'fqdn': socket.getfqdn(),
            'resolve_ip_addresses': 'False'
        }
        config = medusa.config.parse_config(args, self.medusa_config_file)
        assert config['storage']['fqdn'] == socket.gethostbyname(socket.getfqdn())

    @patch('medusa.config.logging.error')
    def test_slash_in_bucket_name(self, mock_log_error):
        args = {
            'bucket_name': 'bucket/name',
            'cql_username': 'Priam',
            'enabled': 'True',
            'file': 'hera.log',
            'monitoring_provider': 'local',
            'query': 'SELECT * FROM greek_mythology',
            'use_mgmt_api': 'True',
            'username': 'Zeus',
            'fqdn': 'localhost',
        }
        with self.assertRaises(SystemExit) as cm:
            medusa.config.load_config(args, self.medusa_config_file)
        self.assertEqual(cm.exception.code, 2)
        mock_log_error.assert_called_with('Required configuration "bucket_name" cannot contain a slash ("/")')

    @patch('medusa.config.logging.error')
    def test_slash_in_prefix(self, mock_log_error):
        args = {
            'bucket_name': 'Hector',
            'prefix': 'pre/fix',
            'cql_username': 'Priam',
            'enabled': 'True',
            'file': 'hera.log',
            'monitoring_provider': 'local',
            'query': 'SELECT * FROM greek_mythology',
            'use_mgmt_api': 'True',
            'username': 'Zeus',
            'fqdn': 'localhost',
        }
        with self.assertRaises(SystemExit) as cm:
            medusa.config.load_config(args, self.medusa_config_file)
        self.assertEqual(cm.exception.code, 2)
        mock_log_error.assert_called_with('Required configuration "prefix" cannot contain a slash ("/")')


if __name__ == '__main__':
    unittest.main()
