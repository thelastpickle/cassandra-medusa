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

import medusa.config
import medusa.utils


class ConfigTest(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def setUp(self):
        self.medusa_config_file = pathlib.Path(__file__).parent / "resources/config/medusa.ini"

    def test_no_auth_env_variables(self):
        """Ensure that CQL credentials in config file are honored"""
        os.environ.pop("CQL_USERNAME")
        os.environ.pop("CQL_PASSWORD")
        args = {}
        config = medusa.config.load_config(args, self.medusa_config_file)
        assert config.cassandra.cql_username == "test_username"
        assert config.cassandra.cql_password == "test_password"

    def test_different_auth_env_variables(self):
        """Ensure that CQL credentials env vars have an higher priority than config file"""
        os.environ["CQL_USERNAME"] = "different_username"
        os.environ["CQL_PASSWORD"] = "different_password"
        args = {}
        config = medusa.config.load_config(args, self.medusa_config_file)
        assert config.cassandra.cql_username == "different_username"
        assert config.cassandra.cql_password == "different_password"

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
        }
        config = medusa.config.load_config(args, self.medusa_config_file)
        assert config.storage.bucket_name == 'Hector'
        assert config.cassandra.cql_username == 'Priam'
        assert medusa.utils.evaluate_boolean(config.grpc.enabled)  # FIXME collision
        assert medusa.utils.evaluate_boolean(config.kubernetes.enabled)  # FIXME collision
        assert config.logging.file == 'hera.log'
        assert config.monitoring.monitoring_provider == 'local'
        assert config.checks.query == 'SELECT * FROM greek_mythology'
        assert medusa.utils.evaluate_boolean(config.kubernetes.use_mgmt_api)
        assert config.ssh.username == 'Zeus'


if __name__ == '__main__':
    unittest.main()
