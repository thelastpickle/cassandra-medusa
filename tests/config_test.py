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


class RestoreNodeTest(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def test_no_auth_env_variables(self):
        os.environ.pop("CQL_USERNAME")
        os.environ.pop("CQL_PASSWORD")
        medusa_config_file = pathlib.Path(__file__).parent / "resources/config/medusa.ini"
        args = {}
        config = medusa.config.load_config(args, medusa_config_file)
        assert config.cassandra.cql_username == "test_username"
        assert config.cassandra.cql_password == "test_password"

    def test_different_auth_env_variables(self):
        os.environ["CQL_USERNAME"] = "different_username"
        os.environ["CQL_PASSWORD"] = "different_password"
        medusa_config_file = pathlib.Path(__file__).parent / "resources/config/medusa.ini"
        args = {}
        config = medusa.config.load_config(args, medusa_config_file)
        assert config.cassandra.cql_username == "different_username"
        assert config.cassandra.cql_password == "different_password"


if __name__ == '__main__':
    unittest.main()
