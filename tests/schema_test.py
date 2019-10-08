# Copyright 2018 Spotify AB
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

import unittest

from medusa.schema import parse_schema


class SchemaTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def test_keyspaces(self):
        with open("tests/resources/schema.cql", 'r') as f:
            schema = parse_schema(f.read())
            assert "system_auth" in schema.keys()
            assert "system" in schema.keys()
            assert "system_distributed" in schema.keys()
            assert "tlp_stress2" in schema.keys()

    def test_tables(self):
        with open("tests/resources/schema.cql", 'r') as f:
            schema = parse_schema(f.read())
            assert len(schema["system_auth"]["tables"].keys()) == 4
            assert len(schema["system"]["tables"].keys()) == 20
            assert len(schema["system_distributed"]["tables"].keys()) == 2
            assert len(schema["tlp_stress2"]["tables"].keys()) == 1
            assert "random_access" in schema["tlp_stress2"]["tables"].keys()

    def test_indices(self):
        with open("tests/resources/schema.cql", 'r') as f:
            schema = parse_schema(f.read())
            assert len(schema["system_auth"]["indices"].keys()) == 0
            assert len(schema["system"]["indices"].keys()) == 0
            assert len(schema["system_distributed"]["indices"].keys()) == 0
            assert len(schema["tlp_stress2"]["indices"].keys()) == 1
            assert "value_index" in schema["tlp_stress2"]["indices"].keys()

    def test_mv(self):
        with open("tests/resources/schema.cql", 'r') as f:
            schema = parse_schema(f.read())
            assert len(schema["system_auth"]["materialized_views"].keys()) == 0
            assert len(schema["system"]["materialized_views"].keys()) == 0
            assert len(schema["system_distributed"]["materialized_views"].keys()) == 0
            assert len(schema["tlp_stress2"]["materialized_views"].keys()) == 1
            assert "random_access_by_value" in schema["tlp_stress2"]["materialized_views"].keys()

    def test_udf(self):
        with open("tests/resources/schema.cql", 'r') as f:
            schema = parse_schema(f.read())
            assert len(schema["system_auth"]["udt"].keys()) == 0
            assert len(schema["system"]["udt"].keys()) == 0
            assert len(schema["system_distributed"]["udt"].keys()) == 0
            assert len(schema["tlp_stress2"]["udt"].keys()) == 2
            assert "custom_type" in schema["tlp_stress2"]["udt"].keys()
            assert "custom_type2" in schema["tlp_stress2"]["udt"].keys()


if __name__ == '__main__':
    unittest.main()
