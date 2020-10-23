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
import unittest

from medusa.config import MedusaConfig, StorageConfig, _namedtuple_from_dict
from medusa import restore_node


class RestoreNodeTest(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def setUp(self):
        config = configparser.ConfigParser(interpolation=None)
        config['storage'] = {
            'host_file_separator': ','
        }
        self.config = MedusaConfig(
            storage=_namedtuple_from_dict(StorageConfig, config['storage']),
            monitoring={},
            cassandra=None,
            ssh=None,
            checks=None,
            logging=None,
            grpc=None,
            kubernetes=None,
        )

    def test_get_node_tokens(self):
        with open("tests/resources/restore_node_tokenmap.json", 'r') as f:
            tokens = restore_node.get_node_tokens('node3.mydomain.net', f)
            self.assertEqual(tokens, ['3074457345618258400'])

    def test_get_node_tokens_vnodes(self):
        with open("tests/resources/restore_node_tokenmap_vnodes.json", 'r') as f:
            tokens = restore_node.get_node_tokens('node3.mydomain.net', f)
            self.assertEqual(tokens, ['2', '3'])

    def test_keyspace_is_allowed_to_restore(self):

        # the basic case when there's a table from k1
        keyspace, keep_auth, fqtns_to_restore = 'k1', False, {'k1.t1', 'k2.t2'}
        self.assertTrue(restore_node.keyspace_is_allowed_to_restore(keyspace, keep_auth, fqtns_to_restore))

        # case when there's no table from the keyspace we are examining
        keyspace, keep_auth, fqtns_to_restore = 'k2', False, {'k1.t1'}
        self.assertFalse(restore_node.keyspace_is_allowed_to_restore(keyspace, keep_auth, fqtns_to_restore))

        # system keyspace is never allowed
        keyspace, keep_auth, fqtns_to_restore = 'system', False, {'k1.t1'}
        self.assertFalse(restore_node.keyspace_is_allowed_to_restore(keyspace, keep_auth, fqtns_to_restore))

        # system_auth is allowed only when we don't keep auth
        keyspace, keep_auth, fqtns_to_restore = 'system_auth', False, {'k1.t1', 'system_auth.t1'}
        self.assertTrue(restore_node.keyspace_is_allowed_to_restore(keyspace, keep_auth, fqtns_to_restore))

        keyspace, keep_auth, fqtns_to_restore = 'system_auth', True, {'k1.t1', 'system_auth.t1'}
        self.assertFalse(restore_node.keyspace_is_allowed_to_restore(keyspace, keep_auth, fqtns_to_restore))

    def test_table_is_allowed_to_restore(self):
        keyspace, table, fqtns_to_restore = 'k1', 't1', {'k1.t1'}
        self.assertTrue(restore_node.table_is_allowed_to_restore(keyspace, table, fqtns_to_restore))

        keyspace, table, fqtns_to_restore = 'k1', 't1', {'k2.t1'}
        self.assertFalse(restore_node.table_is_allowed_to_restore(keyspace, table, fqtns_to_restore))


if __name__ == '__main__':
    unittest.main()
