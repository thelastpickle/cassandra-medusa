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
from medusa.restore_node import get_node_tokens


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
            restore=None
        )

    def test_get_node_tokens(self):
        with open("tests/resources/restore_node_tokenmap.json", 'r') as f:
            tokens = get_node_tokens('node3.mydomain.net', f)
            self.assertEqual(tokens, ['3074457345618258400'])

    def test_get_node_tokens_vnodes(self):
        with open("tests/resources/restore_node_tokenmap_vnodes.json", 'r') as f:
            tokens = get_node_tokens('node3.mydomain.net', f)
            self.assertEqual(tokens, ['2', '3'])


if __name__ == '__main__':
    unittest.main()
