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

from cassandra.metadata import Murmur3Token
from unittest.mock import Mock

from medusa.config import MedusaConfig, StorageConfig, _namedtuple_from_dict
from medusa.cassandra_utils import CqlSession


class CassandraUtilsTest(unittest.TestCase):
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
        s = CqlSession(session)
        token_map = s.tokenmap()
        self.assertEqual(
            {'localhost': {'is_up': True, 'tokens': [-9]}},
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
        s = CqlSession(session)
        token_map = s.tokenmap()
        self.assertEqual(
            {'localhost': {'is_up': True, 'tokens': [-9, -6, 0]}},
            token_map
        )

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
        s = CqlSession(session)
        token_map = s.tokenmap()
        self.assertEqual(
            {'localhost': {'is_up': True, 'tokens': [-6]}},
            token_map
        )


if __name__ == '__main__':
    unittest.main()
