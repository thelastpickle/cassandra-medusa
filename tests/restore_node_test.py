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
from unittest import mock
from unittest.mock import Mock

from cassandra.util import Version
from libcloud.common.base import BaseDriver

from medusa import restore_node, storage
from medusa.config import MedusaConfig, StorageConfig, _namedtuple_from_dict
from medusa.host_man import HostMan
from medusa.storage import abstract_storage


class RestoreNodeTest(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def setUp(self):
        config = configparser.ConfigParser(interpolation=None)
        config['storage'] = {
            'host_file_separator': ','
        }
        self.config = MedusaConfig(
            file_path=None,
            storage=_namedtuple_from_dict(StorageConfig, config['storage']),
            monitoring={},
            cassandra=None,
            ssh=None,
            checks=None,
            logging=None,
            grpc=None,
            kubernetes=None,
        )
        # Reset to ensure no stale singleton state exists
        HostMan.reset()

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

    @mock.patch.object(abstract_storage.AbstractStorage, '__init__')
    @mock.patch.object(storage.Storage, '__init__')
    def test_capture_release_version_from_CLI(self, mock_storage, mock_abstract_storage):
        # WHEN api_version is specified from CLI, and api_version is available from driver.
        mock_abstract_storage.driver = BaseDriver(key=Mock(), api_version='5.0.0')
        mock_storage.attach_mock(mock_abstract_storage, 'storage_driver')
        restore_node.capture_release_version(mock_storage, '4.0.0')

        # THEN expect the api_version from driver is captured.
        self.assertEqual(HostMan.get_release_version(), Version('4.0.0'))

    @mock.patch.object(abstract_storage.AbstractStorage, '__init__')
    @mock.patch.object(storage.Storage, '__init__')
    def test_capture_release_version_from_driver(self, mock_storage, mock_abstract_storage):
        # WHEN api_version from driver is specified and no specified CLI version.
        mock_abstract_storage.driver = BaseDriver(key=Mock(), api_version='5.0.0')
        mock_storage.attach_mock(mock_abstract_storage, 'storage_driver')
        restore_node.capture_release_version(mock_storage, None)

        # THEN expect the api_version from driver is captured.
        self.assertEqual(HostMan.get_release_version(), Version('5.0.0'))

    @mock.patch.object(abstract_storage.AbstractStorage, '__init__')
    @mock.patch.object(storage.Storage, '__init__')
    def test_capture_release_version_from_default(self, mock_storage, mock_abstract_storage):
        # WHEN api_version is not available from either CLI or the driver.
        mock_abstract_storage.driver = BaseDriver(key=Mock())
        mock_storage.attach_mock(mock_abstract_storage, 'storage_driver')
        restore_node.capture_release_version(mock_storage, None)

        # THEN expect default release version will be captured.
        self.assertEqual(HostMan.get_release_version(), Version(HostMan.DEFAULT_RELEASE_VERSION))


if __name__ == '__main__':
    unittest.main()
