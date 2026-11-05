# -*- coding: utf-8 -*-
# Copyright 2019 Spotify AB
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
from unittest.mock import MagicMock

from medusa.index import add_snapshot_time_to_index, clean_backup_from_index
from medusa.storage import Storage


class IndexTest(unittest.TestCase):

    def _storage(self, prefix_path=''):
        storage = MagicMock()
        storage.prefix_path = prefix_path
        return storage

    def _node_backup(self, name='backup1', fqdn='node1', snapshot_time=1500):
        node_backup = MagicMock()
        node_backup.name = name
        node_backup.fqdn = fqdn
        node_backup.snapshot_time = snapshot_time
        return node_backup

    def test_add_snapshot_time_to_index_uploads_blob_with_exact_name_and_content(self):
        storage = self._storage()

        add_snapshot_time_to_index(storage, self._node_backup())

        storage.storage_driver.upload_blob_from_string.assert_called_once_with(
            'index/backup_index/backup1/snapshot_time_node1_1500.timestamp', '1500')

    def test_add_snapshot_time_to_index_honours_prefix_path(self):
        storage = self._storage('prefix/')

        add_snapshot_time_to_index(storage, self._node_backup())

        storage.storage_driver.upload_blob_from_string.assert_called_once_with(
            'prefix/index/backup_index/backup1/snapshot_time_node1_1500.timestamp', '1500')

    def test_add_snapshot_time_to_index_blob_name_is_readable_by_storage(self):
        storage = self._storage('prefix/')
        add_snapshot_time_to_index(storage, self._node_backup(fqdn='some_host.example.com'))
        blob_name = storage.storage_driver.upload_blob_from_string.call_args[0][0]

        self.assertEqual('some_host.example.com', Storage.get_fqdn_from_any_index_blob(blob_name))
        self.assertEqual(1500, Storage.get_timestamp_from_blob_name(blob_name))

    def test_add_snapshot_time_to_index_uploads_nothing_for_none(self):
        storage = self._storage()

        add_snapshot_time_to_index(storage, self._node_backup(snapshot_time=None))

        storage.storage_driver.upload_blob_from_string.assert_not_called()

    def test_clean_backup_from_index_deletes_snapshot_time_blob(self):
        storage = self._storage()
        blobs = []
        for name in ('tokenmap_node1.json', 'finished_node1_2000.timestamp', 'snapshot_time_node1_1500.timestamp',
                     'snapshot_time_node2_1600.timestamp'):
            blob = MagicMock()
            blob.name = 'index/backup_index/backup1/' + name
            blobs.append(blob)
        storage.storage_driver.list_objects.return_value = blobs

        clean_backup_from_index(storage, self._node_backup())

        deleted = {call[0][0].name for call in storage.storage_driver.delete_object.call_args_list}
        self.assertIn('index/backup_index/backup1/snapshot_time_node1_1500.timestamp', deleted)
        self.assertNotIn('index/backup_index/backup1/snapshot_time_node2_1600.timestamp', deleted)


if __name__ == '__main__':
    unittest.main()
