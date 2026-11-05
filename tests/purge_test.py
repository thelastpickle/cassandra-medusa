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

from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

from medusa.config import MedusaConfig, PitrConfig, StorageConfig, _namedtuple_from_dict
from medusa.storage import NodeBackup, Storage
from medusa.purge import backups_to_purge_by_age, backups_to_purge_by_count, backups_to_purge_by_name
from medusa.purge import filter_differential_backups, filter_files_within_gc_grace
from medusa.purge import main, oldest_kept_snapshot_time, purge_commitlogs

from tests.storage_test import make_node_backup, make_cluster_backup, make_blob


class PurgeTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def setUp(self):
        config = configparser.ConfigParser(interpolation=None)
        config['storage'] = {
            'host_file_separator': ',',
            'storage_provider': 'local',
            'base_path': '/tmp',
            'bucket_name': 'purge_test',
            'fqdn': 'node1'
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
        self.storage = Storage(config=self.config.storage)

    def test_purge_backups_by_date(self):
        backups = list()
        # Build a list of 40 daily backups
        for i in range(1, 80, 2):
            file_time = datetime.now() + timedelta(days=(i + 1) - 80)
            backups.append(make_node_backup(self.storage, str(i), file_time, differential=True))

        obsolete_backups = backups_to_purge_by_age(backups, 1)
        assert len(obsolete_backups) == 39

        obsolete_backups = backups_to_purge_by_age(backups, 10)
        assert len(obsolete_backups) == 35

        obsolete_backups = backups_to_purge_by_age(backups, 78)
        assert len(obsolete_backups) == 1

        obsolete_backups = backups_to_purge_by_age(backups, 80)
        assert len(obsolete_backups) == 0

        obsolete_backups = backups_to_purge_by_age(backups, 90)
        assert len(obsolete_backups) == 0

    def test_purge_backups_by_count(self):
        backups = list()

        # Build a list of 40 daily backups
        for i in range(1, 80, 2):
            file_time = datetime.now() + timedelta(days=(i + 1) - 80)
            backups.append(make_node_backup(self.storage, str(i), file_time, differential=True))

        obsolete_backups = backups_to_purge_by_count(backups, 10)
        assert(obsolete_backups[0].name == "1")

        obsolete_backups = backups_to_purge_by_count(backups, 20)
        assert len(obsolete_backups) == 20

        obsolete_backups = backups_to_purge_by_count(backups, 40)
        assert len(obsolete_backups) == 0

    def test_filter_differential_backups(self):
        backups = list()
        backups.append(make_node_backup(self.storage, "one", datetime.now(), differential=True))
        backups.append(make_node_backup(self.storage, "two", datetime.now(), differential=True))
        backups.append(make_node_backup(self.storage, "three", datetime.now(), differential=False))
        backups.append(make_node_backup(self.storage, "four", datetime.now(), differential=True))
        backups.append(make_node_backup(self.storage, "five", datetime.now(), differential=False))
        assert 3 == len(filter_differential_backups(backups))

    def test_filter_files_within_gc_grace(self):
        blobs = list()
        blobs.append(make_blob("file1", datetime.timestamp(datetime.now())))
        blobs.append(make_blob("file2", datetime.timestamp(datetime.now() + timedelta(hours=-12))))
        blobs.append(make_blob("file3", datetime.timestamp(datetime.now() + timedelta(days=-1))))
        blobs.append(make_blob("file4", datetime.timestamp(datetime.now() + timedelta(days=-2))))
        blob_map = {blob.name: blob for blob in blobs}
        assert 2 == len(filter_files_within_gc_grace(self.storage, blob_map.keys(), blob_map, 1))

    def test_purge_backups_by_name(self):
        nodes = ["node1", "node2", "node3"]
        backup_names = ["backup1", "backup2", "backup3"]
        cluster_backups = list()
        for backup_name in backup_names:
            cluster_backups.append(
                make_cluster_backup(self.storage, backup_name, datetime.now(), nodes, differential=True)
            )

        # all nodes, one backups
        backups_to_purge = backups_to_purge_by_name(self.storage, cluster_backups, ["backup1"], True)
        self.assertEqual(3, len(backups_to_purge))
        for btp in backups_to_purge:
            self.assertEqual("backup1", btp.name)
        self.assertFalse(set(nodes) - set(map(lambda x: x.fqdn, backups_to_purge)))

        # one node, one backup
        backups_to_purge = backups_to_purge_by_name(self.storage, cluster_backups, ["backup1"], False)
        self.assertEqual(1, len(backups_to_purge))
        for btp in backups_to_purge:
            self.assertEqual("backup1", btp.name)
            self.assertEqual("node1", btp.fqdn)

        # all nodes, 2 backups
        backups_to_purge = backups_to_purge_by_name(self.storage, cluster_backups, ["backup1", "backup2"], True)
        self.assertEqual(6, len(backups_to_purge))
        for btp in backups_to_purge:
            self.assertTrue(btp.name in ["backup1", "backup2"])
        self.assertFalse(set(nodes) - set(map(lambda x: x.fqdn, backups_to_purge)))

        # one nodes, 2 backups
        backups_to_purge = backups_to_purge_by_name(self.storage, cluster_backups, ["backup1", "backup2"], False)
        self.assertEqual(2, len(backups_to_purge))
        for btp in backups_to_purge:
            self.assertTrue(btp.name in ["backup1", "backup2"])
            self.assertEqual("node1", btp.fqdn)

        # non-existent backup name raises KeyError
        self.assertRaises(KeyError, backups_to_purge_by_name, self.storage, cluster_backups, ["nonexistent"], False)

    # --- commitlog purge (PITR) ---

    def _commitlog_storage(self, blobs, prefix_path=''):
        storage = MagicMock()
        storage.prefix_path = prefix_path
        storage.storage_driver.list_objects.return_value = blobs
        storage.storage_driver.get_object_datetime.side_effect = lambda blob: blob.last_modified
        return storage

    def _pitr_node_backup(self, name, started, snapshot_time):
        return NodeBackup(storage=self.storage, fqdn='node1', name=name,
                          started_timestamp=started, finished_timestamp=started,
                          snapshot_time_timestamp=snapshot_time)

    def _pitr_config(self, enabled, interval_seconds='60'):
        pitr = _namedtuple_from_dict(PitrConfig, {
            'enabled': enabled,
            'commitlog_archive_interval_seconds': interval_seconds,
        })
        return self.config._replace(pitr=pitr)

    def test_purge_commitlogs_boundaries(self):
        # oldest_kept_snapshot_ts = 1000, interval = 60 -> purge_threshold = 910
        blobs = {ts: make_blob("node1/commitlogs/CommitLog-{}.log".format(ts), ts)
                 for ts in (0, 909, 910, 911, 10 ** 9)}
        storage = self._commitlog_storage(list(blobs.values()))
        self.assertIsInstance(blobs[909].last_modified, datetime)

        deleted = purge_commitlogs(storage, 'node1', 1000, 60)

        self.assertEqual(2, deleted)
        deleted_blobs = {call.args[0] for call in storage.storage_driver.delete_object.call_args_list}
        self.assertEqual({blobs[0], blobs[909]}, deleted_blobs)

    def test_purge_commitlogs_empty(self):
        storage = self._commitlog_storage([])
        self.assertEqual(0, purge_commitlogs(storage, 'node1', 1000, 60))
        storage.storage_driver.delete_object.assert_not_called()

    def test_purge_commitlogs_threshold_depends_on_interval(self):
        for interval_seconds, threshold in ((0, 970), (60, 910), (300, 670)):
            below = make_blob("node1/commitlogs/below.log", threshold - 1)
            at = make_blob("node1/commitlogs/at.log", threshold)
            storage = self._commitlog_storage([below, at])

            self.assertEqual(1, purge_commitlogs(storage, 'node1', 1000, interval_seconds))
            storage.storage_driver.delete_object.assert_called_once_with(below)

    def test_purge_commitlogs_lists_only_node_prefix(self):
        storage = self._commitlog_storage([])
        purge_commitlogs(storage, 'node1', 1000, 60)
        storage.storage_driver.list_objects.assert_called_once_with('node1/commitlogs/')

        storage = self._commitlog_storage([], prefix_path='some/prefix/')
        purge_commitlogs(storage, 'node1', 1000, 60)
        storage.storage_driver.list_objects.assert_called_once_with('some/prefix/node1/commitlogs/')

    def test_purge_commitlogs_logs_deleted_count(self):
        blobs = [make_blob("node1/commitlogs/{}.log".format(ts), ts) for ts in (1, 2, 3, 5000)]
        storage = self._commitlog_storage(blobs)
        with self.assertLogs(level='INFO') as logs:
            purge_commitlogs(storage, 'node1', 1000, 60)
        self.assertIn('INFO:root:Purged 3 commitlog segments of node1', logs.output)

    def test_oldest_kept_snapshot_time(self):
        backups = [
            self._pitr_node_backup('one', 100, None),
            self._pitr_node_backup('two', 200, 1500),
            self._pitr_node_backup('three', 300, 1200),
        ]
        self.assertEqual(1200, oldest_kept_snapshot_time(backups))
        self.assertIsNone(oldest_kept_snapshot_time([self._pitr_node_backup('one', 100, None)]))
        self.assertIsNone(oldest_kept_snapshot_time([]))

    def _run_main(self, config, backups, max_backup_count=0):
        with patch('medusa.purge.Storage') as storage_cls, \
                patch('medusa.purge.Monitoring'), \
                patch('medusa.purge.purge_backups', return_value=(0, 0, 0)), \
                patch('medusa.purge.purge_commitlogs') as purge_commitlogs_mock:
            storage = storage_cls.return_value.__enter__.return_value
            storage.list_backup_index_blobs.return_value = []
            storage.list_node_backups.return_value = backups
            result = main(config, max_backup_age=0, max_backup_count=max_backup_count)
        return result, storage, purge_commitlogs_mock

    def test_main_skips_commitlog_purge_when_pitr_disabled(self):
        backups = [self._pitr_node_backup('one', 100, 1000)]
        # no [pitr] section at all
        _, _, purge_commitlogs_mock = self._run_main(self.config, backups)
        purge_commitlogs_mock.assert_not_called()
        # [pitr] enabled = false
        _, _, purge_commitlogs_mock = self._run_main(self._pitr_config('false'), backups)
        purge_commitlogs_mock.assert_not_called()

    def test_main_purges_commitlogs_anchored_on_surviving_backups(self):
        backups = [
            self._pitr_node_backup('one', 100, 1000),   # purged by count
            self._pitr_node_backup('two', 200, None),   # purged by count
            self._pitr_node_backup('three', 300, 1300),  # kept -> anchor
        ]
        result, storage, purge_commitlogs_mock = self._run_main(self._pitr_config('true'), backups,
                                                                max_backup_count=1)
        purge_commitlogs_mock.assert_called_once_with(storage, 'node1', 1300, 60)
        self.assertEqual((0, 0, 0, 2), result)

    def test_main_no_anchor_deletes_nothing(self):
        backups = [self._pitr_node_backup('one', 100, None), self._pitr_node_backup('two', 200, None)]
        with self.assertLogs(level='INFO') as logs:
            _, _, purge_commitlogs_mock = self._run_main(self._pitr_config('true'), backups)
        purge_commitlogs_mock.assert_not_called()
        self.assertTrue(any('snapshot time' in record.getMessage() for record in logs.records))


if __name__ == '__main__':
    unittest.main()
