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
from unittest.mock import patch

from medusa.config import MedusaConfig, StorageConfig, _namedtuple_from_dict
from medusa.storage import Storage, ClusterBackup
from medusa.purge import (
    backups_to_purge_by_age, backups_to_purge_by_count, backups_to_purge_by_name, backups_to_purge_by_completion,
    backups_to_purge_by_cluster_backup_completion
)
from medusa.purge import filter_differential_backups, filter_files_within_gc_grace

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

    def test_purge_backups_by_completion(self):
        backups = list()

        # Build a list of 40 bi-daily backups, making every second backup incomplete
        complete = True
        now = datetime.now()
        for i in range(0, 80, 2):
            file_time = now + timedelta(days=(i + 1) - 80)
            backups.append(make_node_backup(self.storage, str(i), file_time, differential=True, complete=complete))
            complete = not complete

        self.assertEqual(40, len(backups))
        complete_backup_names = {nb.name for nb in filter(lambda nb: nb.finished is not None, backups)}
        self.assertEqual(len(complete_backup_names), 20, "The amount of complete backups is not correct")

        # the base with all 40 backups
        complete, incomplete_to_purge = backups_to_purge_by_completion(backups)
        self.assertEqual(20, len(complete))  # 1 is kept because it might be in progress
        self.assertEqual(19, len(incomplete_to_purge))  # 1 is kept because it might be in progress

        # take all complete backups, but only half of the incomplete ones
        test_backups = list()
        for i in range(0, 40, 1):
            # take each complete backup
            if backups[i].finished is not None:
                test_backups.append(backups[i])
                continue
            # but only first half of the incomplete ones
            if i > 20:
                continue
            test_backups.append(backups[i])
        self.assertEqual(20, len(list(filter(lambda b: b.finished is not None, test_backups))))
        self.assertEqual(10, len(list(filter(lambda b: b.finished is None, test_backups))))
        complete, incomplete_to_purge = backups_to_purge_by_completion(test_backups)
        self.assertEqual(20, len(complete))  # 1 is kept because it might be in progress
        self.assertEqual(9, len(incomplete_to_purge))  # 1 is kept because it might be in progress

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

    # patch a call of cluster_backup.missing_nodes because it'd actually go and read a blob off disk
    # it's ok to do this because we are not testing missing nodes; we only test backup completion
    # we will indicate cluster backup completion via marking one of its node backups as incomplete
    @patch.object(ClusterBackup, 'missing_nodes', lambda _: {})
    def test_purge_backups_by_cluster_backup_completion(self):
        t = datetime.now()
        complete_node_backups = [
            make_node_backup(self.storage, "backup1", t, differential=True, complete=True, fqdn="node1"),
            make_node_backup(self.storage, "backup2", t, differential=True, complete=True, fqdn="node1"),
            make_node_backup(self.storage, "backup3", t, differential=True, complete=True, fqdn="node1"),
        ]
        cluster_backups = [
            ClusterBackup("backup1", [
                make_node_backup(self.storage, "backup1", t, differential=True, complete=True, fqdn="node1"),
                make_node_backup(self.storage, "backup1", t, differential=True, complete=True, fqdn="node2"),
            ]),
            ClusterBackup("backup2", [
                make_node_backup(self.storage, "backup2", t, differential=True, complete=True, fqdn="node1"),
                # backup 2 has an unfinished node, this will render it purge-able by cluster backup completion
                make_node_backup(self.storage, "backup2", t, differential=True, complete=False, fqdn="node2"),
            ]),
        ]
        # verify the backup2 is not purge-able by looking at node backups alone
        complete, incomplete_to_purge = backups_to_purge_by_completion(complete_node_backups)
        self.assertEqual(3, len(complete))
        self.assertEqual(0, len(incomplete_to_purge))
        # verify that backup2 becomes eligible for purge if cross-checked with cluster backups
        backups_to_purge = backups_to_purge_by_cluster_backup_completion(complete_node_backups, cluster_backups)
        self.assertEqual(1, len(backups_to_purge))
        self.assertEqual("backup2", backups_to_purge.pop().name)


if __name__ == '__main__':
    unittest.main()
