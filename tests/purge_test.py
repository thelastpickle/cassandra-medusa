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

from medusa.config import MedusaConfig, StorageConfig, _namedtuple_from_dict
from medusa.storage import Storage
from medusa.purge import backups_to_purge_by_age, backups_to_purge_by_count, backups_to_purge_by_name
from medusa.purge import filter_differential_backups, filter_files_within_gc_grace, find_backups_to_purge

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

    def test_backups_to_purge_by_completion(self):
        backups = list()

        # Build a list of 40 bi-daily backups, making every second backup incomplete
        complete = True
        now = datetime.now()
        for i in range(1, 80, 2):
            file_time = now + timedelta(days=(i + 1) - 80)
            backups.append(make_node_backup(self.storage, str(i), file_time, differential=True, complete=complete))
            complete = not complete

        complete_backup_names = {nb.name for nb in filter(lambda nb: nb.finished is not None, backups)}
        self.assertEqual(len(complete_backup_names), len(backups) / 2, "The amount of complete backups is not correct")

        # the age of the backups is 78 (oldest), to 0 the newest, with a 2 days step
        self.assertEqual(timedelta(days=78).days, timedelta(seconds=now.timestamp() - backups[0].started).days)
        self.assertEqual(timedelta(days=76).days, timedelta(seconds=now.timestamp() - backups[1].started).days)
        self.assertEqual(timedelta(days=74).days, timedelta(seconds=now.timestamp() - backups[2].started).days)
        self.assertEqual(timedelta(days=0).days, timedelta(seconds=now.timestamp() - backups[-1].started).days)

        prefer_incomplete = False

        # the base case with all purge parameters disabled
        max_backup_age, max_backup_count = 0, 0
        r = find_backups_to_purge(complete_backup_names, backups, prefer_incomplete, max_backup_age, max_backup_count)
        self.assertEqual(0, len(r))

        # discarding some of the backups by count
        max_backup_age, max_backup_count = 0, 25
        r = find_backups_to_purge(complete_backup_names, backups, prefer_incomplete, max_backup_age, max_backup_count)
        self.assertEqual(15, len(r))
        # we're purging slightly different amounts of complete and incomplete backups because 25 is an odd number
        self.assertEqual(7, len({nb for nb in r if nb.finished is None}))
        self.assertEqual(8, len({nb for nb in r if nb.finished is not None}))

        # discarding some of the backups by age
        max_backup_age, max_backup_count = 15, 0
        r = find_backups_to_purge(complete_backup_names, backups, prefer_incomplete, max_backup_age, max_backup_count)
        # oldest can be 15 days, so we purge 80-15=65 days worth of backups, so ~32 of them
        self.assertEqual(32, len(r))
        self.assertEqual(16, len({nb for nb in r if nb.finished is None}))
        self.assertEqual(16, len({nb for nb in r if nb.finished is not None}))

        max_backup_age, max_backup_count = 40, 15
        r = find_backups_to_purge(complete_backup_names, backups, prefer_incomplete, max_backup_age, max_backup_count)
        # 40 days age limit means purging 19 backups only, so we need to purge more based on the count
        self.assertEqual(25, len(r))
        # because we don't care about the completion, we purge comparable amounts
        self.assertEqual(12, len({nb for nb in r if nb.finished is None}))
        self.assertEqual(13, len({nb for nb in r if nb.finished is not None}))

        prefer_incomplete = True

        # just enabling prefer incomplete does not change anything
        max_backup_age, max_backup_count = 0, 0
        r = find_backups_to_purge(complete_backup_names, backups, prefer_incomplete, max_backup_age, max_backup_count)
        self.assertEqual(0, len(r))

        # keep 25 backups again, but prefer to purge the incomplete ones
        max_backup_age, max_backup_count = 0, 25
        r = find_backups_to_purge(complete_backup_names, backups, prefer_incomplete, max_backup_age, max_backup_count)
        self.assertEqual(15, len(r))
        # we're keeping more than half of the backups, so we only get to purge 15 incomplete ones
        self.assertEqual(15, len({nb for nb in r if nb.finished is None}))
        self.assertEqual(0, len({nb for nb in r if nb.finished is not None}))

        # if we keep only 15 backups, we need to purge all incomplete and 5 complete
        max_backup_age, max_backup_count = 0, 15
        r = find_backups_to_purge(complete_backup_names, backups, prefer_incomplete, max_backup_age, max_backup_count)
        self.assertEqual(25, len(r))
        self.assertEqual(20, len({nb for nb in r if nb.finished is None}))
        self.assertEqual(5, len({nb for nb in r if nb.finished is not None}))

        # now we also limit the age to 15 days
        max_backup_age, max_backup_count = 15, 15
        r = find_backups_to_purge(complete_backup_names, backups, prefer_incomplete, max_backup_age, max_backup_count)
        # we purged 32 (got left with 8) backups based on the age, the count never come to play
        self.assertEqual(32, len(r))
        self.assertEqual(16, len({nb for nb in r if nb.finished is None}))
        self.assertEqual(16, len({nb for nb in r if nb.finished is not None}))

        # increase the backup age to 40 days
        max_backup_age, max_backup_count = 40, 15
        r = find_backups_to_purge(complete_backup_names, backups, prefer_incomplete, max_backup_age, max_backup_count)
        # age limit of 40 means retaining 21, purging 19.
        # but we limit the retaining to only 15, so we need to purge 6 more, totalling 25 (+15=40)
        self.assertEqual(25, len(r))
        # we want to see more of the incomplete backups purged
        self.assertEqual(15, len({nb for nb in r if nb.finished is None}))
        self.assertEqual(10, len({nb for nb in r if nb.finished is not None}))

        # increasing the age limit means more incomplete backups gone, which allows complete ones to stay
        max_backup_age, max_backup_count = 60, 15
        r = find_backups_to_purge(complete_backup_names, backups, prefer_incomplete, max_backup_age, max_backup_count)
        self.assertEqual(25, len(r))
        self.assertEqual(20, len({nb for nb in r if nb.finished is None}))
        self.assertEqual(5, len({nb for nb in r if nb.finished is not None}))


if __name__ == '__main__':
    unittest.main()
