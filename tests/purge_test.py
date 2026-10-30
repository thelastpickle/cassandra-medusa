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
import pathlib
import shutil
import unittest

from datetime import datetime, timedelta

from medusa.config import MedusaConfig, StorageConfig, _namedtuple_from_dict
from medusa.storage import Storage
from medusa.purge import backups_to_purge_by_age, backups_to_purge_by_count, backups_to_purge_by_name
from medusa.purge import filter_differential_backups, filter_files_within_gc_grace
from medusa.purge import purge_backup, get_file_paths_from_storage

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

    def test_purge_backup_leaves_prefix_sibling_intact(self):
        # A backup name can be a string prefix of another backup's name, as with
        # backup-2 and backup-2026. Purging the first must not take the second's
        # objects with it.
        root = pathlib.Path('/tmp/purge_test')
        self.addCleanup(shutil.rmtree, root, ignore_errors=True)
        for name in ('backup-2', 'backup-2026'):
            meta_dir = root / 'localhost' / name / 'meta'
            meta_dir.mkdir(parents=True, exist_ok=True)
            (meta_dir / 'manifest.json').write_text('[]')
            index_dir = root / 'index' / 'backup_index' / name
            index_dir.mkdir(parents=True, exist_ok=True)
            (index_dir / 'finished_localhost_1.timestamp').write_text('1')

        purge_backup(self.storage, make_node_backup(self.storage, 'backup-2', datetime.now()))

        assert not (root / 'localhost' / 'backup-2' / 'meta' / 'manifest.json').exists()
        assert (root / 'localhost' / 'backup-2026' / 'meta' / 'manifest.json').exists()
        assert not (root / 'index' / 'backup_index' / 'backup-2' / 'finished_localhost_1.timestamp').exists()
        assert (root / 'index' / 'backup_index' / 'backup-2026' / 'finished_localhost_1.timestamp').exists()

    def _storage_tree(self, prefix=''):
        # Fresh local bucket for tests that lay out real objects. Returns its root and a Storage on it.
        root = pathlib.Path('/tmp/purge_test')
        shutil.rmtree(root, ignore_errors=True)
        self.addCleanup(shutil.rmtree, root, ignore_errors=True)
        storage_config = self.config.storage._replace(prefix=prefix) if prefix else self.config.storage
        return root, Storage(config=storage_config)

    @staticmethod
    def _put_node_backup(root, fqdn, name, differential=False):
        # The objects and index entries a node backup leaves in storage, named as medusa names them.
        # Returns their paths relative to the bucket root.
        meta = ['manifest.json', 'schema.cql', 'tokenmap.json', 'server_version.json']
        index = ['tokenmap_{f}.json', 'schema_{f}.cql', 'manifest_{f}.json',
                 'started_{f}_1700000000.timestamp', 'finished_{f}_1700000100.timestamp']
        if differential:
            meta.append('differential')
            index.append('differential_{f}')
        paths = ['{}/{}/meta/{}'.format(fqdn, name, m) for m in meta]
        if not differential:
            paths.append('{}/{}/data/ks/t-1/nb-1-big-Data.db'.format(fqdn, name))
        paths += ['index/backup_index/{}/{}'.format(name, i.format(f=fqdn)) for i in index]
        for path in paths:
            (root / path).parent.mkdir(parents=True, exist_ok=True)
            (root / path).write_text(path)
        return set(paths)

    @staticmethod
    def _files(root):
        return {str(p.relative_to(root)) for p in root.rglob('*') if p.is_file()}

    def test_purge_backup_deletes_exactly_its_own_objects(self):
        # Two nodes whose names are prefixes of each other (as with pods ...-sts-1 and ...-sts-10), two backups
        # whose names are prefixes of each other, full and differential, plus the node's shared data folder.
        # Purging one node's backup must remove all of its objects and index entries, and nothing else.
        root, storage = self._storage_tree()
        target = self._put_node_backup(root, 'node1', 'backup-2')
        for fqdn in ('node1', 'node10'):
            for name, differential in (('backup-2', False), ('backup-2026', True)):
                if (fqdn, name) != ('node1', 'backup-2'):
                    self._put_node_backup(root, fqdn, name, differential)
            (root / fqdn / 'data' / 'ks' / 't-1').mkdir(parents=True, exist_ok=True)
            (root / fqdn / 'data' / 'ks' / 't-1' / 'nb-7-big-Data.db').write_text('shared')
        before = self._files(root)

        purge_backup(storage, make_node_backup(storage, 'backup-2', datetime.now(), fqdn='node1'))

        self.assertEqual(target, before - self._files(root))

    def test_purge_backup_ignores_fqdn_appearing_in_backup_name(self):
        # The node is identified by the index file name, not by a substring anywhere in the path:
        # in index/backup_index/nightly_node1/manifest_node2.json the "_node1" belongs to the backup name.
        root, storage = self._storage_tree()
        target = self._put_node_backup(root, 'node1', 'nightly_node1')
        self._put_node_backup(root, 'node2', 'nightly_node1')
        before = self._files(root)

        purge_backup(storage, make_node_backup(storage, 'nightly_node1', datetime.now(), fqdn='node1'))

        self.assertEqual(target, before - self._files(root))

    def test_purge_backup_is_scoped_under_a_storage_prefix(self):
        # Multi-tenant buckets put everything under <prefix>/; the scoping must hold there too.
        root, storage = self._storage_tree(prefix='tenant')
        target = {'tenant/' + p for p in self._put_node_backup(root / 'tenant', 'node1', 'backup-2')}
        self._put_node_backup(root / 'tenant', 'node1', 'backup-2026')
        self._put_node_backup(root / 'tenant', 'node10', 'backup-2')
        before = self._files(root)

        purge_backup(storage, make_node_backup(storage, 'backup-2', datetime.now(), fqdn='node1'))

        self.assertEqual(target, before - self._files(root))

    def test_obsolete_file_candidates_exclude_a_backup_named_like_the_data_folder(self):
        # cleanup_obsolete_files() deletes what get_file_paths_from_storage() returns and no manifest references.
        # A backup named data-weekly lives at <fqdn>/data-weekly/, next to the shared <fqdn>/data/ folder.
        root, storage = self._storage_tree()
        self._put_node_backup(root, 'node1', 'data-weekly')
        (root / 'node1' / 'data' / 'ks' / 't-1').mkdir(parents=True)
        (root / 'node1' / 'data' / 'ks' / 't-1' / 'nb-9-big-Data.db').write_text('orphan')

        candidates = get_file_paths_from_storage(storage, 'node1')

        self.assertEqual({'node1/data/ks/t-1/nb-9-big-Data.db'}, set(candidates))

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


if __name__ == '__main__':
    unittest.main()
