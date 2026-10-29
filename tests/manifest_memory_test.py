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
import json
import os
import shutil
import tracemalloc
import unittest

from datetime import datetime

from medusa.config import MedusaConfig, StorageConfig, _namedtuple_from_dict
from medusa.service.grpc.server import get_backup_summary
from medusa.storage import ClusterBackup, NodeBackup, Storage

from tests.storage_test import make_blob

NODES = ["n%d" % i for i in range(5)]
BACKUPS = ["b%d" % i for i in range(6)]
OBJECTS_PER_MANIFEST = 20000          # ~4 MiB of JSON each
PEAK_LIMIT = 40 * 1024 * 1024         # 30 manifests would be >120 MiB if all were held


class ManifestMemoryTest(unittest.TestCase):
    """
    Listing backups must not hold every node's manifest of every backup in memory (DESIGN §2.2).
    """
    base = "/tmp/medusa_manifest_memory"

    def setUp(self):
        shutil.rmtree(self.base, ignore_errors=True)
        os.makedirs(self.base)
        config = configparser.ConfigParser(interpolation=None)
        config['storage'] = {
            'host_file_separator': ',',
            'storage_provider': 'local',
            'base_path': self.base,
            'bucket_name': 'bucket',
            'fqdn': NODES[0],
        }
        self.config = MedusaConfig(
            file_path=None,
            storage=_namedtuple_from_dict(StorageConfig, config['storage']),
            monitoring={}, cassandra=None, ssh=None, checks=None, logging=None, grpc=None, kubernetes=None,
        )
        self.storage = Storage(config=self.config.storage)
        section = {"keyspace": "ks", "columnfamily": "cf", "objects": [
            {"path": "x/data/ks/cf/nb-%d-big-Data.db" % i, "MD5": "0123456789abcdef0123456789abcdef", "size": 160 << 20}
            for i in range(OBJECTS_PER_MANIFEST)]}
        payload = json.dumps([section])
        self.one_manifest_bytes = len(payload)
        tokenmap = json.dumps({n: {"tokens": [1], "is_up": True, "rack": "r", "dc": "d"} for n in NODES})
        for b in BACKUPS:
            for n in NODES:
                self.storage.storage_driver.upload_blob_from_string(
                    "{}{}/{}/meta/manifest.json".format(self.storage.prefix_path, n, b), payload)
                self.storage.storage_driver.upload_blob_from_string(
                    "{}{}/{}/meta/tokenmap.json".format(self.storage.prefix_path, n, b), tokenmap)

    def tearDown(self):
        shutil.rmtree(self.base, ignore_errors=True)

    def _cluster_backup(self, name):
        # like tests.storage_test.make_cluster_backup, but with integer timestamps as real index entries carry
        ts = int(datetime.now().timestamp())
        node_backups = [
            NodeBackup(storage=self.storage, fqdn=fqdn, name=name,
                       manifest_blob=make_blob("{}/{}/meta/manifest.json".format(fqdn, name), ts),
                       tokenmap_blob=make_blob("{}/{}/meta/tokenmap.json".format(fqdn, name), ts),
                       schema_blob=make_blob("{}/{}/meta/schema.cql".format(fqdn, name), ts),
                       differential_blob=make_blob("{}/{}/meta/differential".format(fqdn, name), ts),
                       started_timestamp=ts, finished_timestamp=ts)
            for fqdn in NODES
        ]
        return ClusterBackup(name, node_backups)

    def test_listing_peak_memory_is_bounded_by_one_manifest(self):
        cluster_backups = [self._cluster_backup(b) for b in BACKUPS]
        tracemalloc.start()
        summaries = [get_backup_summary(cb) for cb in cluster_backups]
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        self.assertEqual(len(BACKUPS), len(summaries))
        self.assertEqual(OBJECTS_PER_MANIFEST * len(NODES), summaries[0].totalObjects)
        self.assertLess(peak, PEAK_LIMIT, "peak %d MiB for %d manifests of %d MiB" % (
            peak >> 20, len(NODES) * len(BACKUPS), self.one_manifest_bytes >> 20))
