# -*- coding: utf-8 -*-
# Copyright 2024 DataStax, Inc.
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
import json
from unittest.mock import MagicMock
from medusa.storage import Storage

class GetAllManifestsTest(unittest.TestCase):
    def setUp(self):
        self.config = MagicMock()
        self.config.fqdn = "test-fqdn"
        self.config.prefix = "prefix"
        self.config.storage_provider = "local" # Just to pass initialization check
        self.config.k8s_mode = None
        self.storage = Storage(config=self.config)

    def test_get_files_from_all_differential_backups(self):
        # Create dummy backups
        # Full Backup (Should be ignored by differential aggregation)
        backup1 = MagicMock()
        backup1.name = "backup1"
        backup1.is_differential = False
        backup1.manifest = json.dumps([
            {
                "keyspace": "ks1",
                "columnfamily": "cf1",
                "objects": [
                    {
                        "path": "prefix/test-fqdn/backup1/data/ks1/cf1/f_full.db",
                        "size": 100,
                        "MD5": "md5_1",
                        "source_size": 90,
                        "source_MD5": "src_md5_1"
                    }
                ]
            }
        ])

        # Differential Backup 1
        backup2 = MagicMock()
        backup2.name = "backup2"
        backup2.is_differential = True
        backup2.manifest = json.dumps([
             {
                "keyspace": "ks1",
                "columnfamily": "cf1",
                "objects": [
                    {
                        "path": "prefix/test-fqdn/backup2/data/ks1/cf1/f_diff1.db",
                        "size": 110,
                        "MD5": "md5_2",
                        "source_size": 95,
                        "source_MD5": "src_md5_2"
                    }
                ]
            }
        ])

        # Differential Backup 2
        backup3 = MagicMock()
        backup3.name = "backup3"
        backup3.is_differential = True
        backup3.manifest = json.dumps([
             {
                "keyspace": "ks1",
                "columnfamily": "cf1",
                "objects": [
                    {
                        # Update to f_diff1.db
                        "path": "prefix/test-fqdn/backup3/data/ks1/cf1/f_diff1.db",
                        "size": 111,
                        "MD5": "md5_2_updated",
                        "source_size": 96,
                        "source_MD5": "src_md5_2_updated"
                    },
                    {
                        "path": "prefix/test-fqdn/backup3/data/ks1/cf1/f_diff2.db",
                        "size": 200,
                        "MD5": "md5_3",
                        "source_size": 190,
                        "source_MD5": "src_md5_3"
                    }
                ]
            }
        ])

        # Mock list_node_backups to return these in chronological order
        self.storage.list_node_backups = MagicMock(return_value=[backup1, backup2, backup3])

        # Call the method
        files = self.storage.get_files_from_all_differential_backups()

        # Check structure: files[ks][table][filename]
        self.assertIn("ks1", files)
        self.assertIn("cf1", files["ks1"])

        cf_files = files["ks1"]["cf1"]

        # Verify f_full.db from backup1 (Full) is NOT included
        self.assertNotIn("f_full.db", cf_files)

        # Verify f_diff1.db is from backup3 (latest)
        self.assertIn("f_diff1.db", cf_files)
        f_diff1 = cf_files["f_diff1.db"]
        self.assertEqual(f_diff1.source_MD5, "src_md5_2_updated")
        self.assertEqual(f_diff1.path, "prefix/test-fqdn/backup3/data/ks1/cf1/f_diff1.db")

        # Verify f_diff2.db is included
        self.assertIn("f_diff2.db", cf_files)
        f_diff2 = cf_files["f_diff2.db"]
        self.assertEqual(f_diff2.source_MD5, "src_md5_3")

if __name__ == '__main__':
    unittest.main()
