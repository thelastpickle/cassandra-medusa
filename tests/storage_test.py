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

import base64
import configparser
import hashlib
import json
import os
import pathlib
import shutil
import tempfile
import unittest

from datetime import datetime
from random import randrange
from unittest.mock import patch

import medusa.storage.abstract_storage

from medusa.storage import NodeBackup, ClusterBackup
from medusa.storage.node_backup import ManifestNotFoundError
from medusa.storage.abstract_storage import AbstractStorage, AbstractBlob, ManifestObject
from medusa.config import MedusaConfig, StorageConfig, _namedtuple_from_dict, CassandraConfig
from medusa.index import build_indices
from medusa.storage import Storage


class AttributeDict(dict):
    __slots__ = ()
    __getattr__ = dict.__getitem__
    __setattr__ = dict.__setitem__


class StorageTest(unittest.TestCase):
    TEST_FILE_CONTENT = "content of the test file1"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.local_storage_dir = "/tmp/medusa_local_storage"
        self.medusa_bucket_dir = "/tmp/medusa_test_bucket"

    def setUp(self):
        NodeBackup.clear_manifest_stats_memo()
        if os.path.isdir(self.local_storage_dir):
            shutil.rmtree(self.local_storage_dir)
        if os.path.isdir(self.medusa_bucket_dir):
            shutil.rmtree(self.medusa_bucket_dir)

        os.makedirs(self.local_storage_dir)
        config = configparser.ConfigParser(interpolation=None)
        config['storage'] = {
            'host_file_separator': ',',
            'bucket_name': 'medusa_test_bucket',
            'key_file': '',
            'storage_provider': 'local',
            'fqdn': '127.0.0.1',
            'api_key_or_username': '',
            'api_secret_or_password': '',
            'base_path': '/tmp',
            'concurrent_transfers': 1,
        }
        config['cassandra'] = {
            'is_ccm': 1
        }

        self.config = MedusaConfig(
            file_path=None,
            storage=_namedtuple_from_dict(StorageConfig, config['storage']),
            cassandra=_namedtuple_from_dict(CassandraConfig, config['cassandra']),
            monitoring={},
            ssh=None,
            checks=None,
            logging=None,
            grpc=None,
            kubernetes=None,
        )

        self.storage = Storage(config=self.config.storage)

    def test_add_object_from_string(self):
        file_content = self.TEST_FILE_CONTENT
        self.storage.storage_driver.upload_blob_from_string("test1/file.txt", file_content)
        self.assertEqual(self.storage.storage_driver.get_blob_content_as_string("test1/file.txt"), file_content)

    def test_download_blobs(self):
        files_to_download = list()
        file1_content = self.TEST_FILE_CONTENT
        file2_content = "content of the test file2"
        self.storage.storage_driver.upload_blob_from_string("test_download_blobs1/file1.txt", file1_content)
        files_to_download.append("test_download_blobs1/file1.txt")
        self.storage.storage_driver.upload_blob_from_string("test_download_blobs2/file2.txt", file2_content)
        files_to_download.append("test_download_blobs2/file2.txt")
        self.assertEqual(len(os.listdir(self.medusa_bucket_dir)), 2)
        self.storage.storage_driver.download_blobs(files_to_download, self.local_storage_dir)
        self.assertEqual(len(os.listdir(self.local_storage_dir)), 2)

    def test_list_objects(self):
        file1_content = self.TEST_FILE_CONTENT
        file2_content = "content of the test file2"
        file3_content = ""
        self.storage.storage_driver.upload_blob_from_string("test_download_blobs1/file1.txt", file1_content)
        self.storage.storage_driver.upload_blob_from_string("test_download_blobs2/file2.txt", file2_content)
        self.storage.storage_driver.upload_blob_from_string("test_download_blobs3/file3.txt", file3_content)
        objects = self.storage.storage_driver.list_objects()
        self.assertEqual(len(objects), 3)
        one_object = self.storage.storage_driver.list_objects("test_download_blobs2")
        self.assertEqual(len(one_object), 1)

    def test_read_blob(self):
        file1_content = self.TEST_FILE_CONTENT
        self.storage.storage_driver.upload_blob_from_string("test_download_blobs1/file1.txt", file1_content)
        objects = self.storage.storage_driver.list_objects("test_download_blobs1")
        object_content = self.storage.storage_driver.read_blob_as_string(objects[0])
        self.assertEqual(object_content, file1_content)

    def test_get_blob(self):
        file1_content = self.TEST_FILE_CONTENT
        self.storage.storage_driver.upload_blob_from_string("test_download_blobs1/file1.txt", file1_content)
        obj = self.storage.storage_driver.get_blob("test_download_blobs1/file1.txt")
        self.assertEqual(obj.name, "test_download_blobs1/file1.txt")

    def test_read_blob_as_bytes(self):
        file1_content = self.TEST_FILE_CONTENT
        self.storage.storage_driver.upload_blob_from_string("test_download_blobs1/file1.txt", file1_content)
        object_content = self.storage.storage_driver.get_blob_content_as_bytes("test_download_blobs1/file1.txt")
        self.assertEqual(object_content, self.TEST_FILE_CONTENT.encode())

    def test_verify_hash(self):
        file1_content = self.TEST_FILE_CONTENT
        manifest = self.storage.storage_driver.upload_blob_from_string("test_download_blobs1/file1.txt", file1_content)
        obj = self.storage.storage_driver.get_blob("test_download_blobs1/file1.txt")
        self.assertEqual(manifest.MD5, obj.hash)

    def test_hashes_match(self):
        # Should match
        hash1 = "S1EAM/BVMqhbJnAUs/nWlQ=="
        hash2 = "4b510033f05532a85b267014b3f9d695"
        self.assertTrue(
            medusa.storage.abstract_storage.AbstractStorage.hashes_match(hash1, hash2)
        )

        # Should match
        hash1 = "4b510033f05532a85b267014b3f9d695"
        hash2 = "4b510033f05532a85b267014b3f9d695"
        self.assertTrue(
            medusa.storage.abstract_storage.AbstractStorage.hashes_match(hash1, hash2)
        )

        # Should not match
        hash1 = "S1EAM/BVMqhbJnAUs/nWlQsdfsdf=="
        hash2 = "4b510033f05532a85b267014b3f9d695"
        self.assertFalse(
            medusa.storage.abstract_storage.AbstractStorage.hashes_match(hash1, hash2)
        )

    def test_generate_md5_hash(self):
        with tempfile.NamedTemporaryFile() as tf:
            # write random bytes
            two_megabytes = 2 * 1024 * 1024
            tf.write(os.urandom(two_megabytes))
            tf.flush()

            # compute checksum of the whole file at once
            tf.seek(0)
            checksum_full = hashlib.md5(tf.read()).digest()
            digest_full = base64.encodebytes(checksum_full).decode('UTF-8').strip()

            # compute checksum using default-size chunks
            tf.seek(0)
            digest_chunk = AbstractStorage.generate_md5_hash(tf.name)

            # compare the digests
            self.assertEqual(digest_chunk, digest_full)

            # compute checksum using custom size chunks
            tf.seek(0)
            self.assertEqual(digest_full, AbstractStorage.generate_md5_hash(tf.name, block_size=128))
            tf.seek(0)
            self.assertEqual(digest_full, AbstractStorage.generate_md5_hash(tf.name, block_size=256))
            tf.seek(0)
            self.assertEqual(digest_full, AbstractStorage.generate_md5_hash(tf.name, block_size=1024))
            tf.seek(0)
            self.assertEqual(digest_full, AbstractStorage.generate_md5_hash(tf.name, block_size=100000000))     # 100M
            tf.seek(0)
            self.assertEqual(digest_full, AbstractStorage.generate_md5_hash(tf.name, block_size=-1))
            tf.seek(0)
            self.assertNotEqual(digest_full, AbstractStorage.generate_md5_hash(tf.name, block_size=0))

    def test_get_object_datetime(self):
        file1_content = self.TEST_FILE_CONTENT
        self.storage.storage_driver.upload_blob_from_string("test_download_blobs1/file1.txt", file1_content)
        obj = self.storage.storage_driver.get_blob("test_download_blobs1/file1.txt")
        self.assertEqual(
            obj.last_modified,
            self.storage.storage_driver.get_object_datetime(obj)
        )

    def test_get_fqdn_from_backup_index_blob(self):
        blob_name = "index/backup_index/2019051307/manifest_node1.whatever.com.json"
        self.assertEqual(
            "node1.whatever.com",
            self.storage.get_fqdn_from_any_index_blob(blob_name)
        )

        blob_name = "index/backup_index/2019051307/schema_node2.whatever.com.cql"
        self.assertEqual(
            "node2.whatever.com",
            self.storage.get_fqdn_from_any_index_blob(blob_name)
        )

        blob_name = "index/backup_index/2019051307/schema_node3.whatever.com.txt"
        self.assertEqual(
            "node3.whatever.com",
            self.storage.get_fqdn_from_any_index_blob(blob_name)
        )

        blob_name = "index/backup_index/2019051307/schema_node_with_underscores.whatever.com.txt"
        self.assertEqual(
            "node_with_underscores.whatever.com",
            self.storage.get_fqdn_from_any_index_blob(blob_name)
        )

    def test_get_fqdn_from_any_index_blob(self):
        blob_name = "tokenmap_hostname-with-dashes-and-3-numbers.json"
        self.assertEqual(
            "hostname-with-dashes-and-3-numbers",
            self.storage.get_fqdn_from_any_index_blob(blob_name)
        )
        blob_name = "tokenmap_hostname-with-dashes.and-dots.json"
        self.assertEqual(
            "hostname-with-dashes.and-dots",
            self.storage.get_fqdn_from_any_index_blob(blob_name)
        )
        blob_name = "tokenmap_hostname_with-underscores.and-dots-and.dashes.json"
        self.assertEqual(
            "hostname_with-underscores.and-dots-and.dashes",
            self.storage.get_fqdn_from_any_index_blob(blob_name)
        )
        blob_name = "index/bi/third_backup/finished_localhost_1574343029.timestamp"
        self.assertEqual(
            "localhost",
            self.storage.get_fqdn_from_any_index_blob(blob_name)
        )

    def test_parse_backup_index(self):
        file_content = "content of the test file"
        # SSTables for node1 and backup1
        self.storage.storage_driver.upload_blob_from_string(
            "{}node1/backup1/data/ks1/sstable1.db".format(self.storage.prefix_path), file_content)
        self.storage.storage_driver.upload_blob_from_string(
            "{}node1/backup1/data/ks1/sstable2.db".format(self.storage.prefix_path), file_content)
        # Metadata for node1 and backup1
        self.storage.storage_driver.upload_blob_from_string(
            "{}node1/backup1/meta/tokenmap.json".format(self.storage.prefix_path), file_content)
        self.storage.storage_driver.upload_blob_from_string(
            "{}node1/backup1/meta/manifest.json".format(self.storage.prefix_path), file_content)
        self.storage.storage_driver.upload_blob_from_string(
            "{}node1/backup1/meta/schema.cql".format(self.storage.prefix_path), file_content)
        # SSTables for node2 and backup1
        self.storage.storage_driver.upload_blob_from_string(
            "{}node2/backup1/data/ks1/sstable1.db".format(self.storage.prefix_path), file_content)
        self.storage.storage_driver.upload_blob_from_string(
            "{}node2/backup1/data/ks1/sstable2.db".format(self.storage.prefix_path), file_content)
        # Metadata for node2 and backup1
        self.storage.storage_driver.upload_blob_from_string(
            "{}node2/backup1/meta/tokenmap.json".format(self.storage.prefix_path), file_content)
        self.storage.storage_driver.upload_blob_from_string(
            "{}node2/backup1/meta/manifest.json".format(self.storage.prefix_path), file_content)
        self.storage.storage_driver.upload_blob_from_string(
            "{}node2/backup1/meta/schema.cql".format(self.storage.prefix_path), file_content)
        # SSTables for node1 and backup2
        self.storage.storage_driver.upload_blob_from_string(
            "{}node1/backup2/data/ks1/sstable1.db".format(self.storage.prefix_path), file_content)
        self.storage.storage_driver.upload_blob_from_string(
            "{}node1/backup2/data/ks1/sstable2.db".format(self.storage.prefix_path), file_content)
        # Metadata for node1 and backup2
        self.storage.storage_driver.upload_blob_from_string(
            "{}node1/backup2/meta/tokenmap.json".format(self.storage.prefix_path), file_content)
        self.storage.storage_driver.upload_blob_from_string(
            "{}node1/backup2/meta/manifest.json".format(self.storage.prefix_path), file_content)
        self.storage.storage_driver.upload_blob_from_string(
            "{}node1/backup2/meta/schema.cql".format(self.storage.prefix_path), file_content)
        build_indices(self.config, False)
        path = '{}index/backup_index'.format(self.storage.prefix_path)
        backup_index = self.storage.storage_driver.list_objects(path)
        blobs_by_backup = self.storage.group_backup_index_by_backup_and_node(backup_index)
        self.assertTrue("backup1" in blobs_by_backup)
        self.assertTrue("backup2" in blobs_by_backup)
        self.assertTrue("node1" in blobs_by_backup["backup1"])
        self.assertTrue("node2" in blobs_by_backup["backup1"])
        self.assertTrue("node1" in blobs_by_backup["backup2"])
        self.assertFalse("node2" in blobs_by_backup["backup2"])

    def test_parse_backup_index_with_wrong_names(self):
        file_content = "content of the test file"
        prefix_path = self.storage.prefix_path

        # Index files for a backup
        self.storage.storage_driver.upload_blob_from_string(
            "{}index/backup_index/backup3/tokenmap_node1.json".format(prefix_path), file_content)
        self.storage.storage_driver.upload_blob_from_string(
            "{}index/backup_index/backup3/schema_node1.cql".format(prefix_path), file_content)
        self.storage.storage_driver.upload_blob_from_string(
            "{}index/backup_index/backup3/started_node1_1689598370.timestamp".format(prefix_path), file_content)
        self.storage.storage_driver.upload_blob_from_string(
            "{}index/backup_index/backup3/finished_node1_1689598370.timestamp".format(prefix_path), file_content)
        # Files that we want to see filtered out
        self.storage.storage_driver.upload_blob_from_string(
            "{}index/backup_index/extra_folder/backup3/tokenmap_node2.json".format(prefix_path), file_content)
        self.storage.storage_driver.upload_blob_from_string(
            "{}index/missing_folder/tokenmap_node2.json".format(prefix_path), file_content)
        self.storage.storage_driver.upload_blob_from_string(
            "{}index/backup_index/missing_file".format(prefix_path), file_content)

        path = '{}index/backup_index'.format(prefix_path)
        backup_index = self.storage.storage_driver.list_objects(path)
        blobs_by_backup = self.storage.group_backup_index_by_backup_and_node(backup_index)
        self.assertEqual(1, len(blobs_by_backup.keys()))
        self.assertEqual(1, len(blobs_by_backup['backup3'].keys()))
        self.assertEqual(4, len(blobs_by_backup['backup3']['node1']))

    def test_remove_extension(self):
        self.assertEqual(
            'localhost',
            self.storage.remove_extension('localhost.txt')
        )
        self.assertEqual(
            'localhost',
            self.storage.remove_extension('localhost.timestamp')
        )
        self.assertEqual(
            'localhost',
            self.storage.remove_extension('localhost.cql')
        )
        self.assertEqual(
            'localhost.foo',
            self.storage.remove_extension('localhost.foo')
        )

    def test_get_timestamp_from_blob_name(self):
        self.assertEqual(
            1558021519,
            self.storage.get_timestamp_from_blob_name('finished_localhost_1558021519.timestamp')
        )
        self.assertEqual(
            1558021519,
            self.storage.get_timestamp_from_blob_name('finished_some.host.net_1558021519.timestamp')
        )
        self.assertEqual(
            1558021519,
            self.storage.get_timestamp_from_blob_name('finished_some_underscores.host.net_1558021519.timestamp')
        )

        self.assertEqual(
            1574343029,
            self.storage.get_timestamp_from_blob_name('index/bi/third_backup/finished_localhost_1574343029.timestamp')
        )

    def test_list_files_per_table_with_prefix(self):

        def fake_blob(s):
            return AbstractBlob(s, 0, '0', datetime.now(), None)

        def fake_mo(s):
            return ManifestObject(s, 0, '0')

        config = AttributeDict({
            'bucket_name': 'test-bucket',
            'base_path': 'test-path',
            'prefix': 'test-prefix',
            'fqdn': 'test-fqdn',
            'k8s_mode': False,
            'storage_provider': 'local'
        })
        s = Storage(config=config)
        listed_files = [
            fake_blob('test-prefix/test-fqdn/data/test-keyspace/test-table-1/test-file-1.db'),
            fake_blob('test-prefix/test-fqdn/data/test-keyspace/test-table-2/test-file-2.db'),
            fake_blob('test-prefix/test-fqdn/data/test-keyspace/test-table-1/.index/test-file-1.db'),
            fake_blob('test-prefix/test-fqdn/data/test-keyspace-2/test-table-2/test-file-1.db'),
            fake_blob('test-prefix/test-fqdn/data/dse/metadata/nodes/local'),
            # not having prefix actually doesn't matter because we parse from the end
            fake_blob('test-fqdn/data/test-keyspace-3/test-table-1/test-file-1.db'),
            fake_blob('test-fqdn/data/test-keyspace-3/test-table-2/test-file-2.db'),

        ]
        expected_grouping = {
            'test-keyspace': {
                'test-table-1': {
                    'test-file-1.db': fake_mo('test-prefix/test-fqdn/data/test-keyspace/test-table-1/test-file-1.db')
                },
                'test-table-1..index': {
                    'test-file-1.db':
                        fake_mo('test-prefix/test-fqdn/data/test-keyspace/test-table-1/.index/test-file-1.db')
                },
                'test-table-2': {
                    'test-file-2.db': fake_mo('test-prefix/test-fqdn/data/test-keyspace/test-table-2/test-file-2.db')
                },
            },
            'test-keyspace-2': {
                'test-table-2': {
                    'test-file-1.db': fake_mo('test-prefix/test-fqdn/data/test-keyspace-2/test-table-2/test-file-1.db')
                }
            },
            'dse': {
                'metadata.nodes': {
                    'local': fake_mo('test-prefix/test-fqdn/data/dse/metadata/nodes/local')
                }
            },
            'test-keyspace-3': {
                'test-table-1': {
                    'test-file-1.db': fake_mo('test-fqdn/data/test-keyspace-3/test-table-1/test-file-1.db')
                },
                'test-table-2': {
                    'test-file-2.db': fake_mo('test-fqdn/data/test-keyspace-3/test-table-2/test-file-2.db')
                }
            },

        }
        with patch('medusa.storage.local_storage.LocalStorage._list_blobs', return_value=listed_files):
            self.assertEqual(expected_grouping, s.list_files_per_table())

    def test_saniitize_keyspace_and_table_name(self):
        p = pathlib.Path('/some/path/keyspace/table-cfid/snapshots/snapshot-name/nb-5-big-CompressionInfo.db')
        self.assertEqual(
            ('keyspace', 'table-cfid'),
            self.storage.sanitize_keyspace_and_table_name(p)
        )
        p = pathlib.Path('/path/keyspace/table-cfid/snapshots/snapshot-name/.index_name/nb-5-big-CompressionInfo.db')
        self.assertEqual(
            ('keyspace', 'table-cfid..index_name'),
            self.storage.sanitize_keyspace_and_table_name(p)
        )
        p = pathlib.Path('other/path/prefix/fqdn/data/keyspace/table-cfid/nb-5-big-Data.db')
        self.assertEqual(
            ('keyspace', 'table-cfid'),
            self.storage.sanitize_keyspace_and_table_name(p)
        )
        p = pathlib.Path('other/path/prefix/fqdn/data/keyspace/table-cfid/.index_name/nb-5-big-Data.db')
        self.assertEqual(
            ('keyspace', 'table-cfid..index_name'),
            self.storage.sanitize_keyspace_and_table_name(p)
        )

    def test_get_table_prefix(self):
        self.assertEqual('localhost/data/', self.storage._get_table_prefix(None, 'localhost'))
        self.assertEqual('localhost/data/', self.storage._get_table_prefix('', 'localhost'))
        self.assertEqual('prefix/localhost/data/', self.storage._get_table_prefix('prefix', 'localhost'))

    def _upload_manifest(self, fqdn, backup_name, sections):
        path = "{}{}/{}/meta/manifest.json".format(self.storage.prefix_path, fqdn, backup_name)
        self.storage.storage_driver.upload_blob_from_string(path, json.dumps(sections))

    def _two_section_manifest(self):
        return [
            {"keyspace": "ks1", "columnfamily": "t1-cfid",
             "objects": [{"path": "node1/data/ks1/t1-cfid/a-Data.db", "MD5": "x", "size": 100},
                         {"path": "node1/data/ks1/t1-cfid/a-Index.db", "MD5": "y", "size": 20}]},
            {"keyspace": "ks1", "columnfamily": "t2-cfid",
             "objects": [{"path": "node1/data/ks1/t2-cfid/b-Data.db", "MD5": "z", "size": 7}]},
        ]

    def test_manifest_stats_sums_size_and_count_in_one_download(self):
        self._upload_manifest("node1", "b1", self._two_section_manifest())
        nb = make_node_backup(self.storage, "b1", datetime.now(), fqdn="node1")
        with patch.object(self.storage.storage_driver, "get_blob_content_as_string",
                          wraps=self.storage.storage_driver.get_blob_content_as_string) as spy:
            self.assertEqual((127, 3), nb.manifest_stats())
            self.assertEqual(127, nb.size())
            self.assertEqual(3, nb.num_objects())
            self.assertEqual(1, spy.call_count)
        self.assertIsNone(nb.cached_manifest)

    def test_read_manifest_does_not_cache(self):
        self._upload_manifest("node1", "b2", self._two_section_manifest())
        nb = make_node_backup(self.storage, "b2", datetime.now(), fqdn="node1")
        self.assertEqual(2, len(nb.read_manifest()))
        self.assertIsNone(nb.cached_manifest)

    def test_manifest_stats_missing_manifest_raises_and_is_not_cached(self):
        # a node backup must not pretend to be empty: backup_node.py reads size() right after uploading
        # the manifest, and a failure there has to fail the backup as it did before
        nb = make_node_backup(self.storage, "nope", datetime.now(), fqdn="node1")
        self.assertIsNone(nb.read_manifest())
        with self.assertRaises(ManifestNotFoundError):
            nb.manifest_stats()
        with self.assertRaises(ManifestNotFoundError):
            nb.size()
        self.assertIsNone(nb._manifest_stats)

    def test_cluster_backup_stats_skip_node_without_manifest_with_warning(self):
        # the listing (GetBackups / status) degrades per node instead of failing the whole call
        self._upload_manifest("node1", "cb1", self._two_section_manifest())
        # make_node_backup points the tokenmap blob at localhost/<name>/meta/tokenmap.json
        self.storage.storage_driver.upload_blob_from_string(
            "localhost/cb1/meta/tokenmap.json",
            json.dumps({n: {"tokens": [1], "is_up": True, "rack": "r", "dc": "d"} for n in ("node1", "node2")}))
        with_manifest = make_node_backup(self.storage, "cb1", datetime.now(), fqdn="node1")
        without_manifest = make_node_backup(self.storage, "cb1", datetime.now(), fqdn="node2")
        cluster_backup = ClusterBackup("cb1", [with_manifest, without_manifest])
        with self.assertLogs(level="WARNING") as logs:
            self.assertEqual(127, cluster_backup.size())
            self.assertEqual(3, cluster_backup.num_objects())
        self.assertTrue(any("cb1" in line and "node2" in line for line in logs.output), logs.output)

    def test_manifest_setter_resets_stats(self):
        self._upload_manifest("node1", "b3", self._two_section_manifest())
        nb = make_node_backup(self.storage, "b3", datetime.now(), fqdn="node1")
        self.assertEqual((127, 3), nb.manifest_stats())
        nb.manifest = json.dumps([{"keyspace": "k", "columnfamily": "c",
                                   "objects": [{"path": "p", "MD5": "m", "size": 5}]}])
        self.assertEqual((5, 1), nb.manifest_stats())

    def test_manifest_stats_malformed_raises(self):
        path = "{}node1/bad/meta/manifest.json".format(self.storage.prefix_path)
        self.storage.storage_driver.upload_blob_from_string(path, json.dumps([{"keyspace": "k"}]))
        nb = make_node_backup(self.storage, "bad", datetime.now(), fqdn="node1")
        with self.assertRaises(KeyError):
            nb.manifest_stats()

    def test_manifest_stats_memo_avoids_second_download(self):
        self._upload_manifest("node1", "m1", self._two_section_manifest())
        # the same index blob (same last_modified) seen by two listings
        indexed_at = datetime(2026, 10, 29, 12, 0, 0)
        first = make_node_backup(self.storage, "m1", indexed_at, fqdn="node1")
        self.assertEqual((127, 3), first.manifest_stats())
        second = make_node_backup(self.storage, "m1", indexed_at, fqdn="node1")
        with patch.object(self.storage.storage_driver, "get_blob_content_as_string") as spy:
            self.assertEqual((127, 3), second.manifest_stats())
            self.assertEqual(0, spy.call_count)

    def test_manifest_stats_memo_keyed_by_blob_version(self):
        self._upload_manifest("node1", "m2", self._two_section_manifest())
        old = make_node_backup(self.storage, "m2", datetime(2020, 1, 1), fqdn="node1")
        self.assertEqual((127, 3), old.manifest_stats())
        self._upload_manifest("node1", "m2", [{"keyspace": "k", "columnfamily": "c",
                                               "objects": [{"path": "p", "MD5": "m", "size": 9}]}])
        new = make_node_backup(self.storage, "m2", datetime(2021, 1, 1), fqdn="node1")
        self.assertEqual((9, 1), new.manifest_stats())

    def test_manifest_stats_memo_keyed_by_storage(self):
        # same fqdn / backup name / blob version in a different bucket is a different manifest
        indexed_at = datetime(2026, 10, 29, 12, 0, 0)
        self._upload_manifest("node1", "m4", self._two_section_manifest())
        first = make_node_backup(self.storage, "m4", indexed_at, fqdn="node1")
        self.assertEqual((127, 3), first.manifest_stats())
        config = configparser.ConfigParser(interpolation=None)
        config['storage'] = {
            'host_file_separator': ',',
            'storage_provider': 'local',
            'base_path': self.local_storage_dir,
            'bucket_name': 'other_bucket',
            'fqdn': 'node1',
        }
        other_storage = Storage(config=_namedtuple_from_dict(StorageConfig, config['storage']))
        other_storage.storage_driver.upload_blob_from_string(
            "node1/m4/meta/manifest.json",
            json.dumps([{"keyspace": "k", "columnfamily": "c", "objects": [{"path": "p", "MD5": "m", "size": 9}]}]))
        other = make_node_backup(other_storage, "m4", indexed_at, fqdn="node1")
        self.assertEqual((9, 1), other.manifest_stats())

    def test_manifest_stats_no_memo_without_manifest_blob(self):
        self._upload_manifest("node1", "m3", self._two_section_manifest())
        nb = make_unfinished_node_backup(self.storage, "m3", datetime.now(), fqdn="node1")
        self.assertEqual((127, 3), nb.manifest_stats())
        nb2 = make_unfinished_node_backup(self.storage, "m3", datetime.now(), fqdn="node1")
        with patch.object(self.storage.storage_driver, "get_blob_content_as_string",
                          wraps=self.storage.storage_driver.get_blob_content_as_string) as spy:
            nb2.manifest_stats()
            self.assertEqual(1, spy.call_count)


def make_node_backup(storage, name, backup_date, differential=False, fqdn="localhost"):
    if differential is True:
        differential_blob = make_blob("localhost/{}/meta/differential".format(name), backup_date.timestamp())
    else:
        differential_blob = None
    tokenmap_blob = make_blob("localhost/{}/meta/tokenmap.json".format(name), backup_date.timestamp())
    schema_blob = make_blob("localhost/{}/meta/schema.cql".format(name), backup_date.timestamp())
    manifest_blob = make_blob("localhost/{}/meta/manifest.json".format(name), backup_date.timestamp())
    return NodeBackup(storage=storage, fqdn=fqdn, name=str(name),
                      differential_blob=differential_blob, manifest_blob=manifest_blob,
                      tokenmap_blob=tokenmap_blob, schema_blob=schema_blob,
                      started_timestamp=backup_date.timestamp(), finished_timestamp=backup_date.timestamp())


def make_unfinished_node_backup(storage, name, backup_date, differential=False, fqdn="localhost"):
    if differential is True:
        differential_blob = make_blob("localhost/{}/meta/differential".format(name), backup_date.timestamp())
    else:
        differential_blob = None
    tokenmap_blob = make_blob("localhost/{}/meta/tokenmap.json".format(name), backup_date.timestamp())
    schema_blob = make_blob("localhost/{}/meta/schema.cql".format(name), backup_date.timestamp())
    manifest_blob = None
    return NodeBackup(storage=storage, fqdn=fqdn, name=str(name),
                      differential_blob=differential_blob, manifest_blob=manifest_blob,
                      tokenmap_blob=tokenmap_blob, schema_blob=schema_blob,
                      started_timestamp=backup_date.timestamp(), finished_timestamp=None)


def make_cluster_backup(storage, name, backup_date, nodes, differential=False):
    node_backups = list()
    for node in nodes:
        node_backups.append(make_node_backup(storage, name, backup_date, differential, node))
    return ClusterBackup(name, node_backups)


def make_blob(blob_name, blob_date):
    checksum = hashlib.md5()
    checksum.update(os.urandom(4))
    return AbstractBlob(blob_name, randrange(100), checksum.hexdigest(), datetime.fromtimestamp(blob_date), None)


if __name__ == '__main__':
    unittest.main()
