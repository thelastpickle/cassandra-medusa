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
import os
import shutil
import tempfile
import unittest

from datetime import datetime
from random import randrange

import medusa.storage.abstract_storage

from medusa.storage import NodeBackup, ClusterBackup
from medusa.storage.abstract_storage import AbstractStorage, AbstractBlob
from medusa.config import MedusaConfig, StorageConfig, _namedtuple_from_dict, CassandraConfig
from medusa.index import build_indices
from medusa.storage import Storage


class StorageTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.local_storage_dir = "/tmp/medusa_local_storage"
        self.medusa_bucket_dir = "/tmp/medusa_test_bucket"

    def setUp(self):
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
        file_content = "content of the test file"
        self.storage.storage_driver.upload_blob_from_string("test1/file.txt", file_content)
        self.assertEqual(self.storage.storage_driver.get_blob_content_as_string("test1/file.txt"), file_content)

    def test_download_blobs(self):
        files_to_download = list()
        file1_content = "content of the test file1"
        file2_content = "content of the test file2"
        self.storage.storage_driver.upload_blob_from_string("test_download_blobs1/file1.txt", file1_content)
        files_to_download.append("test_download_blobs1/file1.txt")
        self.storage.storage_driver.upload_blob_from_string("test_download_blobs2/file2.txt", file2_content)
        files_to_download.append("test_download_blobs2/file2.txt")
        self.assertEqual(len(os.listdir(self.medusa_bucket_dir)), 2)
        self.storage.storage_driver.download_blobs(files_to_download, self.local_storage_dir)
        self.assertEqual(len(os.listdir(self.local_storage_dir)), 2)

    def test_list_objects(self):
        file1_content = "content of the test file1"
        file2_content = "content of the test file2"
        file3_content = ""
        self.storage.storage_driver.upload_blob_from_string("test_download_blobs1/file1.txt", file1_content)
        self.storage.storage_driver.upload_blob_from_string("test_download_blobs2/file2.txt", file2_content)
        self.storage.storage_driver.upload_blob_from_string("test_download_blobs3/file3.txt", file3_content)
        objects = self.storage.storage_driver.list_objects()
        self.assertEqual(len(objects), 2)
        one_object = self.storage.storage_driver.list_objects("test_download_blobs2")
        self.assertEqual(len(one_object), 1)

    def test_read_blob(self):
        file1_content = "content of the test file1"
        self.storage.storage_driver.upload_blob_from_string("test_download_blobs1/file1.txt", file1_content)
        objects = self.storage.storage_driver.list_objects("test_download_blobs1")
        object_content = self.storage.storage_driver.read_blob_as_string(objects[0])
        self.assertEqual(object_content, file1_content)

    def test_get_blob(self):
        file1_content = "content of the test file1"
        self.storage.storage_driver.upload_blob_from_string("test_download_blobs1/file1.txt", file1_content)
        obj = self.storage.storage_driver.get_blob("test_download_blobs1/file1.txt")
        self.assertEqual(obj.name, "test_download_blobs1/file1.txt")

    def test_read_blob_as_bytes(self):
        file1_content = "content of the test file1"
        self.storage.storage_driver.upload_blob_from_string("test_download_blobs1/file1.txt", file1_content)
        object_content = self.storage.storage_driver.get_blob_content_as_bytes("test_download_blobs1/file1.txt")
        self.assertEqual(object_content, b"content of the test file1")

    def test_verify_hash(self):
        file1_content = "content of the test file1"
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
        file1_content = "content of the test file1"
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


def make_cluster_backup(storage, name, backup_date, nodes, differential=False):
    node_backups = list()
    for node in nodes:
        node_backups.append(make_node_backup(storage, name, backup_date, differential, node))
    return ClusterBackup(name, node_backups)


def make_blob(blob_name, blob_date):
    checksum = hashlib.md5()
    checksum.update(os.urandom(4))
    return AbstractBlob(blob_name, randrange(100), checksum.hexdigest(), datetime.fromtimestamp(blob_date))


if __name__ == '__main__':
    unittest.main()
