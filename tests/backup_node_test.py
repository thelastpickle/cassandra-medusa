# -*- coding: utf-8 -*-
# Copyright 2021- Datastax, Inc. All rights reserved.
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
import os
import pathlib
import unittest
from concurrent.futures.thread import ThreadPoolExecutor
from unittest.mock import MagicMock, patch

from medusa import backup_node, config
from medusa.backup_node import BackupMan, check_already_uploaded
from medusa.config import MedusaConfig, StorageConfig, CassandraConfig, \
    SSHConfig, ChecksConfig, MonitoringConfig, LoggingConfig, GrpcConfig, KubernetesConfig
from medusa.storage.abstract_storage import ManifestObject


class BackupNodeTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def setUp(self):
        self.config = config._build_default_config()
        self.config['grpc'] = {
            'enabled': 'True'
        }
        self.config['cassandra'] = {
            'config_file': MagicMock(),
        }

        self.config['storage'] = {
            'host_file_separator': ',',
            'storage_provider': 'local'
        }
        self.medusa_config = MedusaConfig(
            file_path=os.getcwd(),
            storage=namedtuple_from_dict(StorageConfig, self.config['storage']),
            cassandra=namedtuple_from_dict(CassandraConfig, self.config['cassandra']),
            ssh=namedtuple_from_dict(SSHConfig, self.config['ssh']),
            checks=namedtuple_from_dict(ChecksConfig, self.config['checks']),
            monitoring=namedtuple_from_dict(MonitoringConfig, self.config['monitoring']),
            logging=namedtuple_from_dict(LoggingConfig, self.config['logging']),
            grpc=namedtuple_from_dict(GrpcConfig, self.config['grpc']),
            kubernetes=namedtuple_from_dict(KubernetesConfig, self.config['kubernetes'])
        )

    @patch("medusa.storage.Storage.get_node_backup")
    @patch("medusa.cassandra_utils.Cassandra.__init__")
    @patch("medusa.storage.Storage.__init__")
    @patch("medusa.backup_node.start_backup")
    def test_handle_backup_async(self, mock_start_backup, mock_storage, mock_cassandra, mock_get_node_backup):
        mock_start_backup.return_value = {}
        mock_storage.return_value = None
        mock_cassandra.return_value = None
        mock_node_backup_instance = MagicMock()
        mock_node_backup_instance.exists.return_value = False
        mock_get_node_backup.return_value = mock_node_backup_instance
        test_backup_name = "test-backup"
        with ThreadPoolExecutor(max_workers=1, thread_name_prefix=test_backup_name) as executor:
            BackupMan.register_backup(test_backup_name, is_async=True)
            backup_future = executor.submit(backup_node.handle_backup, config=self.medusa_config,
                                            backup_name_arg=test_backup_name, stagger_time=None,
                                            enable_md5_checks_flag=False, mode="differential")
            mock_future_instance = MagicMock()
            mock_callback = MagicMock()
            mock_future_instance.result.return_value = {"foo": "bar"}
            backup_future.add_done_callback(mock_callback)
            BackupMan.set_backup_future(test_backup_name, mock_future_instance)

        registered_backup_future = BackupMan.get_backup_future(test_backup_name)
        assert registered_backup_future is not None
        result = registered_backup_future.result()
        assert result is not None
        assert result == {"foo": "bar"}

    @patch("medusa.storage")
    @patch("medusa.storage.node_backup.NodeBackup")
    def test_check_already_uploaded(self, mock_storage, mock_node_backup):
        mock_storage.config.key_secret_base64 = None

        def mo(path, size, hash='whatever'):
            return ManifestObject(path, size, hash)

        mock_node_backup.is_differential = True

        # avoid comparing file sizes, always return True
        mock_storage.storage_driver.file_matches_storage.return_value = True

        files_in_storage = {
            'keyspace1': {
                'table1-cfid': {
                    'nb-1-big-Data.db': mo('nb-1-big-Data.db', 250),
                    'nb-1-big-Statistics.db': mo('nb-1-big-Statistics.db', 120)
                },
                'table1-cfid..test_idx': {
                    'nb-1-big-Statistics.db': mo('nb-1-big-Statistics.db', 70),
                    'nb-1-big-CompressionInfo.db': mo('nb-1-big-CompressionInfo.db', 42)
                }
            },
            'keyspace2': {
                'table2-cfid': {
                    'nb-1-big-Data.db': mo('nb-1-big-Data.db', 252),
                },

            }
        }

        table1_srcs = [
            pathlib.Path('/foo/bar/data/keyspace1/table1-cfid/snapshots/snapshot-name/nb-1-big-Data.db'),
            pathlib.Path('/foo/bar/data/keyspace1/table1-cfid/snapshots/snapshot-name/nb-1-big-Statistics.db'),
        ]
        table1_backup, table1_reupload, table1_already_up = check_already_uploaded(
            mock_storage,
            mock_node_backup,
            multipart_threshold=100,
            enable_md5_checks=False,
            files_in_storage=files_in_storage,
            keyspace='keyspace1',
            srcs=table1_srcs
        )

        self.assertEqual(list(), table1_backup)
        self.assertEqual(list(), table1_reupload)
        self.assertEqual(
            [mo('nb-1-big-Data.db', 250), mo('nb-1-big-Statistics.db', 120)],
            table1_already_up
        )

        table1_index_srcs = [
            pathlib.Path('/foo/bar/data/keyspace1/table1-cfid/snapshots/snapshot-name/.test_idx/nb-1-big-Data.db'),
            pathlib.Path('/data/keyspace1/table1-cfid/snapshots/snapshot-name/.test_idx/nb-1-big-Statistics.db'),
            pathlib.Path('/data/keyspace1/table1-cfid/snapshots/snapshot-name/.test_idx/nb-1-big-CompressionInfo.db'),
        ]
        t1_idx_backup, t1_idx_reupload, t1_idx_already_up = check_already_uploaded(
            mock_storage,
            mock_node_backup,
            multipart_threshold=100,
            enable_md5_checks=False,
            files_in_storage=files_in_storage,
            keyspace='keyspace1',
            srcs=table1_index_srcs
        )
        # the first file was not in storage, so we need to reuplaod it
        self.assertEqual([table1_index_srcs[0]], t1_idx_backup)
        self.assertEqual([], t1_idx_reupload)
        # the other two files were already backed up
        self.assertEqual([mo('nb-1-big-Statistics.db', 70), mo('nb-1-big-CompressionInfo.db', 42)], t1_idx_already_up)

        # make every file not match storage
        mock_storage.storage_driver.file_matches_storage.return_value = False
        table2_srcs = [
            pathlib.Path('/foo/bar/data/keyspace2/table2-cfid/snapshots/snapshot-name/nb-1-big-Data.db'),
        ]
        t2_backup, t2_reupload, t2_already_up = check_already_uploaded(
            mock_storage,
            mock_node_backup,
            multipart_threshold=100,
            enable_md5_checks=False,
            files_in_storage=files_in_storage,
            keyspace='keyspace2',
            srcs=table2_srcs,
        )
        self.assertEqual([], t2_backup)
        # the file shows up in the reupload list because it's in the storage, but not at a different shape
        self.assertEqual([table2_srcs[0]], t2_reupload)
        self.assertEqual([], t2_already_up)


if __name__ == '__main__':
    unittest.main()


def namedtuple_from_dict(cls, data):
    return cls(**{
        field: data.get(field)
        for field in cls._fields
    })
