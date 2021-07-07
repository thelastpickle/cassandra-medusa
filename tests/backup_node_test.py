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
import unittest
from concurrent.futures.thread import ThreadPoolExecutor
from unittest.mock import MagicMock, patch

from medusa import backup_node, config
from medusa.backup_node import BackupMan
from medusa.config import MedusaConfig, StorageConfig, CassandraConfig, \
    SSHConfig, ChecksConfig, MonitoringConfig, LoggingConfig, GrpcConfig, KubernetesConfig


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


if __name__ == '__main__':
    unittest.main()


def namedtuple_from_dict(cls, data):
    return cls(**{
        field: data.get(field)
        for field in cls._fields
    })
