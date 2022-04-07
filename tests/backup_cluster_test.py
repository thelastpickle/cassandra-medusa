# -*- coding: utf-8 -*-
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
import unittest
from builtins import staticmethod
from enum import IntEnum
from unittest.mock import create_autospec

from medusa.cassandra_utils import Cassandra, CqlSessionProvider
from medusa.config import (KubernetesConfig, _namedtuple_from_dict,
                           MedusaConfig, CassandraConfig, SSHConfig, StorageConfig)
from medusa.monitoring import Monitoring
from medusa.orchestration import Orchestration
from medusa.storage import Storage
from medusa.backup_cluster import BackupJob, orchestrate


class ExitCode(IntEnum):
    SUCCESS = 0
    ERROR = 1


class BackupClusterTest(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def setUp(self):
        self.config = self._build_config_parser()
        self.medusa_config = self._build_medusa_config(self.config)
        self.mock_orchestration = create_autospec(Orchestration)
        self.mock_cassandra_config = create_autospec(Cassandra)
        self.mock_cql_session_provider = create_autospec(CqlSessionProvider)
        self.mock_monitoring = create_autospec(Monitoring)
        self.mock_storage = create_autospec(Storage)

    @staticmethod
    def _build_config_parser(cassandra_config=None, ssh_config=None, storage_config=None):
        """Build and return a mutable config"""

        config = configparser.ConfigParser(interpolation=None)
        config['cassandra'] = {
            'use_sudo': 'True',
            'resolve_ip_addresses': 'False'
        } if cassandra_config is None else cassandra_config
        config['ssh'] = {
            'username': '',
            'key_file': '',
            'port': '22',
            'cert_file': ''
        } if ssh_config is None else ssh_config
        config['storage'] = {
            'fqdn': '127.0.0.1',
            'storage_provider': ''
        } if storage_config is None else storage_config
        config['kubernetes'] = {
            'enabled': 'False',
        }
        return config

    @staticmethod
    def _build_medusa_config(config):
        return MedusaConfig(
            file_path=None,
            storage=_namedtuple_from_dict(StorageConfig, config['storage']),
            monitoring={},
            cassandra=_namedtuple_from_dict(CassandraConfig, config['cassandra']),
            ssh=_namedtuple_from_dict(SSHConfig, config['ssh']),
            checks=None,
            logging=None,
            grpc=None,
            kubernetes=_namedtuple_from_dict(KubernetesConfig, config['kubernetes']),
        )

    def test_backup_object_creation(self):
        self.mock_orchestration.pssh_run.return_value = True
        medusa_conf = self._build_medusa_config(self.config)
        backup_job = BackupJob(medusa_conf, "backup1", "127.0.0.1", None, True, "differential", pathlib.Path("/tmp"),
                               1, 2, self.mock_orchestration, self.mock_orchestration, self.mock_cassandra_config)
        backup_job.execute(self.mock_cql_session_provider)
        assert self.mock_orchestration.pssh_run.call_count == 2

    def test_backup_orchestration(self):
        self.mock_orchestration.pssh_run.return_value = True
        self.mock_storage.get_cluster_backup.return_value = None
        medusa_conf = self._build_medusa_config(self.config)
        orchestrate(medusa_conf, "backup2", "127.0.0.1", None, True, "differential", pathlib.Path("/tmp"),
                    1, 2, self.mock_orchestration, self.mock_orchestration, self.mock_cassandra_config,
                    self.mock_monitoring, self.mock_storage, self.mock_cql_session_provider)
        assert self.mock_orchestration.pssh_run.call_count == 2

    def test_fail_backup_orchestration(self):
        self.mock_orchestration.pssh_run.return_value = True
        self.mock_storage.get_cluster_backup.return_value = None
        medusa_conf = self._build_medusa_config(self.config)
        with self.assertRaises(SystemExit):
            orchestrate(medusa_conf, "backup2", "127.0.0.1", None, True, "differential", pathlib.Path("/tmp"),
                        1, 2, self.mock_orchestration, self.mock_orchestration, self.mock_cassandra_config,
                        self.mock_monitoring, self.mock_storage)

    def test_backup_orchestration_already_exists(self):
        self.mock_orchestration.pssh_run.return_value = True
        medusa_conf = self._build_medusa_config(self.config)
        with self.assertRaises(SystemExit):
            orchestrate(medusa_conf, "backup2", "127.0.0.1", None, True, "differential", pathlib.Path("/tmp"),
                        1, 2, self.mock_orchestration, self.mock_orchestration, self.mock_cassandra_config,
                        self.mock_monitoring, self.mock_storage)

    def test_backup_orchestration_pssh_snapshot_failure(self):
        self.mock_orchestration.pssh_run.return_value = False
        self.mock_storage.get_cluster_backup.return_value = None
        medusa_conf = self._build_medusa_config(self.config)
        with self.assertRaises(SystemExit):
            orchestrate(medusa_conf, "backup2", "127.0.0.1", None, True, "differential", pathlib.Path("/tmp"),
                        1, 2, self.mock_orchestration, self.mock_orchestration, self.mock_cassandra_config,
                        self.mock_monitoring, self.mock_storage)

    def test_backup_orchestration_pssh_backup_failure(self):
        mock_backup = create_autospec(Orchestration)
        mock_backup.pssh_run.return_value = False
        self.mock_orchestration.pssh_run.return_value = True
        self.mock_storage.get_cluster_backup.return_value = None
        medusa_conf = self._build_medusa_config(self.config)
        with self.assertRaises(SystemExit):
            orchestrate(medusa_conf, "backup2", "127.0.0.1", None, True, "differential", pathlib.Path("/tmp"),
                        1, 2, self.mock_orchestration, self.mock_orchestration, mock_backup,
                        self.mock_monitoring, self.mock_storage)


if __name__ == '__main__':
    unittest.main()
