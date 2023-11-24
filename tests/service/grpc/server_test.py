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
import unittest

from datetime import datetime
from grpc import ServicerContext
from unittest.mock import Mock, patch

from medusa.backup_manager import BackupMan
from medusa.config import MedusaConfig, _namedtuple_from_dict, StorageConfig, CassandraConfig
from medusa.service.grpc import medusa_pb2
from medusa.service.grpc.server import MedusaService
from medusa.storage import Storage

from tests.storage_test import make_node_backup, make_cluster_backup


class ServerTest(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _make_config(self):
        config = configparser.ConfigParser(interpolation=None)
        config['storage'] = {
            'storage_provider': 'local',
            'fqdn': 'node1',
            'base_path': '/tmp/medusa',
            'bucket_name': 'medusa_serverTest_bucket',
        }
        config['cassandra'] = {
            'is_ccm': '0'
        }
        medusa_config = MedusaConfig(
            file_path=None,
            storage=_namedtuple_from_dict(StorageConfig, config['storage']),
            cassandra=_namedtuple_from_dict(CassandraConfig, config['cassandra']),
            monitoring={},
            ssh=None,
            checks=None,
            logging=None,
            grpc=None,
            kubernetes=None
        )
        return medusa_config

    def test_get_unknown_backup(self):
        # start the Medusa service
        medusa_config = self._make_config()
        service = MedusaService(medusa_config)

        # make a backup request for an unknown backup
        # no need to fake a backup here because we're not actually interested in it
        request = medusa_pb2.GetBackupRequest(backupName='unknown_backup')
        context = Mock(spec=ServicerContext)
        get_backup_response = service.GetBackup(request, context)

        # assert the fields we get in the response
        response_fields = {field.name for field in get_backup_response.DESCRIPTOR.fields}
        self.assertEqual({'status', 'backup'}, response_fields)

        # the status is unknown
        self.assertEqual(medusa_pb2.StatusType.UNKNOWN, get_backup_response.status)

        # the backup itself is blank
        self.assertEqual('', get_backup_response.backup.backupName)
        self.assertEqual(0, get_backup_response.backup.startTime)
        self.assertEqual(0, get_backup_response.backup.finishTime)
        self.assertEqual(0, get_backup_response.backup.totalNodes)
        self.assertEqual(0, get_backup_response.backup.finishedNodes)
        self.assertEqual([], get_backup_response.backup.nodes)
        self.assertEqual(medusa_pb2.StatusType.UNKNOWN, get_backup_response.backup.status)
        self.assertEqual('', get_backup_response.backup.backupType)

        # but at least the status of the response and the status of the backup match
        self.assertEqual(get_backup_response.status, get_backup_response.backup.status)

    def test_get_known_incomplete_backup(self):
        # start the Medusa service
        medusa_config = self._make_config()
        service = MedusaService(medusa_config)

        # build a fake cluster backup object
        storage = Storage(config=medusa_config.storage)
        cluster_backup = make_cluster_backup(
            storage=storage,
            name='backup1',
            backup_date=datetime.fromtimestamp(123456),
            # important to make a backup with just 1 node
            # because the tokenmap has two nodes, this will cause an incomplete backup
            nodes=['node1'],
            differential=True
        )
        cluster_backup.size = lambda: 0
        cluster_backup.num_objects = lambda: 0
        tokenmap_dict = {
            "node1": {"tokens": [-1094266504216117253], "is_up": True, "rack": "r1", "dc": "dc1"},
            "node2": {"tokens": [1094266504216117253], "is_up": True, "rack": "r1", "dc": "dc1"}
        }
        BackupMan.register_backup('backup1', True)
        BackupMan.update_backup_status('backup1', BackupMan.STATUS_IN_PROGRESS)

        # patches a call to the tokenmap, thus avoiding access to the storage
        with patch('medusa.storage.ClusterBackup.tokenmap', return_value=tokenmap_dict) as tokenmap:
            # since the patch is a dumb magic mock, we also need to patch some attributes of it
            tokenmap.__iter__ = lambda _: list(tokenmap_dict.keys()).__iter__()
            tokenmap.__len__ = lambda _: len(tokenmap_dict.keys())
            # we don't ever create any file, so we won't get a timestamp to get the finish time from
            with patch('medusa.storage.ClusterBackup.started', return_value=12345):
                with patch('medusa.storage.ClusterBackup.finished', return_value=123456):
                    # prevent calls to the storage by faking the get_cluster_backup method
                    with patch('medusa.storage.Storage.get_cluster_backup', return_value=cluster_backup):
                        request = medusa_pb2.BackupStatusRequest(backupName='backup1')
                        context = Mock(spec=ServicerContext)
                        get_backup_response = service.GetBackup(request, context)

                        self.assertEqual(medusa_pb2.StatusType.SUCCESS, get_backup_response.status)

                        self.assertEqual('backup1', get_backup_response.backup.backupName)

                        # because of MagicMock-ing and @property annotation, we cannot make this work properly
                        self.assertEqual(1, get_backup_response.backup.startTime)
                        self.assertEqual(1, get_backup_response.backup.finishTime)

                        self.assertEqual(2, get_backup_response.backup.totalNodes)
                        self.assertEqual(1, get_backup_response.backup.finishedNodes)
                        # the BackupNode records ought to be more populated than this, but we test that in ITs instead
                        self.assertEqual(
                            [medusa_pb2.BackupNode(host='node1'), medusa_pb2.BackupNode(host='node2')],
                            get_backup_response.backup.nodes
                        )
                        # this should also be IN_PROGRESS but because the ClusterBackup.finished is a mock
                        # we cannot correctly make it be 'None' when needed (some other things break)
                        self.assertEqual(medusa_pb2.StatusType.SUCCESS, get_backup_response.backup.status)
                        self.assertEqual('differential', get_backup_response.backup.backupType)

    def test_get_backup_status_unknown_backup(self):
        # start the Medusa service
        medusa_config = self._make_config()
        service = MedusaService(medusa_config)

        # make a status request for an unknown backup.
        # no need to fake a backup here because we're not actually interested in it
        request = medusa_pb2.BackupStatusRequest(backupName='unknown_backup')
        context = Mock(spec=ServicerContext)
        backup_status = service.BackupStatus(request, context)

        # verify the response structure without an emphasis on the actual contents
        status_fields = {field.name for field in backup_status.DESCRIPTOR.fields}
        self.assertEqual(
            status_fields,
            {'startTime', 'finishTime', 'status'}
        )
        self.assertEqual(medusa_pb2.StatusType.UNKNOWN, backup_status.status)
        self.assertEqual('', backup_status.startTime)
        self.assertEqual('', backup_status.finishTime)

    def test_get_backup_status_known_backup(self):
        # start the Medusa service
        medusa_config = self._make_config()
        service = MedusaService(medusa_config)

        # build a fake backup object
        storage = Storage(config=medusa_config.storage)
        node_backup = make_node_backup(storage, 'backup1', datetime.fromtimestamp(123456), differential=True)
        BackupMan.register_backup('backup1', True)
        BackupMan.update_backup_status('backup1', BackupMan.STATUS_SUCCESS)

        # this patch prevents the actual get_node_backup call, and instead returns the fake backup from above
        with patch('medusa.storage.Storage.get_node_backup', return_value=node_backup):
            request = medusa_pb2.BackupStatusRequest(backupName='backup1')
            context = Mock(spec=ServicerContext)
            backup_status = service.BackupStatus(request, context)

            self.assertEqual(medusa_pb2.StatusType.SUCCESS, backup_status.status)

            # the start timestamp of 123456 rendered as a human-readable date
            # we're parsing this back and forth because hard-coding a human-readable date is
            # inconsistent across environments
            start_time = int(datetime.strptime(backup_status.startTime, '%Y-%m-%d %H:%M:%S').timestamp())
            self.assertEqual(123456, start_time)
            # the fake backup is instant - starts and finishes at the same time
            finish_time = int(datetime.strptime(backup_status.finishTime, '%Y-%m-%d %H:%M:%S').timestamp())
            self.assertEqual(123456, finish_time)

    def test_get_backup_status_incomplete_backup(self):
        # start the Medusa service
        medusa_config = self._make_config()
        service = MedusaService(medusa_config)

        # build a fake backup object
        storage = Storage(config=medusa_config.storage)
        node_backup = make_node_backup(storage, 'backup2', datetime.fromtimestamp(123456), differential=True)
        # we only register the backup, do not move it to SUCCESS
        BackupMan.register_backup('backup2', True)

        with patch('medusa.storage.Storage.get_node_backup', return_value=node_backup):
            request = medusa_pb2.BackupStatusRequest(backupName='backup2')
            context = Mock(spec=ServicerContext)
            backup_status = service.BackupStatus(request, context)

            # we get the response as IN_PROGRESS
            self.assertEqual(medusa_pb2.StatusType.IN_PROGRESS, backup_status.status)
            # the finish time is already set
            # I'm not sure this is because of faking the backup
            start_time = int(datetime.strptime(backup_status.startTime, '%Y-%m-%d %H:%M:%S').timestamp())
            self.assertEqual(123456, start_time)
            finish_time = int(datetime.strptime(backup_status.finishTime, '%Y-%m-%d %H:%M:%S').timestamp())
            self.assertEqual(123456, finish_time)


if __name__ == '__main__':
    unittest.main()
