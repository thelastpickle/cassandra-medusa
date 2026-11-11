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
import asyncio
import concurrent
import configparser
import os
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

import grpc
from grpc import ServicerContext

from medusa.backup_manager import BackupMan
from medusa.config import MedusaConfig, _namedtuple_from_dict, StorageConfig, CassandraConfig, PitrConfig
from medusa.service.grpc import medusa_pb2
from medusa.service.grpc.commitlog_archiver import CommitLogArchiver
from medusa.service.grpc.server import MedusaService, Server
from medusa.storage import Storage

from tests.storage_test import make_node_backup, make_cluster_backup, make_unfinished_node_backup


class ServerTest(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def setUp(self):
        self._fqdn_patcher = patch('socket.getfqdn', return_value='node1')
        self._fqdn_patcher.start()
        self.addCleanup(self._fqdn_patcher.stop)

    def _make_config(self, pitr_enabled='false'):
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
        config['pitr'] = {
            'enabled': pitr_enabled,
            'commitlog_archive_interval_seconds': '60',
            'commitlog_spool_dir': '/tmp/spool',
            'commitlog_staging_dir': '/tmp/staging',
            'commitlog_restore_grace_period_seconds': '3600',
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
            kubernetes=None,
            pitr=_namedtuple_from_dict(PitrConfig, config['pitr']),
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

    def test_get_backups_failure_sets_overall_status(self):
        medusa_config = self._make_config()
        service = MedusaService(medusa_config)

        request = medusa_pb2.GetBackupsRequest()
        context = Mock(spec=ServicerContext)

        with patch('medusa.service.grpc.server.get_backups', side_effect=Exception('storage failure')):
            get_backups_response = service.GetBackups(request, context)

        response_fields = {field.name for field in get_backups_response.DESCRIPTOR.fields}
        self.assertEqual({'backups', 'overallStatus'}, response_fields)
        self.assertEqual([], get_backups_response.backups)
        self.assertEqual(medusa_pb2.StatusType.UNKNOWN, get_backups_response.overallStatus)

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

    def test_get_backup_status_unknown_backup_with_future(self):
        # Backup isn't started yet and we have a future for it. We consider it in UNKNOWN state.
        # start the Medusa service
        medusa_config = self._make_config()
        service = MedusaService(medusa_config)
        BackupMan.register_backup('unknown_backup1', True)
        BackupMan.set_backup_future('unknown_backup1', 'fake_future')

        # make a status request for an unknown backup.
        # no need to fake a backup here because we're not actually interested in it
        request = medusa_pb2.BackupStatusRequest(backupName='unknown_backup1')
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

    def test_get_backup_status_unknown_backup_no_future(self):
        # Backup isn't started yet and we don't have a future for it. We consider it in UNKNOWN state.
        # start the Medusa service
        medusa_config = self._make_config()
        service = MedusaService(medusa_config)
        BackupMan.register_backup('unknown_backup2', True)

        # make a status request for an unknown backup.
        # no need to fake a backup here because we're not actually interested in it
        request = medusa_pb2.BackupStatusRequest(backupName='unknown_backup2')
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

    def test_get_backup_status_finished_backup_no_future(self):
        # Backup that has already finished and has no future. We consider it successful.
        # start the Medusa service
        medusa_config = self._make_config()
        service = MedusaService(medusa_config)

        # build a fake backup object
        storage = Storage(config=medusa_config.storage)
        node_backup = make_node_backup(storage, 'backup1', datetime.fromtimestamp(123456), differential=True)
        BackupMan.register_backup('backup1', True)

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

    def test_get_backup_status_finished_backup_with_future(self):
        # Backup is finished and has a future. We consider it successful.
        # start the Medusa service
        medusa_config = self._make_config()
        service = MedusaService(medusa_config)

        # build a fake backup object
        storage = Storage(config=medusa_config.storage)
        node_backup = make_node_backup(storage, 'backup3', datetime.fromtimestamp(123456), differential=True)
        BackupMan.register_backup('backup1', True)
        mock_future = Mock(spec=concurrent.futures.Future)
        BackupMan.set_backup_future('backup1', mock_future)

        # this patch prevents the actual get_node_backup call, and instead returns the fake backup from above
        with patch('medusa.storage.Storage.get_node_backup', return_value=node_backup):
            request = medusa_pb2.BackupStatusRequest(backupName='backup3')
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

    def test_get_backup_status_started_backup_with_done_future(self):
        # Backup is started and has a future which is done. We consider it failed.
        # start the Medusa service
        medusa_config = self._make_config()
        service = MedusaService(medusa_config)

        # build a fake backup object
        storage = Storage(config=medusa_config.storage)
        node_backup = make_unfinished_node_backup(storage, 'backup4', datetime.fromtimestamp(123456), differential=True)
        # we only register the backup, do not move it to SUCCESS
        BackupMan.register_backup('backup4', True)
        mock_future = Mock(spec=concurrent.futures.Future)
        mock_future.done.return_value = True
        BackupMan.set_backup_future('backup4', mock_future)

        with patch('medusa.storage.Storage.get_node_backup', return_value=node_backup):
            request = medusa_pb2.BackupStatusRequest(backupName='backup4')
            context = Mock(spec=ServicerContext)
            backup_status = service.BackupStatus(request, context)

            # we get the response as SUCCESS because the finish time is set (~not None)
            self.assertEqual(medusa_pb2.StatusType.FAILED, backup_status.status)
            # the finish time is already set
            # I'm not sure this is because of faking the backup
            start_time = int(datetime.strptime(backup_status.startTime, '%Y-%m-%d %H:%M:%S').timestamp())
            self.assertEqual(123456, start_time)

    def test_get_backup_status_started_backup_with_undone_future(self):
        # Backup is started and has a future. We consider it in progress.
        # start the Medusa service
        medusa_config = self._make_config()
        service = MedusaService(medusa_config)

        # build a fake backup object
        storage = Storage(config=medusa_config.storage)
        node_backup = make_unfinished_node_backup(storage, 'backup4', datetime.fromtimestamp(123456), differential=True)
        # we only register the backup, do not move it to SUCCESS
        BackupMan.register_backup('backup4', True)
        mock_future = Mock(spec=concurrent.futures.Future)
        mock_future.done.return_value = False
        BackupMan.set_backup_future('backup4', mock_future)

        with patch('medusa.storage.Storage.get_node_backup', return_value=node_backup):
            request = medusa_pb2.BackupStatusRequest(backupName='backup4')
            context = Mock(spec=ServicerContext)
            backup_status = service.BackupStatus(request, context)

            # we get the response as SUCCESS because the finish time is set (~not None)
            self.assertEqual(medusa_pb2.StatusType.IN_PROGRESS, backup_status.status)
            # the finish time is already set
            # I'm not sure this is because of faking the backup
            start_time = int(datetime.strptime(backup_status.startTime, '%Y-%m-%d %H:%M:%S').timestamp())
            self.assertEqual(123456, start_time)

    def test_get_backup_status_started_backup_no_future(self):
        # Backup is started and has no future. We consider it failed.
        # start the Medusa service
        medusa_config = self._make_config()
        service = MedusaService(medusa_config)

        # build a fake backup object
        storage = Storage(config=medusa_config.storage)
        node_backup = make_unfinished_node_backup(storage, 'backup5', datetime.fromtimestamp(123456), differential=True)
        # we only register the backup, do not move it to SUCCESS
        BackupMan.register_backup('backup5', True)
        self.assertIsNone(BackupMan.get_backup_future('backup5'))

        with patch('medusa.storage.Storage.get_node_backup', return_value=node_backup):
            request = medusa_pb2.BackupStatusRequest(backupName='backup5')
            context = Mock(spec=ServicerContext)
            backup_status = service.BackupStatus(request, context)

            # we get the response as SUCCESS because the finish time is set (~not None)
            self.assertEqual(medusa_pb2.StatusType.FAILED, backup_status.status)
            # the finish time is already set
            # I'm not sure this is because of faking the backup
            start_time = int(datetime.strptime(backup_status.startTime, '%Y-%m-%d %H:%M:%S').timestamp())
            self.assertEqual(123456, start_time)

    def test_get_backup_status_started_backup_failed_future(self):
        # Backup is started and has no future. We consider it failed.
        # start the Medusa service
        medusa_config = self._make_config()
        service = MedusaService(medusa_config)

        # build a fake backup object
        storage = Storage(config=medusa_config.storage)
        node_backup = make_unfinished_node_backup(storage, 'backup6', datetime.fromtimestamp(123456), differential=True)
        # we only register the backup, do not move it to SUCCESS
        BackupMan.register_backup('backup6', True)
        self.assertIsNone(BackupMan.get_backup_future('backup6'))

        with patch('medusa.storage.Storage.get_node_backup', return_value=node_backup):
            request = medusa_pb2.BackupStatusRequest(backupName='backup6')
            context = Mock(spec=ServicerContext)
            mock_future = Mock(spec=concurrent.futures.Future)
            mock_future.done.return_value = True
            mock_future.result.side_effect = Exception('fake exception')
            BackupMan.set_backup_future('backup6', mock_future)
            backup_status = service.BackupStatus(request, context)

            # we get the response as SUCCESS because the finish time is set (~not None)
            self.assertEqual(medusa_pb2.StatusType.FAILED, backup_status.status)
            # the finish time is already set
            # I'm not sure this is because of faking the backup
            start_time = int(datetime.strptime(backup_status.startTime, '%Y-%m-%d %H:%M:%S').timestamp())
            self.assertEqual(123456, start_time)

    def test_get_commitlog_archive_status_no_archiver(self):
        medusa_config = self._make_config()
        service = MedusaService(medusa_config)

        request = medusa_pb2.GetCommitLogArchiveStatusRequest()
        context = Mock(spec=ServicerContext)
        response = service.GetCommitLogArchiveStatus(request, context)

        self.assertFalse(response.running)
        self.assertEqual(0, response.pendingCount)
        self.assertEqual(0, response.lastUploadTime)
        self.assertEqual(0, response.intervalSeconds)
        self.assertEqual('', response.lastError)
        self.assertEqual(medusa_pb2.StatusType.UNKNOWN, response.status)

    def test_get_commitlog_archive_status_with_files_in_spool(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            spool = root / 'spool'
            commitlog = root / 'commitlog'
            storage_dir = root / 'storage'
            for d in (spool, commitlog, storage_dir):
                d.mkdir()
            cassandra_yaml = root / 'cassandra.yaml'
            cassandra_yaml.write_text(f'commitlog_directory: {commitlog}\n')

            # Create 3 dummy files in spool
            for name in ('CommitLog-6-1.log', 'CommitLog-6-2.log', 'CommitLog-6-3.log'):
                (spool / name).write_bytes(b'dummy')

            config = MedusaConfig(
                file_path=None,
                storage=_namedtuple_from_dict(StorageConfig, {
                    'storage_provider': 'local',
                    'fqdn': 'node1',
                    'base_path': str(storage_dir),
                    'bucket_name': 'bucket',
                }),
                cassandra=_namedtuple_from_dict(CassandraConfig, {
                    'is_ccm': '0',
                    'config_file': str(cassandra_yaml),
                }),
                monitoring={},
                ssh=None,
                checks=None,
                logging=None,
                grpc=None,
                kubernetes=None,
                pitr=_namedtuple_from_dict(PitrConfig, {
                    'enabled': 'true',
                    'commitlog_archive_interval_seconds': '10',
                    'commitlog_spool_dir': str(spool),
                    'commitlog_staging_dir': str(root / 'staging'),
                    'commitlog_restore_grace_period_seconds': '3600',
                }),
            )
            archiver = CommitLogArchiver(config)
            service = MedusaService(config, commitlog_archiver=archiver)

            request = medusa_pb2.GetCommitLogArchiveStatusRequest()
            context = Mock(spec=ServicerContext)
            response = service.GetCommitLogArchiveStatus(request, context)

            self.assertEqual(3, response.pendingCount)
            self.assertEqual(10, response.intervalSeconds)
            self.assertFalse(response.running)

    def test_get_commitlog_archive_status_maps_fields(self):
        medusa_config = self._make_config()
        mock_archiver = Mock()
        mock_archiver.status.return_value = {
            'running': True,
            'last_upload_time': 1600000000,
            'pending_count': 7,
            'interval_seconds': 42,
            'status': medusa_pb2.StatusType.IN_PROGRESS,
            'last_error': 'something went wrong',
        }
        service = MedusaService(medusa_config, commitlog_archiver=mock_archiver)

        request = medusa_pb2.GetCommitLogArchiveStatusRequest()
        context = Mock(spec=ServicerContext)
        response = service.GetCommitLogArchiveStatus(request, context)

        self.assertTrue(response.running)
        self.assertEqual(1600000000, response.lastUploadTime)
        self.assertEqual(7, response.pendingCount)
        self.assertEqual(42, response.intervalSeconds)
        self.assertEqual(medusa_pb2.StatusType.IN_PROGRESS, response.status)
        self.assertEqual('something went wrong', response.lastError)

    def test_get_commitlog_archive_status_archiver_raises_internal(self):
        medusa_config = self._make_config()
        mock_archiver = Mock()
        mock_archiver.status.side_effect = Exception('unexpected disk failure')
        service = MedusaService(medusa_config, commitlog_archiver=mock_archiver)

        request = medusa_pb2.GetCommitLogArchiveStatusRequest()
        context = Mock(spec=ServicerContext)
        response = service.GetCommitLogArchiveStatus(request, context)

        self.assertIsNotNone(response)
        context.set_code.assert_called_once_with(grpc.StatusCode.INTERNAL)
        context.set_details.assert_called_once()
        self.assertIn('unexpected disk failure', context.set_details.call_args[0][0])

    def _write_temp_ini(self, pitr_enabled='false'):
        f = tempfile.NamedTemporaryFile('w', delete=False)
        f.write(
            "[storage]\n"
            "storage_provider = local\n"
            "fqdn = 127.0.0.1\n"
            "base_path = /tmp/medusa\n"
            "bucket_name = bucket\n"
            "[cassandra]\n"
            "is_ccm = 0\n"
            "resolve_ip_addresses = False\n"
            "[pitr]\n"
            f"enabled = {pitr_enabled}\n"
            "commitlog_archive_interval_seconds = 10\n"
            "commitlog_spool_dir = /tmp/spool\n"
        )
        f.close()
        self.addCleanup(os.unlink, f.name)
        return f.name

    def test_serve_pitr_enabled_creates_and_starts_archiver(self):
        ini_path = self._write_temp_ini(pitr_enabled='true')
        with patch('medusa.service.grpc.server.aio.server') as mock_aio_server:
            mock_server = Mock()
            mock_server.start = AsyncMock()
            mock_aio_server.return_value = mock_server
            server = Server(ini_path, testing=True)

            with patch('medusa.service.grpc.server.CommitLogArchiver') as mock_archiver_cls:
                asyncio.run(server.serve())
                mock_archiver_cls.assert_called_once()
                mock_archiver_cls.return_value.start.assert_called_once()
                self.assertEqual(server.commitlog_archiver, mock_archiver_cls.return_value)

    def test_serve_pitr_enabled_constructor_raises_runtime_error_propagates(self):
        ini_path = self._write_temp_ini(pitr_enabled='true')
        with patch('medusa.service.grpc.server.aio.server') as mock_aio_server:
            mock_server = Mock()
            mock_server.start = AsyncMock()
            mock_aio_server.return_value = mock_server
            server = Server(ini_path, testing=True)

            with patch('medusa.service.grpc.server.CommitLogArchiver', side_effect=RuntimeError("Device mismatch")), \
                    self.assertRaises(RuntimeError) as ctx:
                asyncio.run(server.serve())
            self.assertIn("Device mismatch", str(ctx.exception))

    def test_serve_pitr_disabled_no_archiver(self):
        ini_path = self._write_temp_ini(pitr_enabled='false')
        with patch('medusa.service.grpc.server.aio.server') as mock_aio_server:
            mock_server = Mock()
            mock_server.start = AsyncMock()
            mock_aio_server.return_value = mock_server
            server = Server(ini_path, testing=True)

            with patch('medusa.service.grpc.server.CommitLogArchiver') as mock_archiver_cls:
                asyncio.run(server.serve())
                mock_archiver_cls.assert_not_called()
                self.assertIsNone(server.commitlog_archiver)

    def test_serve_stops_archiver_when_server_terminates(self):
        ini_path = self._write_temp_ini(pitr_enabled='true')
        with patch('medusa.service.grpc.server.aio.server') as mock_aio_server:
            mock_server = Mock()
            mock_server.start = AsyncMock()
            mock_server.wait_for_termination = AsyncMock()
            mock_aio_server.return_value = mock_server
            server = Server(ini_path, testing=False)

            with patch('medusa.service.grpc.server.CommitLogArchiver') as mock_archiver_cls, \
                    patch('medusa.service.grpc.server.handle_backup_removal_all'):
                asyncio.run(server.serve())
            mock_archiver_cls.return_value.stop.assert_called_once()

    def test_shutdown_calls_stop_on_archiver(self):
        ini_path = self._write_temp_ini()
        with patch('medusa.service.grpc.server.aio.server') as mock_aio_server:
            mock_server = Mock()
            mock_aio_server.return_value = mock_server
            server = Server(ini_path, testing=True)
            mock_archiver = Mock()
            server.commitlog_archiver = mock_archiver

            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                mock_server.stop.return_value = loop.create_future()
                mock_server.stop.return_value.set_result(None)
                with patch('medusa.service.grpc.server.handle_backup_removal_all'):
                    server.shutdown()
                mock_archiver.stop.assert_called_once()
            finally:
                loop.close()

    def test_shutdown_no_archiver_does_not_fail(self):
        ini_path = self._write_temp_ini()
        with patch('medusa.service.grpc.server.aio.server') as mock_aio_server:
            mock_server = Mock()
            mock_aio_server.return_value = mock_server
            server = Server(ini_path, testing=True)
            server.commitlog_archiver = None

            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                mock_server.stop.return_value = loop.create_future()
                mock_server.stop.return_value.set_result(None)
                with patch('medusa.service.grpc.server.handle_backup_removal_all'):
                    server.shutdown()
            finally:
                loop.close()


if __name__ == '__main__':
    unittest.main()
