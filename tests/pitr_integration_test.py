# -*- coding: utf-8 -*-
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
import configparser
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import grpc

from medusa.config import is_pitr_enabled
from medusa.service.grpc import medusa_pb2
from medusa.service.grpc.client import Client
from medusa.service.grpc.restore import get_pitr_mapping
from tests.integration import pitr_helpers


class FakeClock:
    def __init__(self):
        self.now = 0.0
        self.sleeps = []

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


def drained_status(running=True, pending=0, last_upload=1000):
    return medusa_pb2.GetCommitLogArchiveStatusResponse(
        running=running, pendingCount=pending, lastUploadTime=last_upload)


class PitrHelpersTest(unittest.TestCase):

    def test_archive_command_hardlinks_into_the_spool(self):
        self.assertEqual('/bin/ln %path /tmp/spool/%name', pitr_helpers.archive_command('/tmp/spool'))

    def test_archiving_properties_only_archive(self):
        properties = pitr_helpers.archiving_properties(Path('/tmp/spool'))
        self.assertEqual('archive_command=/bin/ln %path /tmp/spool/%name\n', properties)

    def test_format_target_timestamp_truncates_to_milliseconds(self):
        self.assertEqual('1767225600.123', pitr_helpers.format_target_timestamp(1767225600.1239))
        self.assertEqual('1767225600.000', pitr_helpers.format_target_timestamp(1767225600))

    def test_format_target_timestamp_is_not_later_than_the_instant(self):
        instant = 1767225600.9999
        self.assertLessEqual(float(pitr_helpers.format_target_timestamp(instant)), instant)

    def test_format_target_timestamp_rejects_non_finite(self):
        for value in (float('nan'), float('inf')):
            with self.assertRaises(ValueError):
                pitr_helpers.format_target_timestamp(value)

    def test_enable_pitr_in_ini_creates_the_section(self):
        parser = configparser.ConfigParser(interpolation=None)
        pitr_helpers.enable_pitr_in_ini(parser, '/tmp/spool', Path('/tmp/staging'), archive_interval_s=3)
        self.assertEqual('true', parser['pitr']['enabled'])
        self.assertEqual('3', parser['pitr']['commitlog_archive_interval_seconds'])
        self.assertEqual('/tmp/spool', parser['pitr']['commitlog_spool_dir'])
        self.assertEqual('/tmp/staging', parser['pitr']['commitlog_staging_dir'])

    def test_enable_pitr_in_ini_keeps_other_settings(self):
        parser = configparser.ConfigParser(interpolation=None)
        parser.read_dict({'pitr': {'commitlog_restore_grace_period_seconds': '5'}, 'storage': {'fqdn': 'x'}})
        pitr_helpers.enable_pitr_in_ini(parser, '/s', '/t')
        self.assertEqual('5', parser['pitr']['commitlog_restore_grace_period_seconds'])
        self.assertEqual('x', parser['storage']['fqdn'])

    def test_enabled_ini_turns_pitr_on(self):
        parser = configparser.ConfigParser(interpolation=None)
        pitr_helpers.enable_pitr_in_ini(parser, '/s', '/t')
        config = mock.Mock(pitr=mock.Mock(enabled=parser['pitr']['enabled']))
        self.assertTrue(is_pitr_enabled(config))

    def test_enable_kubernetes_in_ini(self):
        parser = configparser.ConfigParser(interpolation=None)
        pitr_helpers.enable_kubernetes_in_ini(parser)
        self.assertEqual('True', parser['kubernetes']['enabled'])
        parser.set('kubernetes', 'enabled', 'False')
        pitr_helpers.enable_kubernetes_in_ini(parser)
        self.assertEqual('True', parser['kubernetes']['enabled'])

    def test_build_restore_mapping(self):
        mapping = pitr_helpers.build_restore_mapping({'127.0.0.1': 'pitr_base'}, '1767225600.000', pod_name='pod-0')
        self.assertEqual(
            {
                'in_place': True,
                'host_map': {'pod-0': {'source': ['127.0.0.1'], 'seed': False}},
                'pitr': {'target_timestamp': '1767225600.000', 'node_backups': {'127.0.0.1': 'pitr_base'}},
            },
            mapping
        )

    def test_build_restore_mapping_is_understood_by_the_restore_entrypoints(self):
        mapping = pitr_helpers.build_restore_mapping({'127.0.0.1': 'b'}, '1')
        self.assertEqual({'target_timestamp': '1', 'node_backups': {'127.0.0.1': 'b'}}, get_pitr_mapping(mapping))

    def test_build_restore_mapping_needs_exactly_one_node(self):
        for backups in ({}, {'a': '1', 'b': '2'}):
            with self.assertRaises(ValueError):
                pitr_helpers.build_restore_mapping(backups, '1')

    def test_restore_environment(self):
        base = {'PATH': '/bin', 'POD_IP': '10.0.0.1', 'PYTHONPATH': '../..'}
        env = pitr_helpers.restore_environment(base, '{"a": 1}', 'key-1', pod_name='pod-0', repo_root='/repo')
        self.assertNotIn('POD_IP', env)
        self.assertEqual('{"a": 1}', env['RESTORE_MAPPING'])
        self.assertEqual('key-1', env['RESTORE_KEY'])
        self.assertEqual('pod-0', env['POD_NAME'])
        self.assertEqual('/repo:../..', env['PYTHONPATH'])
        self.assertEqual('/bin', env['PATH'])
        self.assertIn('POD_IP', base)

    def test_restore_environment_without_pythonpath_is_absolute(self):
        env = pitr_helpers.restore_environment({}, '{}', 'k')
        self.assertTrue(Path(env['PYTHONPATH']).is_absolute())
        self.assertTrue((Path(env['PYTHONPATH']) / 'medusa').is_dir())

    def test_archiver_is_drained(self):
        self.assertTrue(pitr_helpers.archiver_is_drained(drained_status()))
        self.assertTrue(pitr_helpers.archiver_is_drained(drained_status(last_upload=10), min_upload_time=10))

    def test_archiver_is_not_drained(self):
        self.assertFalse(pitr_helpers.archiver_is_drained(None))
        self.assertFalse(pitr_helpers.archiver_is_drained(drained_status(running=False)))
        self.assertFalse(pitr_helpers.archiver_is_drained(drained_status(pending=1)))
        self.assertFalse(pitr_helpers.archiver_is_drained(drained_status(last_upload=0)))
        self.assertFalse(pitr_helpers.archiver_is_drained(drained_status(last_upload=9), min_upload_time=10))

    def test_wait_for_archiver_drain_returns_when_drained(self):
        clock = FakeClock()
        statuses = iter([drained_status(pending=2), drained_status(pending=1), drained_status(), drained_status()])
        status = pitr_helpers.wait_for_archiver_drain(lambda: next(statuses), clock=clock, sleep=clock.sleep)
        self.assertEqual(0, status.pendingCount)
        self.assertEqual([2, 2, 2], clock.sleeps)

    def test_wait_for_archiver_drain_needs_consecutive_polls(self):
        clock = FakeClock()
        statuses = iter([drained_status(), drained_status(pending=1), drained_status(), drained_status()])
        calls = []

        def get_status():
            calls.append(1)
            return next(statuses)

        pitr_helpers.wait_for_archiver_drain(get_status, clock=clock, sleep=clock.sleep)
        self.assertEqual(4, len(calls))

    def test_wait_for_archiver_drain_times_out(self):
        clock = FakeClock()
        with self.assertRaises(TimeoutError):
            pitr_helpers.wait_for_archiver_drain(
                lambda: drained_status(pending=3), timeout_s=10, interval_s=2, clock=clock, sleep=clock.sleep)
        self.assertEqual(10, clock.now)

    def test_wait_for_archiver_drain_times_out_without_status(self):
        clock = FakeClock()
        with self.assertRaises(TimeoutError):
            pitr_helpers.wait_for_archiver_drain(lambda: None, timeout_s=4, clock=clock, sleep=clock.sleep)

    def test_wait_for_archiver_drain_ignores_uploads_before_min_upload_time(self):
        clock = FakeClock()
        with self.assertRaises(TimeoutError):
            pitr_helpers.wait_for_archiver_drain(
                lambda: drained_status(last_upload=5), min_upload_time=6, timeout_s=4,
                clock=clock, sleep=clock.sleep)

    def test_remove_paths(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp) / 'dir'
            (directory / 'sub').mkdir(parents=True)
            (directory / 'sub' / 'f').write_text('x')
            a_file = Path(tmp) / 'file'
            a_file.write_text('x')
            pitr_helpers.remove_paths([directory, a_file, Path(tmp) / 'missing'])
            self.assertFalse(directory.exists())
            self.assertFalse(a_file.exists())


def _rpc_error():
    class FakeRpcError(grpc.RpcError):
        pass

    return FakeRpcError('boom')


class PitrClientTest(unittest.TestCase):

    def _client(self):
        client = Client.__new__(Client)
        client.channel = mock.Mock()
        return client

    def test_get_commitlog_archive_status(self):
        expected = drained_status()
        stub = mock.Mock()
        stub.GetCommitLogArchiveStatus = mock.AsyncMock(return_value=expected)
        with mock.patch('medusa.service.grpc.client.medusa_pb2_grpc.MedusaStub', return_value=stub):
            result = asyncio.run(self._client().get_commitlog_archive_status())
        self.assertIs(expected, result)
        request = stub.GetCommitLogArchiveStatus.call_args[0][0]
        self.assertIsInstance(request, medusa_pb2.GetCommitLogArchiveStatusRequest)

    def test_get_commitlog_archive_status_returns_none_on_rpc_error(self):
        stub = mock.Mock()
        stub.GetCommitLogArchiveStatus = mock.AsyncMock(side_effect=_rpc_error())
        with mock.patch('medusa.service.grpc.client.medusa_pb2_grpc.MedusaStub', return_value=stub):
            self.assertIsNone(asyncio.run(self._client().get_commitlog_archive_status()))

    def test_prepare_pitr_restore(self):
        expected = medusa_pb2.PreparePitrRestoreResponse(status=medusa_pb2.StatusType.SUCCESS)
        stub = mock.Mock()
        stub.PreparePitrRestore = mock.AsyncMock(return_value=expected)
        with mock.patch('medusa.service.grpc.client.medusa_pb2_grpc.MedusaStub', return_value=stub):
            result = asyncio.run(self._client().prepare_pitr_restore('1767225600.000', 'dc1'))
        self.assertIs(expected, result)
        request = stub.PreparePitrRestore.call_args[0][0]
        self.assertEqual('1767225600.000', request.targetTimestamp)
        self.assertEqual('dc1', request.datacenter)

    def test_prepare_pitr_restore_defaults_to_all_datacenters(self):
        stub = mock.Mock()
        stub.PreparePitrRestore = mock.AsyncMock(return_value=medusa_pb2.PreparePitrRestoreResponse())
        with mock.patch('medusa.service.grpc.client.medusa_pb2_grpc.MedusaStub', return_value=stub):
            asyncio.run(self._client().prepare_pitr_restore('1'))
        self.assertEqual('', stub.PreparePitrRestore.call_args[0][0].datacenter)

    def test_prepare_pitr_restore_returns_none_on_rpc_error(self):
        stub = mock.Mock()
        stub.PreparePitrRestore = mock.AsyncMock(side_effect=_rpc_error())
        with mock.patch('medusa.service.grpc.client.medusa_pb2_grpc.MedusaStub', return_value=stub):
            self.assertIsNone(asyncio.run(self._client().prepare_pitr_restore('1')))


if __name__ == '__main__':
    unittest.main()
