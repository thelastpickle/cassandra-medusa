# -*- coding: utf-8 -*-
# Copyright 2020- Datastax, Inc. All rights reserved.
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
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from medusa.config import MedusaConfig, StorageConfig, CassandraConfig, ChecksConfig, PitrConfig
from medusa.service.grpc import medusa_pb2
from medusa.service.grpc.commitlog_archiver import CommitLogArchiver, DrainResult
from medusa.storage import Storage
from medusa.storage.abstract_storage import AbstractBlob

MODULE = 'medusa.service.grpc.commitlog_archiver'
STATUS_KEYS = {'running', 'last_upload_time', 'pending_count', 'interval_seconds', 'status', 'last_error'}


def _namedtuple(cls, **values):
    return cls(**{field: values.get(field) for field in cls._fields})


class CommitLogArchiverTest(unittest.TestCase):

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        root = Path(self._tmp.name)
        self.spool = root / 'spool'
        self.commitlog = root / 'commitlog'
        self.bucket_root = root / 'storage'
        for d in (self.spool, self.commitlog, self.bucket_root):
            d.mkdir()
        self.cassandra_yaml = root / 'cassandra.yaml'
        self.cassandra_yaml.write_text('commitlog_directory: {}\n'.format(self.commitlog))
        self.archivers = []

    def tearDown(self):
        for archiver in self.archivers:
            archiver.stop()

    def _make_config(self, spool=None, interval='1', md5='false', prefix='', provider='local'):
        return MedusaConfig(
            file_path=None,
            storage=_namedtuple(
                StorageConfig, storage_provider=provider, fqdn='node1', prefix=prefix, bucket_name='bucket',
                base_path=str(self.bucket_root), concurrent_transfers='1', multi_part_upload_threshold='20971520',
                multipart_chunksize='20971520'),
            cassandra=_namedtuple(CassandraConfig, config_file=str(self.cassandra_yaml)),
            ssh=None,
            checks=_namedtuple(ChecksConfig, enable_md5_checks=md5),
            monitoring=None, logging=None, grpc=None, kubernetes=None,
            pitr=_namedtuple(PitrConfig, enabled='true', commitlog_archive_interval_seconds=interval,
                             commitlog_spool_dir=str(spool or self.spool)),
        )

    def _make_archiver(self, **kwargs):
        archiver = CommitLogArchiver(self._make_config(**kwargs))
        self.archivers.append(archiver)
        return archiver

    def _write_spool(self, *names, content=b'segment'):
        for name in names:
            (self.spool / name).write_bytes(content)

    def _mock_storage(self):
        storage = MagicMock()
        ctx = MagicMock()
        ctx.__enter__.return_value = storage
        ctx.__exit__.return_value = False
        patcher = patch(MODULE + '.Storage', return_value=ctx)
        self.storage_cls = patcher.start()
        self.addCleanup(patcher.stop)
        return storage.storage_driver

    # ---- __init__ ----

    def test_init_succeeds_on_same_device_and_does_not_start(self):
        archiver = self._make_archiver()
        self.assertFalse(archiver.is_alive())
        self.assertTrue(archiver.daemon)

    def test_init_raises_on_different_device(self):
        real_stat = os.stat

        def with_dev(st, dev):
            fields = list(st)
            fields[2] = dev  # st_dev
            return os.stat_result(fields)

        def fake_stat(path, *args, **kwargs):
            st = real_stat(path, *args, **kwargs)
            if str(path) == str(self.spool):
                return with_dev(st, 1)
            if str(path) == str(self.commitlog):
                return with_dev(st, 2)
            return st

        config = self._make_config()
        with patch(MODULE + '.os.stat', side_effect=fake_stat):
            with self.assertRaises(RuntimeError) as ctx:
                CommitLogArchiver(config)
        message = str(ctx.exception)
        self.assertIn(str(self.spool), message)
        self.assertIn(str(self.commitlog), message)
        self.assertIn('Hardlinks cannot cross filesystem boundaries', message)

    def test_init_creates_missing_spool(self):
        spool = self.spool.parent / 'new' / 'spool'
        CommitLogArchiver(self._make_config(spool=spool))
        self.assertTrue(spool.is_dir())

    def test_init_raises_when_spool_cannot_be_created(self):
        blocker = self.spool.parent / 'file'
        blocker.write_bytes(b'')
        config = self._make_config(spool=blocker / 'spool')
        with self.assertRaises(RuntimeError) as ctx:
            CommitLogArchiver(config)
        self.assertIsInstance(ctx.exception.__cause__, OSError)

    def test_init_raises_when_stat_of_spool_fails(self):
        real_stat = os.stat

        def fake_stat(path, *args, **kwargs):
            if str(path) == str(self.spool):
                raise FileNotFoundError(path)
            return real_stat(path, *args, **kwargs)

        config = self._make_config()
        with patch(MODULE + '.os.stat', side_effect=fake_stat):
            with self.assertRaises(RuntimeError) as ctx:
                CommitLogArchiver(config)
        self.assertIsInstance(ctx.exception.__cause__, FileNotFoundError)
        self.assertIn('Hardlinks cannot cross filesystem boundaries', str(ctx.exception))

    def test_init_raises_when_commitlog_dir_missing(self):
        self.commitlog.rmdir()
        config = self._make_config()
        with self.assertRaises(RuntimeError) as ctx:
            CommitLogArchiver(config)
        self.assertIsInstance(ctx.exception.__cause__, FileNotFoundError)

    def test_init_raises_when_cassandra_yaml_missing(self):
        self.cassandra_yaml.unlink()
        config = self._make_config()
        with self.assertRaises(RuntimeError):
            CommitLogArchiver(config)

    def test_init_rejects_spool_names_that_change_the_storage_key(self):
        for name in ('.spool', 'spool_nodes'):
            bad = self.spool.parent / name
            bad.mkdir()
            config = self._make_config(spool=bad)
            with self.assertRaises(RuntimeError, msg=name):
                CommitLogArchiver(config)

    def test_init_rejects_interval_below_one(self):
        for interval in ('0', '-5'):
            config = self._make_config(interval=interval)
            with self.assertRaises(RuntimeError, msg=interval):
                CommitLogArchiver(config)

    # ---- drain_spool ----

    def test_drain_empty_spool_does_not_open_storage(self):
        self._mock_storage()
        result = self._make_archiver().drain_spool()
        self.assertEqual((0, 0), tuple(result[:2]))
        self.storage_cls.assert_not_called()

    def test_drain_uploads_absent_file_and_removes_it(self):
        driver = self._mock_storage()
        driver.get_object.return_value = None
        self._write_spool('CommitLog-7-1.log')
        archiver = self._make_archiver(prefix='pfx')

        result = archiver.drain_spool()

        self.assertEqual(DrainResult(1, 0, len(b'segment'), ''), result)
        driver.get_object.assert_called_once_with('pfx/node1/commitlogs/CommitLog-7-1.log')
        driver.get_blob.assert_not_called()
        driver.upload_blobs.assert_called_once_with([self.spool / 'CommitLog-7-1.log'], 'pfx/node1/commitlogs')
        self.assertFalse((self.spool / 'CommitLog-7-1.log').exists())

    def test_drain_skips_matching_file_and_removes_it(self):
        driver = self._mock_storage()
        driver.get_object.return_value = AbstractBlob('k', 7, 'h', None, None)
        driver.file_matches_storage.return_value = True
        self._write_spool('CommitLog-7-1.log')

        result = self._make_archiver().drain_spool()

        self.assertEqual((0, 1), tuple(result[:2]))
        driver.upload_blobs.assert_not_called()
        self.assertFalse((self.spool / 'CommitLog-7-1.log').exists())

    def test_drain_uploads_again_on_mismatch(self):
        driver = self._mock_storage()
        driver.get_object.return_value = AbstractBlob('k', 3, 'h', None, None)
        driver.file_matches_storage.return_value = False
        self._write_spool('CommitLog-7-1.log')

        result = self._make_archiver().drain_spool()

        self.assertEqual((1, 0), tuple(result[:2]))
        driver.upload_blobs.assert_called_once()
        self.assertFalse((self.spool / 'CommitLog-7-1.log').exists())

    def test_drain_passes_md5_flag_hash_and_thresholds(self):
        driver = self._mock_storage()
        driver.get_object.return_value = AbstractBlob('k', 7, 'abc', None, None)
        driver.file_matches_storage.return_value = True
        self._write_spool('CommitLog-7-1.log')

        self._make_archiver(md5='true').drain_spool()

        args = driver.file_matches_storage.call_args[0]
        self.assertEqual(self.spool / 'CommitLog-7-1.log', args[0])
        self.assertEqual(('k', 7, 'abc'), tuple(args[1]))
        self.assertEqual('20971520', args[2])
        self.assertTrue(args[3])
        self.assertEqual('20971520', args[4])

    def test_drain_stops_at_first_failure_and_keeps_the_rest(self):
        driver = self._mock_storage()
        driver.get_object.return_value = None
        driver.upload_blobs.side_effect = [None, Exception('boom'), None]
        self._write_spool('a.log', 'b.log', 'c.log')

        result = self._make_archiver().drain_spool()

        self.assertEqual((1, 0), tuple(result[:2]))
        self.assertIn('b.log', result.error)
        self.assertIn('boom', result.error)
        self.assertEqual(2, driver.upload_blobs.call_count)
        self.assertFalse((self.spool / 'a.log').exists())
        self.assertTrue((self.spool / 'b.log').exists())
        self.assertTrue((self.spool / 'c.log').exists())

    def test_drain_stops_when_existence_check_fails(self):
        driver = self._mock_storage()
        driver.get_object.side_effect = Exception('storage down')
        self._write_spool('a.log', 'b.log')

        result = self._make_archiver().drain_spool()

        self.assertEqual((0, 0), tuple(result[:2]))
        self.assertIn('storage down', result.error)
        driver.get_object.assert_called_once()
        driver.upload_blobs.assert_not_called()
        self.assertEqual(2, len(os.listdir(self.spool)))

    def test_drain_ignores_subdirectories_and_symlinks(self):
        driver = self._mock_storage()
        driver.get_object.return_value = None
        (self.spool / 'subdir').mkdir()
        target = self.commitlog / 'target.log'
        target.write_bytes(b'x')
        os.symlink(target, self.spool / 'link.log')
        self._write_spool('real.log')

        result = self._make_archiver().drain_spool()

        self.assertEqual((1, 0), tuple(result[:2]))
        self.assertTrue((self.spool / 'subdir').exists())
        self.assertTrue((self.spool / 'link.log').is_symlink())

    def test_drain_files_are_processed_in_name_order(self):
        driver = self._mock_storage()
        driver.get_object.return_value = None
        self._write_spool('CommitLog-7-3.log', 'CommitLog-7-1.log', 'CommitLog-7-2.log')

        self._make_archiver().drain_spool()

        uploaded = [c[0][0][0].name for c in driver.upload_blobs.call_args_list]
        self.assertEqual(['CommitLog-7-1.log', 'CommitLog-7-2.log', 'CommitLog-7-3.log'], uploaded)

    def test_drain_stops_between_files_when_stop_requested(self):
        driver = self._mock_storage()
        driver.get_object.return_value = None
        self._write_spool('a.log', 'b.log', 'c.log')
        archiver = self._make_archiver()

        def upload_and_stop(*args, **kwargs):
            archiver._stop_event.set()

        driver.upload_blobs.side_effect = upload_and_stop

        result = archiver.drain_spool()

        self.assertEqual((1, 0), tuple(result[:2]))
        self.assertFalse((self.spool / 'a.log').exists())
        self.assertTrue((self.spool / 'b.log').exists())
        self.assertTrue((self.spool / 'c.log').exists())

    def test_drain_end_to_end_with_local_storage(self):
        for prefix in ('', 'myprefix'):
            with self.subTest(prefix=prefix):
                self._write_spool('CommitLog-7-1.log', content=b'hello')
                archiver = self._make_archiver(prefix=prefix)

                result = archiver.drain_spool()

                self.assertEqual((1, 0), tuple(result[:2]))
                expected = self.bucket_root / 'bucket' / prefix / 'node1' / 'commitlogs' / 'CommitLog-7-1.log'
                self.assertEqual(b'hello', expected.read_bytes())
                self.assertFalse((self.spool / 'CommitLog-7-1.log').exists())

                # the same segment again (e.g. after a restart) is skipped, not uploaded again
                self._write_spool('CommitLog-7-1.log', content=b'hello')
                with patch.object(Storage, '__enter__', autospec=True, side_effect=Storage.__enter__), \
                        patch('medusa.storage.local_storage.LocalStorage.upload_blobs') as upload:
                    self.assertEqual((0, 1), tuple(archiver.drain_spool()[:2]))
                    upload.assert_not_called()
                self.assertFalse((self.spool / 'CommitLog-7-1.log').exists())

    # ---- _run_once ----

    def test_run_once_never_raises_and_keeps_file_when_upload_fails(self):
        driver = self._mock_storage()
        driver.get_object.return_value = None
        driver.upload_blobs.side_effect = Exception('boom')
        self._write_spool('a.log', 'b.log')
        archiver = self._make_archiver()

        archiver._run_once()

        self.assertTrue((self.spool / 'a.log').exists())
        self.assertEqual(1, driver.upload_blobs.call_count)
        status = archiver.status()
        self.assertIn('boom', status['last_error'])
        self.assertEqual(2, status['pending_count'])
        self.assertEqual(0, status['last_upload_time'])

    def test_run_once_clears_last_error_after_failed_then_good_tick(self):
        driver = self._mock_storage()
        driver.get_object.return_value = None
        driver.upload_blobs.side_effect = [Exception('boom'), None, None]
        self._write_spool('a.log', 'b.log')
        archiver = self._make_archiver()

        archiver._run_once()
        self.assertNotEqual('', archiver.status()['last_error'])
        archiver._run_once()

        status = archiver.status()
        self.assertEqual('', status['last_error'])
        self.assertEqual(0, status['pending_count'])
        self.assertGreater(status['last_upload_time'], 0)

    def test_run_once_sets_last_upload_time_on_success(self):
        driver = self._mock_storage()
        driver.get_object.return_value = None
        self._write_spool('a.log')
        archiver = self._make_archiver()

        archiver._run_once()

        self.assertGreater(archiver.status()['last_upload_time'], 0)

    def test_run_once_sets_last_error_when_spool_unreadable_then_clears(self):
        archiver = self._make_archiver()
        with patch(MODULE + '.os.scandir', side_effect=PermissionError('denied')):
            archiver._run_once()
            self.assertEqual('denied', archiver._last_error)
        archiver._run_once()
        self.assertEqual('', archiver._last_error)

    def test_run_once_sets_last_error_when_storage_unavailable(self):
        self._write_spool('a.log')
        archiver = self._make_archiver()
        with patch(MODULE + '.Storage', side_effect=Exception('no storage')):
            archiver._run_once()
        self.assertEqual('no storage', archiver._last_error)
        self.assertTrue((self.spool / 'a.log').exists())

    def test_run_once_keeps_pending_count_when_spool_unreadable(self):
        archiver = self._make_archiver()
        archiver._pending_count = 4
        with patch(MODULE + '.os.scandir', side_effect=PermissionError('denied')):
            archiver._run_once()
        self.assertEqual(4, archiver._pending_count)

    def test_run_finishes_whole_batch_before_waiting(self):
        driver = self._mock_storage()
        driver.get_object.return_value = None
        self._write_spool('a.log', 'b.log', 'c.log')
        archiver = self._make_archiver()
        seen = {}
        spool = self.spool

        class FakeEvent:
            waited = False

            def is_set(self):
                return FakeEvent.waited

            def set(self):
                FakeEvent.waited = True

            def wait(self, timeout):
                FakeEvent.waited = True
                seen['uploads'] = driver.upload_blobs.call_count
                seen['left'] = len(os.listdir(spool))
                seen['timeout'] = timeout

        archiver._stop_event = FakeEvent()

        archiver.run()

        self.assertEqual({'uploads': 3, 'left': 0, 'timeout': 1}, seen)

    # ---- status / stop ----

    def test_status_unknown_before_start(self):
        status = self._make_archiver().status()
        self.assertEqual(STATUS_KEYS, set(status))
        self.assertEqual(medusa_pb2.StatusType.UNKNOWN, status['status'])
        self.assertFalse(status['running'])
        self.assertEqual(1, status['interval_seconds'])

    def test_status_reports_pending_count_without_a_tick(self):
        self._write_spool('a.log', 'b.log', 'c.log')
        self.assertEqual(3, self._make_archiver().status()['pending_count'])

    def test_status_keeps_pending_count_and_sets_error_when_listing_fails(self):
        self._write_spool('a.log', 'b.log')
        archiver = self._make_archiver()
        self.assertEqual(2, archiver.status()['pending_count'])
        with patch(MODULE + '.os.scandir', side_effect=PermissionError('denied')):
            status = archiver.status()
        self.assertEqual(2, status['pending_count'])
        self.assertEqual('denied', status['last_error'])

    def test_status_in_progress_while_alive_and_success_after_stop(self):
        archiver = self._make_archiver(interval='30')
        started = threading.Event()
        with patch.object(CommitLogArchiver, '_run_once', side_effect=lambda: started.set()):
            archiver.start()
            self.assertTrue(started.wait(5))
            status = archiver.status()
            self.assertEqual(STATUS_KEYS, set(status))
            self.assertTrue(status['running'])
            self.assertEqual(medusa_pb2.StatusType.IN_PROGRESS, status['status'])
            self.assertIsInstance(status['last_upload_time'], int)
            self.assertIsInstance(status['pending_count'], int)

            begin = time.monotonic()
            archiver.stop()
            self.assertLess(time.monotonic() - begin, 35)
        self.assertFalse(archiver.is_alive())
        status = archiver.status()
        self.assertFalse(status['running'])
        self.assertEqual(medusa_pb2.StatusType.SUCCESS, status['status'])

    def test_status_failed_after_unexpected_exit(self):
        archiver = self._make_archiver()
        with patch.object(CommitLogArchiver, 'run', return_value=None):
            archiver.start()
            archiver.join(5)
        self.assertFalse(archiver.is_alive())
        self.assertEqual(medusa_pb2.StatusType.FAILED, archiver.status()['status'])

    def test_status_success_after_stop_even_with_last_error(self):
        archiver = self._make_archiver(interval='30')
        started = threading.Event()

        def failing_tick():
            archiver._last_error = 'boom'
            started.set()

        with patch.object(CommitLogArchiver, '_run_once', side_effect=failing_tick):
            archiver.start()
            self.assertTrue(started.wait(5))
            archiver.stop()
        status = archiver.status()
        self.assertEqual('boom', status['last_error'])
        self.assertEqual(medusa_pb2.StatusType.SUCCESS, status['status'])

    def test_stop_before_start_does_not_raise(self):
        self._make_archiver().stop()


if __name__ == '__main__':
    unittest.main()
