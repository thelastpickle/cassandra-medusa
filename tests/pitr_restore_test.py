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
import configparser
import datetime
import os
import shutil
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import MagicMock

from medusa.config import MedusaConfig, StorageConfig, _namedtuple_from_dict, CassandraConfig
from medusa.pitr_restore import (
    select_node_base_snapshot,
    find_next_backup,
    find_commitlog_segments,
    download_commitlog_segments,
    generate_commitlog_archiving_properties,
)
from medusa.storage import Storage
from medusa.storage.abstract_storage import AbstractBlob

FQDN = 'node1.example.com'
# 2026-01-01 00:00:00 UTC
T = 1767225600


class FakeNodeBackup(object):
    """
    Stands in for NodeBackup. `started` is always set, to prove that it is never used as a fallback.
    """
    def __init__(self, name, snapshot_time, started=0, fqdn=FQDN):
        self.name = name
        self.fqdn = fqdn
        self._snapshot_time = snapshot_time
        self.started = started

    @property
    def snapshot_time(self):
        if isinstance(self._snapshot_time, Exception):
            raise self._snapshot_time
        return self._snapshot_time

    def __repr__(self):
        return 'FakeNodeBackup({})'.format(self.name)


def make_storage(backups=()):
    storage = MagicMock()
    storage.list_node_backups.return_value = list(backups)
    return storage


class SelectNodeBaseSnapshotTest(unittest.TestCase):

    def test_selects_latest_eligible_backup(self):
        b1, b2, b3 = FakeNodeBackup('b1', 100), FakeNodeBackup('b2', 200), FakeNodeBackup('b3', 300)
        storage = make_storage([b2, b1, b3])
        self.assertIs(b2, select_node_base_snapshot(storage, FQDN, 250))
        storage.list_node_backups.assert_called_once_with(fqdn=FQDN)

    def test_ignores_backups_after_target(self):
        b1, b2 = FakeNodeBackup('b1', 100), FakeNodeBackup('b2', 300)
        self.assertIs(b1, select_node_base_snapshot(make_storage([b1, b2]), FQDN, 250.5))

    def test_all_backups_after_target_raises(self):
        storage = make_storage([FakeNodeBackup('b1', 300), FakeNodeBackup('b2', 400)])
        with self.assertRaises(ValueError):
            select_node_base_snapshot(storage, FQDN, 250)

    def test_backup_one_second_before_target_is_selected(self):
        # snapshot_time is truncated to whole seconds: s + 1 is the earliest target for which s is safe
        b1, b2 = FakeNodeBackup('b1', 100), FakeNodeBackup('b2', 200)
        self.assertIs(b2, select_node_base_snapshot(make_storage([b1, b2]), FQDN, 201.0))

    def test_backup_less_than_one_second_before_target_is_not_selected(self):
        b1, b2 = FakeNodeBackup('b1', 100), FakeNodeBackup('b2', 200)
        self.assertIs(b1, select_node_base_snapshot(make_storage([b1, b2]), FQDN, 200.999))

    def test_backup_exactly_at_target_is_not_selected(self):
        # the snapshot may have been captured up to 1s after its recorded (truncated) snapshot_time
        b1, b2 = FakeNodeBackup('b1', 100), FakeNodeBackup('b2', 200)
        self.assertIs(b1, select_node_base_snapshot(make_storage([b1, b2]), FQDN, 200.0))

    def test_snapshot_captured_after_target_within_the_same_second_is_not_selected(self):
        # a snapshot captured at 12:07:00.850 is recorded as 12:07:00; a restore to 12:07:00.100 must not use it,
        # because its SSTables already hold writes from 12:07:00.100 to 12:07:00.850
        older = FakeNodeBackup('older', T - 3600)
        same_second = FakeNodeBackup('same_second', T)
        storage = make_storage([older, same_second])
        self.assertIs(older, select_node_base_snapshot(storage, FQDN, T + 0.100))

    def test_snapshot_captured_after_target_within_the_same_second_without_older_backup_raises(self):
        storage = make_storage([FakeNodeBackup('same_second', T)])
        with self.assertRaises(ValueError):
            select_node_base_snapshot(storage, FQDN, T + 0.100)

    def test_snapshot_time_none_is_ignored_without_fallback_to_started(self):
        # started is far below the target, snapshot_time is None: must not be used as a proxy
        legacy = FakeNodeBackup('legacy', None, started=T - 1000)
        pitr = FakeNodeBackup('pitr', T - 500, started=T - 600)
        self.assertIs(pitr, select_node_base_snapshot(make_storage([legacy, pitr]), FQDN, T))

    def test_only_snapshot_time_none_raises(self):
        storage = make_storage([FakeNodeBackup('legacy', None, started=T - 1000)])
        with self.assertRaises(ValueError):
            select_node_base_snapshot(storage, FQDN, T)

    def test_invalid_snapshot_time_is_ignored(self):
        broken = FakeNodeBackup('broken', ValueError('invalid literal for int()'), started=T - 1000)
        garbage = FakeNodeBackup('garbage', 'not-a-number', started=T - 1000)
        nan = FakeNodeBackup('nan', float('nan'), started=T - 1000)
        good = FakeNodeBackup('good', T - 10)
        self.assertIs(good, select_node_base_snapshot(make_storage([broken, garbage, nan, good]), FQDN, T))
        with self.assertRaises(ValueError):
            select_node_base_snapshot(make_storage([broken, garbage, nan]), FQDN, T)

    def test_snapshot_time_zero_is_a_valid_value(self):
        b0 = FakeNodeBackup('b0', 0)
        self.assertIs(b0, select_node_base_snapshot(make_storage([b0]), FQDN, 10))

    def test_backups_of_other_nodes_are_ignored(self):
        # list_node_backups() matches the fqdn by substring
        other = FakeNodeBackup('other', 200, fqdn='node1.example.com.other')
        mine = FakeNodeBackup('mine', 100)
        self.assertIs(mine, select_node_base_snapshot(make_storage([other, mine]), FQDN, 250))

    def test_no_backups_raises(self):
        with self.assertRaises(ValueError):
            select_node_base_snapshot(make_storage([]), FQDN, T)


class FindNextBackupTest(unittest.TestCase):

    def test_returns_earliest_backup_after_base(self):
        b1, b2, b3, b4 = (FakeNodeBackup('b{}'.format(i), i * 100) for i in range(1, 5))
        storage = make_storage([b4, b1, b3, b2])
        self.assertIs(b3, find_next_backup(storage, FQDN, b2))
        storage.list_node_backups.assert_called_once_with(fqdn=FQDN)

    def test_base_is_the_newest(self):
        b1, b2 = FakeNodeBackup('b1', 100), FakeNodeBackup('b2', 200)
        self.assertIsNone(find_next_backup(make_storage([b1, b2]), FQDN, b2))

    def test_backup_with_same_snapshot_time_is_not_next(self):
        b1, same = FakeNodeBackup('b1', 100), FakeNodeBackup('same', 100)
        self.assertIsNone(find_next_backup(make_storage([b1, same]), FQDN, b1))

    def test_snapshot_time_none_is_ignored(self):
        b1 = FakeNodeBackup('b1', 100)
        legacy = FakeNodeBackup('legacy', None, started=500)
        b3 = FakeNodeBackup('b3', 300)
        self.assertIs(b3, find_next_backup(make_storage([legacy, b1, b3]), FQDN, b1))
        self.assertIsNone(find_next_backup(make_storage([legacy, b1]), FQDN, b1))

    def test_invalid_snapshot_time_is_ignored(self):
        b1 = FakeNodeBackup('b1', 100)
        broken = FakeNodeBackup('broken', ValueError('bad'))
        self.assertIsNone(find_next_backup(make_storage([b1, broken]), FQDN, b1))

    def test_base_without_snapshot_time_raises_value_error(self):
        legacy = FakeNodeBackup('legacy', None)
        with self.assertRaises(ValueError):
            find_next_backup(make_storage([legacy, FakeNodeBackup('b2', 200)]), FQDN, legacy)


def blob(name, last_modified):
    return AbstractBlob(name, 10, 'hash', last_modified, None)


def make_driver_storage(blobs):
    storage = MagicMock()
    storage.storage_driver.list_objects.return_value = list(blobs)
    # mimics AbstractStorage.get_object_datetime, which returns blob.last_modified as is
    storage.storage_driver.get_object_datetime.side_effect = lambda b: b.last_modified
    return storage


def utc(ts):
    return datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc)


class FindCommitlogSegmentsTest(unittest.TestCase):

    AFTER = 1000.0
    UPPER = 2000.0
    MARGIN = 30.0

    def _find(self, blobs, **kwargs):
        storage = make_driver_storage(blobs)
        kwargs.setdefault('upper_bound_s', self.UPPER)
        return find_commitlog_segments(storage, 'prefix', FQDN, self.AFTER, **kwargs), storage

    def test_lists_the_node_commitlogs_folder(self):
        _, storage = self._find([])
        storage.storage_driver.list_objects.assert_called_once_with(path='prefix/{}/commitlogs/'.format(FQDN))

    def test_empty_and_dot_prefix(self):
        for prefix in ('', None, '.'):
            storage = make_driver_storage([])
            find_commitlog_segments(storage, prefix, FQDN, self.AFTER, self.UPPER)
            storage.storage_driver.list_objects.assert_called_once_with(path='{}/commitlogs/'.format(FQDN))

    def test_prefix_with_trailing_slash(self):
        storage = make_driver_storage([])
        find_commitlog_segments(storage, 'prefix/', FQDN, self.AFTER, self.UPPER)
        storage.storage_driver.list_objects.assert_called_once_with(path='prefix/{}/commitlogs/'.format(FQDN))

    def test_boundaries_are_inclusive(self):
        lower = blob('p/n/commitlogs/lower', utc(self.AFTER - self.MARGIN))
        upper = blob('p/n/commitlogs/upper', utc(self.UPPER))
        below = blob('p/n/commitlogs/below', utc(self.AFTER - self.MARGIN - 0.001))
        above = blob('p/n/commitlogs/above', utc(self.UPPER + 0.001))
        found, _ = self._find([below, lower, upper, above])
        self.assertEqual([lower, upper], found)

    def test_safety_margin(self):
        inside_margin = blob('c/inside', utc(self.AFTER - 10))
        outside_margin = blob('c/outside', utc(self.AFTER - 31))
        found, _ = self._find([inside_margin, outside_margin])
        self.assertEqual([inside_margin], found)

        # a custom margin moves the lower bound
        found, _ = self._find([inside_margin, outside_margin], safety_margin_s=5)
        self.assertEqual([], found)
        found, _ = self._find([inside_margin, outside_margin], safety_margin_s=100)
        self.assertEqual([inside_margin, outside_margin], found)

    def test_zero_safety_margin(self):
        exact = blob('c/exact', utc(self.AFTER))
        before = blob('c/before', utc(self.AFTER - 0.5))
        found, _ = self._find([exact, before], safety_margin_s=0)
        self.assertEqual([exact], found)

    def test_upper_bound_cutoff(self):
        inside = blob('c/inside', utc(self.UPPER - 1))
        late = blob('c/late', utc(self.UPPER + 100))
        found, _ = self._find([inside, late])
        self.assertEqual([inside], found)

    def test_upper_bound_is_mandatory(self):
        storage = make_driver_storage([])
        with self.assertRaises(TypeError):
            find_commitlog_segments(storage, 'prefix', FQDN, self.AFTER)

    def test_sorted_by_filename(self):
        names = ['c/CommitLog-7-3.log', 'c/CommitLog-7-1.log', 'c/CommitLog-7-2.log', 'c/CommitLog-6-9.log']
        blobs = [blob(n, utc(self.AFTER + i)) for i, n in enumerate(names)]
        found, _ = self._find(blobs)
        self.assertEqual(sorted(names), [b.name for b in found])

    def test_last_modified_as_naive_datetime(self):
        # LocalStorage hands out naive datetimes in local time
        naive = datetime.datetime.fromtimestamp(self.AFTER + 5)
        inside = blob('c/inside', naive)
        outside = blob('c/outside', datetime.datetime.fromtimestamp(self.UPPER + 5))
        found, _ = self._find([inside, outside])
        self.assertEqual([inside], found)

    def test_last_modified_as_number(self):
        int_inside = blob('c/a_int', int(self.AFTER + 5))
        float_inside = blob('c/b_float', self.UPPER - 0.5)
        int_outside = blob('c/c_int', int(self.UPPER + 5))
        float_outside = blob('c/d_float', self.AFTER - 100.5)
        found, _ = self._find([float_outside, float_inside, int_outside, int_inside])
        self.assertEqual([int_inside, float_inside], found)

    def test_uses_the_storage_driver_to_get_the_datetime(self):
        b = blob('c/a', utc(self.AFTER))
        _, storage = self._find([b])
        storage.storage_driver.get_object_datetime.assert_called_with(b)

    # KNOWN BUG: Cassandra only calls archive_command for a segment when it is full, drained or restarted,
    # not on snapshot/flush. So a segment holding writes made before the restore target can be sealed, and thus
    # uploaded, after next_backup.snapshot_time + grace. Its blob last_modified (upload time) is then greater than
    # upper_bound_s and the segment is silently left out of the restore. Remove expectedFailure once fixed.
    @unittest.expectedFailure
    def test_segment_open_across_next_backup_is_selected(self):
        t = 1767225600  # 2026-01-01 00:00:00 UTC, "day 1 00:00"
        base_started = t + 7200  # day 1 02:00, base backup started / snapshot_time
        # restore target is day 1 14:00 (t + 50400); next backup snapshot_time is day 2 02:00 (t + 93600)
        upper_bound = t + 93600 + 3600  # next snapshot_time + 1h grace = day 2 03:00

        # CommitLog-6-9 was uploaded at day 1 01:00, before the base backup: not needed
        n6 = blob('prefix/{}/commitlogs/CommitLog-6-9.log'.format(FQDN), utc(t + 3600))
        # CommitLog-7-1 was the active segment from day 1 00:00 to day 2 06:00. It holds the write made at
        # day 1 10:00 (before the target) and was only archived and uploaded at day 2 06:01.
        n7 = blob('prefix/{}/commitlogs/CommitLog-7-1.log'.format(FQDN), utc(t + 86400 + 21660))

        storage = make_driver_storage([n6, n7])
        found = find_commitlog_segments(
            storage, 'prefix', FQDN, after_snapshot_time_s=base_started, upper_bound_s=upper_bound
        )

        self.assertIn(n7, found)


class DownloadCommitlogSegmentsTest(unittest.TestCase):

    def setUp(self):
        self.base_path = tempfile.mkdtemp()
        config = configparser.ConfigParser(interpolation=None)
        config['storage'] = {
            'host_file_separator': ',',
            'bucket_name': 'pitr_restore_test_bucket',
            'key_file': '',
            'storage_provider': 'local',
            'fqdn': FQDN,
            'api_key_or_username': '',
            'api_secret_or_password': '',
            'base_path': self.base_path,
            'concurrent_transfers': 1,
        }
        config['cassandra'] = {'is_ccm': 1}
        self.config = MedusaConfig(
            file_path=None,
            storage=_namedtuple_from_dict(StorageConfig, config['storage']),
            cassandra=_namedtuple_from_dict(CassandraConfig, config['cassandra']),
            monitoring={}, ssh=None, checks=None, logging=None, grpc=None, kubernetes=None,
        )
        # LocalStorage only touches the local filesystem, there is no bucket involved
        self.storage = Storage(config=self.config.storage)
        self.destination = Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.base_path, ignore_errors=True)
        shutil.rmtree(self.destination, ignore_errors=True)

    def _upload(self, name, content):
        key = '{}/commitlogs/{}'.format(FQDN, name)
        self.storage.storage_driver.upload_blob_from_string(key, content)
        return self.storage.storage_driver.get_blob(key)

    def test_downloads_into_destination_dir(self):
        segments = [self._upload('CommitLog-7-1.log', 'one'), self._upload('CommitLog-7-2.log', 'two')]
        download_commitlog_segments(self.storage, segments, self.destination)
        self.assertEqual('one', (self.destination / 'CommitLog-7-1.log').read_text())
        self.assertEqual('two', (self.destination / 'CommitLog-7-2.log').read_text())
        self.assertEqual(2, len(os.listdir(self.destination)))

    def test_overwrites_existing_files(self):
        segments = [self._upload('CommitLog-7-1.log', 'new content')]
        (self.destination / 'CommitLog-7-1.log').write_text('old, much longer, stale content')
        (self.destination / 'untouched.log').write_text('keep me')
        download_commitlog_segments(self.storage, segments, str(self.destination))
        self.assertEqual('new content', (self.destination / 'CommitLog-7-1.log').read_text())
        self.assertEqual('keep me', (self.destination / 'untouched.log').read_text())

    def test_creates_missing_destination_dir(self):
        segments = [self._upload('CommitLog-7-1.log', 'one')]
        destination = self.destination / 'does' / 'not' / 'exist'
        download_commitlog_segments(self.storage, segments, destination)
        self.assertEqual('one', (destination / 'CommitLog-7-1.log').read_text())

    def test_no_segments_is_a_noop(self):
        storage = MagicMock()
        destination = self.destination / 'untouched'
        download_commitlog_segments(storage, [], destination)
        storage.storage_driver.download_blobs.assert_not_called()
        self.assertFalse(destination.exists())

    def test_asks_the_driver_to_download_by_blob_name(self):
        storage = MagicMock()
        segments = [blob('p/n/commitlogs/a', 1), blob('p/n/commitlogs/b', 2)]
        download_commitlog_segments(storage, segments, self.destination)
        storage.storage_driver.download_blobs.assert_called_once_with(
            ['p/n/commitlogs/a', 'p/n/commitlogs/b'], str(self.destination))


class GenerateCommitlogArchivingPropertiesTest(unittest.TestCase):

    ARCHIVE = '/bin/ln %path /var/lib/spool/%name'
    RESTORE_COMMAND = 'restore_command=/bin/cp -f %from %to'

    def _lines(self, *args, **kwargs):
        return generate_commitlog_archiving_properties(*args, **kwargs).splitlines()

    def _restore_point(self, ts):
        props = generate_commitlog_archiving_properties(self.ARCHIVE, '/var/lib/staging', ts)
        lines = [line for line in props.splitlines() if line.startswith('restore_point_in_time=')]
        self.assertEqual(1, len(lines))
        return lines[0][len('restore_point_in_time='):]

    def test_archive_only_modes(self):
        expected = ['archive_command=' + self.ARCHIVE]
        self.assertEqual(expected, self._lines(self.ARCHIVE))
        self.assertEqual(expected, self._lines(self.ARCHIVE, restore_directories='/var/lib/staging'))
        self.assertEqual(expected, self._lines(self.ARCHIVE, target_timestamp_s=T))
        self.assertEqual(expected, self._lines(self.ARCHIVE, None, None))

    def test_restore_mode(self):
        props = generate_commitlog_archiving_properties(self.ARCHIVE, '/var/lib/staging', T)
        self.assertEqual(
            [
                'archive_command=' + self.ARCHIVE,
                'restore_directories=/var/lib/staging',
                'restore_command=/bin/cp -f %from %to',
                'restore_point_in_time=2026:01:01 00:00:00.000',
            ],
            props.splitlines()
        )

    def test_restore_mode_always_has_restore_command(self):
        # Without restore_command Cassandra, DSE and HCD fail with a NullPointerException in
        # CommitLogArchiver.maybeRestoreArchive
        for ts in (0, T, T + 0.123, time.time()):
            self.assertIn(self.RESTORE_COMMAND, self._lines(self.ARCHIVE, '/staging', ts))

    def test_archive_only_mode_has_no_restore_settings(self):
        for props in (self._lines(self.ARCHIVE), self._lines(self.ARCHIVE, '/staging'),
                      self._lines(self.ARCHIVE, target_timestamp_s=T)):
            self.assertFalse([line for line in props if line.startswith('restore_')])

    def test_target_timestamp_zero_counts_as_provided(self):
        self.assertEqual('1970:01:01 00:00:00.000', self._restore_point(0))

    def test_restore_point_format(self):
        self.assertEqual('2026:01:01 00:00:00.000', self._restore_point(1767225600.0))
        self.assertEqual('2026:01:01 00:00:00.000', self._restore_point(1767225600))
        self.assertEqual('2026:01:01 00:00:00.500', self._restore_point(1767225600.5))
        self.assertEqual('2026:01:01 00:00:00.001', self._restore_point(1767225600.001))
        self.assertEqual('2026:01:01 00:00:00.999', self._restore_point(1767225600.9999))

    def test_restore_point_is_truncated_never_rounded_up(self):
        self.assertEqual('2026:01:01 00:00:00.999', self._restore_point(1767225600.99999))
        self.assertEqual('2026:01:01 00:00:00.123', self._restore_point(1767225600.1239999))
        # a rounded-up value would roll over into the next second
        self.assertEqual('2026:01:01 00:00:59.999', self._restore_point(1767225659.99999))

    def test_restore_point_is_robust_to_float_error(self):
        for millis in range(0, 1000):
            ts = T + millis / 1000
            self.assertEqual('2026:01:01 00:00:00.{:03d}'.format(millis), self._restore_point(ts), ts)

    def test_restore_point_carries_across_date_boundaries(self):
        self.assertEqual('2025:12:31 23:59:59.999', self._restore_point(T - 0.001))
        self.assertEqual('2025:12:31 23:59:59.500', self._restore_point(T - 0.5))

    def test_restore_point_before_epoch(self):
        self.assertEqual('1969:12:31 23:59:59.999', self._restore_point(-0.001))

    def test_invalid_target_timestamp(self):
        for bad in (float('nan'), float('inf'), float('-inf')):
            with self.assertRaises(ValueError):
                generate_commitlog_archiving_properties(self.ARCHIVE, '/staging', bad)

    def test_restore_point_is_utc_regardless_of_local_timezone(self):
        if not hasattr(time, 'tzset'):
            self.skipTest('time.tzset is not available on this platform')
        old_tz = os.environ.get('TZ')
        try:
            os.environ['TZ'] = 'Asia/Kolkata'
            time.tzset()
            # sanity check: the local time zone really is UTC+5:30 now
            self.assertEqual(19800, -time.timezone)
            self.assertEqual('2026:01:01 00:00:00.000', self._restore_point(T))
            self.assertEqual('2026:01:01 00:00:00.001', self._restore_point(1767225600.001))
        finally:
            if old_tz is None:
                os.environ.pop('TZ', None)
            else:
                os.environ['TZ'] = old_tz
            time.tzset()


if __name__ == '__main__':
    unittest.main()
