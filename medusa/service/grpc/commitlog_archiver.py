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

import asyncio
import collections
import logging
import os
import threading
import time
from pathlib import Path

from medusa.cassandra_utils import CassandraConfigReader
from medusa.service.grpc import medusa_pb2
from medusa.storage import Storage
from medusa.storage.abstract_storage import AbstractStorage, ManifestObject
from medusa.utils import evaluate_boolean

# error is '' when every listed segment was archived (or a stop was requested), otherwise it describes the
# segment that failed and ended the tick.
DrainResult = collections.namedtuple('DrainResult', ['uploaded', 'skipped', 'uploaded_bytes', 'error'],
                                     defaults=[0, ''])


class CommitLogArchiver(threading.Thread):
    """
    Uploads the commitlog segments that Cassandra hardlinks into the spool directory.

    Cassandra's ``archive_command`` (``/bin/ln %path <commitlog_spool_dir>/%name``) creates a hardlink of every
    finished segment in ``commitlog_spool_dir``. A hardlink cannot cross filesystem boundaries (``ln`` fails with
    ``EXDEV``), so the spool directory must be on the same device as the Cassandra commitlog directory. ``__init__``
    checks this and raises ``RuntimeError`` if it does not hold, so a bad mount stops the server at startup.

    Every tick uploads the spooled segments in name order to ``<prefix>/<fqdn>/commitlogs/<filename>``, unless a
    matching object is already there (a restart must not upload twice). The hardlink is removed only after the
    upload or the skip was confirmed.

    The first segment that cannot be archived ends the tick. It and every later segment stay in the spool and are
    retried on the next tick. Continuing would not help: failures are nearly always storage-wide (each one costing
    the storage driver's own retries), and a point-in-time restore cannot replay past a missing segment anyway.

    ``last_error`` rules (``status()``):
    - set to the exception message when a tick cannot read the spool, cannot open storage, or fails to archive a
      segment; also set when ``status()`` itself cannot list the spool,
    - reset to ``''`` by the next tick that archives everything it listed.
    ``pending_count`` is the number of files in the spool, listed again on every ``status()`` call. If that listing
    fails the last known value is kept, so ``pending_count=0`` with an empty ``last_error`` means an empty spool,
    while ``pending_count=0`` with a non-empty ``last_error`` means the spool could not be read.
    """

    def __init__(self, config):
        pitr = config.pitr
        self._spool_dir = Path(pitr.commitlog_spool_dir)
        self._interval = int(pitr.commitlog_archive_interval_seconds)
        self._storage_config = config.storage
        self._enable_md5_checks = evaluate_boolean(config.checks.enable_md5_checks) if config.checks else False
        self._folder = str(Path(config.storage.prefix or '.') / config.storage.fqdn / 'commitlogs')

        if self._interval < 1:
            raise RuntimeError(
                'commitlog_archive_interval_seconds must be at least 1, got {}'.format(
                    pitr.commitlog_archive_interval_seconds))

        # AbstractStorage.path_maybe_with_parent() adds the parent folder name to the object key for such names
        # (secondary index and DSE folders), which would put the segments where restore and purge never look.
        spool_name = self._spool_dir.name
        if spool_name.startswith('.') or spool_name.endswith('nodes'):
            raise RuntimeError(
                "commitlog_spool_dir '{}' is not supported: its name must not start with '.' "
                "or end with 'nodes'".format(self._spool_dir))

        commitlog_dir = CassandraConfigReader(config.cassandra.config_file).commitlog_directory
        msg = ("commitlog_spool_dir '{}' is on a different device than the Cassandra commitlog directory '{}'. "
               "Hardlinks cannot cross filesystem boundaries. Mount both paths on the same volume."
               .format(self._spool_dir, commitlog_dir))
        try:
            commitlog_dev = os.stat(commitlog_dir).st_dev
            # Creating a missing spool dir is safe: if the shared volume is not mounted, the new folder ends up on
            # another device and the check below still fails.
            self._spool_dir.mkdir(parents=True, exist_ok=True)
            spool_dev = os.stat(self._spool_dir).st_dev
        except OSError as e:
            raise RuntimeError('{} ({})'.format(msg, e)) from e
        if spool_dev != commitlog_dev:
            raise RuntimeError(msg)

        super().__init__(name='commitlog-archiver', daemon=True)
        self._stop_event = threading.Event()
        self._lock = threading.Lock()
        self._last_upload_time = 0
        self._last_error = ''
        self._pending_count = 0

    def run(self):
        # the storage drivers run their coroutines on the thread's event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            while not self._stop_event.is_set():
                self._run_once()
                # the wait starts only after the whole batch is done
                self._stop_event.wait(self._interval)
        finally:
            loop.close()

    def _run_once(self):
        """
        Runs one tick and never raises an Exception. The outcome is stored in last_error.
        """
        try:
            result = self.drain_spool()
            error = result.error
        except Exception as e:
            logging.exception('Commitlog archiver tick failed: {}'.format(e))
            result, error = None, str(e)

        with self._lock:
            self._last_error = error
            if result is not None and result.uploaded:
                self._last_upload_time = int(time.time())

        if result is not None and (result.uploaded or result.skipped):
            logging.info('Archived {} commitlog segment(s) ({}), skipped {} already in storage'.format(
                result.uploaded, AbstractStorage.human_readable_size(result.uploaded_bytes), result.skipped))

    def _list_spool(self):
        with os.scandir(self._spool_dir) as entries:
            return sorted(e.name for e in entries if e.is_file(follow_symlinks=False))

    def drain_spool(self):
        """
        Uploads (or skips, if already in storage) the spooled segments in name order and removes each one
        afterwards. Stops at the first segment that fails and reports it in DrainResult.error.
        Raises if the spool cannot be listed or storage cannot be opened. With an empty spool storage is not opened.
        """
        names = self._list_spool()
        if not names:
            return DrainResult(0, 0)

        uploaded = skipped = uploaded_bytes = 0
        with Storage(config=self._storage_config) as storage:
            driver = storage.storage_driver
            for i, name in enumerate(names):
                if self._stop_event.is_set():
                    logging.info('Stop requested, {} file(s) left in the commitlog spool'.format(len(names) - i))
                    break
                path = self._spool_dir / name
                try:
                    # get_object() rather than get_blob(): the next tick is the retry, the long tenacity
                    # retry of get_blob() would only hold up the tick and stop() during a storage outage.
                    blob = driver.get_object('{}/{}'.format(self._folder, name))
                    if blob is not None and driver.file_matches_storage(
                            path, ManifestObject(blob.name, blob.size, blob.hash),
                            self._storage_config.multi_part_upload_threshold, self._enable_md5_checks,
                            self._storage_config.multipart_chunksize):
                        skipped += 1
                    else:
                        size = path.stat().st_size
                        driver.upload_blobs([path], self._folder)
                        uploaded += 1
                        uploaded_bytes += size
                    os.remove(path)
                except Exception as e:
                    logging.exception('Failed to archive commitlog segment {}, {} file(s) left in the spool'.format(
                        name, len(names) - i))
                    return DrainResult(uploaded, skipped, uploaded_bytes,
                                       'Failed to archive commitlog segment {}: {}'.format(name, e))
        return DrainResult(uploaded, skipped, uploaded_bytes)

    def stop(self):
        self._stop_event.set()
        if self.is_alive():
            self.join(timeout=self._interval + 5)
            if self.is_alive():
                logging.warning('Commitlog archiver did not stop within {} seconds'.format(self._interval + 5))

    def status(self):
        try:
            pending, listing_error = len(self._list_spool()), None
        except OSError as e:
            pending, listing_error = None, str(e)
            logging.warning('Cannot read commitlog spool directory {}: {}'.format(self._spool_dir, e))

        with self._lock:
            if listing_error is None:
                self._pending_count = pending
            else:
                self._last_error = listing_error
            last_error = self._last_error
            pending_count = self._pending_count
            last_upload_time = self._last_upload_time

        running = self.is_alive()
        if self.ident is None:
            status = medusa_pb2.StatusType.UNKNOWN
        elif running:
            status = medusa_pb2.StatusType.IN_PROGRESS
        elif self._stop_event.is_set():
            # stopped because stop() was called; an earlier error, if any, is in last_error
            status = medusa_pb2.StatusType.SUCCESS
        else:
            status = medusa_pb2.StatusType.FAILED

        return {
            'running': running,
            'last_upload_time': last_upload_time,
            'pending_count': pending_count,
            'interval_seconds': self._interval,
            'status': status,
            'last_error': last_error,
        }
