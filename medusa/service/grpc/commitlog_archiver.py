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
from medusa.storage.abstract_storage import ManifestObject
from medusa.utils import evaluate_boolean

DrainResult = collections.namedtuple('DrainResult', ['uploaded', 'skipped', 'failed', 'uploaded_bytes'],
                                     defaults=[0])


class CommitLogArchiver(threading.Thread):
    """
    Uploads the commitlog segments that Cassandra hardlinks into the spool directory.

    Cassandra's ``archive_command`` (for example ``/bin/ln %path <spool>/%name``) creates a hardlink of every
    finished segment in ``commitlog_spool_dir``. A hardlink cannot cross filesystem boundaries (``ln`` fails with
    ``EXDEV``), so the spool directory must be on the same device as the Cassandra commitlog directory. This is
    checked in ``__init__`` and a ``RuntimeError`` is raised if it does not hold.

    Every tick the thread uploads each spooled file to ``<prefix>/<fqdn>/commitlogs/<filename>``, unless a matching
    object is already there (restarts must not upload twice), and removes the hardlink only after the upload or
    the skip was confirmed. Files that fail to upload stay in the spool and are retried on the next tick.

    The ``last_error`` field of ``status()`` is set when ``drain_spool()`` raises (spool directory unreadable or
    storage unreachable). It is cleared by the next tick that completes. Failures of single files are only logged
    and show up as ``pending_count`` staying above zero.
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

        # Storage.upload_blobs() adds the parent folder name to the key for such names, which would
        # silently put the segments under a different key than the one restore and purge look at.
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
            spool_dev = os.stat(self._spool_dir).st_dev
            commitlog_dev = os.stat(commitlog_dir).st_dev
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
        Runs one tick and never raises an Exception.
        Any exception from drain_spool() is logged and stored in last_error. With an empty spool storage is never
        opened.
        """
        try:
            result = self.drain_spool()
        except Exception as e:
            logging.exception('Commitlog archiver tick failed: {}'.format(e), exc_info=True)
            with self._lock:
                self._last_error = str(e)
            return

        with self._lock:
            self._last_error = ''
            self._pending_count = result.failed
            if result.uploaded:
                self._last_upload_time = int(time.time())
        if result.uploaded:
            logging.info('Archived {} commitlog segment(s) ({} bytes), skipped {} already in storage, '
                         '{} failed and left in the spool'.format(result.uploaded, result.uploaded_bytes,
                                                                  result.skipped, result.failed))

    def _list_spool(self):
        with os.scandir(self._spool_dir) as entries:
            return sorted(e.name for e in entries if e.is_file(follow_symlinks=False))

    def drain_spool(self):
        """
        Uploads (or skips, if already in storage) every file of the spool and removes it afterwards.
        Raises if the spool cannot be listed or storage cannot be opened.
        """
        names = self._list_spool()
        if not names:
            return DrainResult(0, 0, 0)

        uploaded = skipped = failed = uploaded_bytes = 0
        with Storage(config=self._storage_config) as storage:
            driver = storage.storage_driver
            for i, name in enumerate(names):
                if self._stop_event.is_set():
                    logging.info('Stop requested, {} file(s) left in the commitlog spool'.format(len(names) - i))
                    break
                path = self._spool_dir / name
                try:
                    blob = driver.get_blob('{}/{}'.format(self._folder, name))
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
                    logging.exception('Failed to archive commitlog segment {}: {}'.format(name, e), exc_info=True)
                    failed += 1
        return DrainResult(uploaded, skipped, failed, uploaded_bytes)

    def stop(self):
        self._stop_event.set()
        if self.is_alive():
            self.join(timeout=self._interval + 5)
            if self.is_alive():
                logging.warning('Commitlog archiver did not stop within {} seconds'.format(self._interval + 5))

    def status(self):
        try:
            pending = len(self._list_spool())
            listing_error = None
        except OSError as e:
            pending = None
            listing_error = str(e)
            logging.warning('Cannot read commitlog spool directory: {}'.format(e))

        with self._lock:
            if pending is not None:
                self._pending_count = pending
            else:
                self._last_error = listing_error if listing_error is not None else ""
            last_error = self._last_error
            pending_count = self._pending_count
            last_upload_time = self._last_upload_time

        running = self.is_alive()
        if self.ident is None:
            status = medusa_pb2.StatusType.UNKNOWN
        elif running:
            status = medusa_pb2.StatusType.IN_PROGRESS
        elif self._stop_event.is_set() and not last_error:
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
