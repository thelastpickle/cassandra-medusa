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
"""
Pure decision logic for point-in-time restore (PITR).

This module deliberately has no gRPC or SSH dependencies, so that it can be shared by the coordinator RPCs and by the
node initContainers, and unit tested in isolation.
"""
import datetime
import logging
import math
import typing as t

from decimal import Decimal, ROUND_FLOOR
from pathlib import Path, PurePosixPath

from medusa.storage import Storage
from medusa.storage.abstract_storage import AbstractBlob
from medusa.storage.node_backup import NodeBackup

COMMITLOGS_FOLDER = 'commitlogs'
RESTORE_COMMAND = '/bin/cp -f %from %to'

# NodeBackup.snapshot_time is the snapshot directory mtime truncated to whole seconds (see
# Cassandra.Snapshot.get_snapshot_time), so the real capture time lies in [snapshot_time, snapshot_time + 1).
# Commitlog replay cannot undo writes that are already in the snapshot, so a backup is only safe as a base when
# even the latest possible capture time, snapshot_time + SNAPSHOT_TIME_RESOLUTION_S, is not after the target.
SNAPSHOT_TIME_RESOLUTION_S = 1


def _valid_snapshot_time(node_backup: NodeBackup) -> t.Optional[t.Union[int, float]]:
    """
    Returns the snapshot_time of the backup, or None if it is missing or invalid.
    Never falls back to any other timestamp (e.g. `started`): see design doc 4.5.
    """
    try:
        snapshot_time = node_backup.snapshot_time
    except (ValueError, TypeError):
        logging.warning('Backup {} of {} has an invalid snapshot_time, ignoring it'.format(
            node_backup.name, node_backup.fqdn))
        return None
    if isinstance(snapshot_time, bool) or not isinstance(snapshot_time, (int, float)):
        return None
    if not math.isfinite(snapshot_time):
        return None
    return snapshot_time


def _backups_with_snapshot_time(storage, fqdn: str) -> t.List[t.Tuple[t.Union[int, float], NodeBackup]]:
    """
    Lists the backups of fqdn that have a usable snapshot_time, paired with that snapshot_time.
    """
    result = []
    for node_backup in storage.list_node_backups(fqdn=fqdn):
        # list_node_backups() matches the fqdn by substring, so make sure we only keep this node's backups
        if node_backup.fqdn != fqdn:
            continue

        snapshot_time = _valid_snapshot_time(node_backup)
        if snapshot_time is None:
            continue
        result.append((snapshot_time, node_backup))
    return result


def select_node_base_snapshot(storage, fqdn: str, target_timestamp_s: float) -> NodeBackup:
    """
    Returns the node backup of fqdn with the latest snapshot_time such that
    snapshot_time + SNAPSHOT_TIME_RESOLUTION_S <= target_timestamp_s.

    snapshot_time is truncated to whole seconds, so the snapshot may really have been captured up to
    SNAPSHOT_TIME_RESOLUTION_S later. A backup whose snapshot could have been captured after the target is ineligible,
    because its SSTables could already contain writes past the target, which commitlog replay cannot undo.

    Backups without a snapshot_time (pre-PITR backups) are ineligible. We never fall back to `started`: it can be
    earlier than the target while the snapshot itself was finished after it, which would silently restore data
    from past the target time.

    :raises ValueError: if there is no eligible backup
    """
    eligible = [
        (snapshot_time, node_backup)
        for snapshot_time, node_backup in _backups_with_snapshot_time(storage, fqdn)
        if snapshot_time + SNAPSHOT_TIME_RESOLUTION_S <= target_timestamp_s
    ]
    if not eligible:
        raise ValueError(
            'No backup of {} with a snapshot_time at least {}s before {} was found'.format(
                fqdn, SNAPSHOT_TIME_RESOLUTION_S, target_timestamp_s)
        )
    return max(eligible, key=lambda pair: pair[0])[1]


def find_next_backup(storage, fqdn: str, base_backup: NodeBackup) -> t.Optional[NodeBackup]:
    """
    Returns the backup of fqdn with the smallest snapshot_time > base_backup.snapshot_time, or None if there is none.
    Backups without a snapshot_time are skipped.

    :raises ValueError: if the base backup itself has no usable snapshot_time
    """
    base_snapshot_time = _valid_snapshot_time(base_backup)
    if base_snapshot_time is None:
        raise ValueError('Backup {} of {} has no valid snapshot_time'.format(base_backup.name, fqdn))

    later = [
        (snapshot_time, node_backup)
        for snapshot_time, node_backup in _backups_with_snapshot_time(storage, fqdn)
        if snapshot_time > base_snapshot_time
    ]
    if not later:
        return None
    return min(later, key=lambda pair: pair[0])[1]


def _last_modified_s(storage: Storage, blob: AbstractBlob) -> float:
    """
    Epoch seconds of the blob's last modification. Depending on the driver, last_modified is a datetime or a number.
    """
    last_modified = storage.storage_driver.get_object_datetime(blob)
    if isinstance(last_modified, (int, float)):
        return float(last_modified)
    return last_modified.timestamp()


def find_commitlog_segments(storage: Storage, prefix_path: str, fqdn: str,
                            after_snapshot_time_s: float, upper_bound_s: float,
                            safety_margin_s: float = 30.0) -> t.List[AbstractBlob]:
    """
    Lists the archived commitlog segments of fqdn that were last modified in
    [after_snapshot_time_s - safety_margin_s, upper_bound_s] (both ends inclusive), sorted ascending by filename.

    Callers pass base_backup.started as after_snapshot_time_s (design doc 4.5).
    """
    # same layout the CommitLogArchiver uploads to: <prefix>/<fqdn>/commitlogs/<filename>
    folder = '{}/'.format(Path(prefix_path or '.') / fqdn / COMMITLOGS_FOLDER)
    lower_bound_s = after_snapshot_time_s - safety_margin_s

    segments = [
        blob for blob in storage.storage_driver.list_objects(path=folder)
        if lower_bound_s <= _last_modified_s(storage, blob) <= upper_bound_s
    ]
    return sorted(segments, key=lambda blob: PurePosixPath(blob.name).name)


def download_commitlog_segments(storage: Storage, segments: t.List[AbstractBlob],
                                destination_dir: t.Union[Path, str]) -> None:
    """
    Downloads the commitlog segments into destination_dir, overwriting the files with the same name.
    """
    if not segments:
        return
    destination = Path(destination_dir)
    destination.mkdir(parents=True, exist_ok=True)
    storage.storage_driver.download_blobs([blob.name for blob in segments], str(destination))


def _format_restore_point_in_time(target_timestamp_s: float) -> str:
    """
    Formats an epoch timestamp as `yyyy:MM:dd HH:mm:ss.SSS` in UTC, truncating (never rounding) to milliseconds,
    so that the restore point is never later than requested. DSE rejects microseconds, milliseconds work everywhere.
    """
    if not math.isfinite(target_timestamp_s):
        raise ValueError('Invalid target timestamp: {}'.format(target_timestamp_s))
    # repr() gives the shortest decimal that round-trips the float: 1767225600.001 stays .001, not .000999
    total_ms = int((Decimal(repr(float(target_timestamp_s))) * 1000).to_integral_value(rounding=ROUND_FLOOR))
    seconds, millis = divmod(total_ms, 1000)
    moment = datetime.datetime.fromtimestamp(seconds, tz=datetime.timezone.utc)
    return '{}.{:03d}'.format(moment.strftime('%Y:%m:%d %H:%M:%S'), millis)


def generate_commitlog_archiving_properties(archive_command: str,
                                            restore_directories: t.Optional[str] = None,
                                            target_timestamp_s: t.Optional[float] = None) -> str:
    """
    Returns the content of commitlog_archiving.properties.

    archive_command is always written. restore_directories, restore_command and restore_point_in_time are written
    only when both restore_directories and target_timestamp_s are given. restore_command is mandatory whenever
    restore_directories is set: without it Cassandra, DSE and HCD hit a NullPointerException in
    CommitLogArchiver.maybeRestoreArchive and the node does not start.
    """
    lines = ['archive_command={}'.format(archive_command)]
    if restore_directories and target_timestamp_s is not None:
        lines.append('restore_directories={}'.format(restore_directories))
        lines.append('restore_command={}'.format(RESTORE_COMMAND))
        lines.append('restore_point_in_time={}'.format(_format_restore_point_in_time(target_timestamp_s)))
    return '\n'.join(lines) + '\n'
