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

"""Unit tests for per-node PITR staging and Cassandra archiving configuration."""

import json
import logging
import runpy
import sys
from importlib import import_module
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from medusa import pitr_utils as restore_logic
from medusa.service.grpc import restore

ENTRYPOINT = 'medusa.service.grpc.pitr_restore'
RESTORE_KEY = 'restore-2'
TARGET_TIMESTAMP_S = 1767225600.123
BASE_STARTED_S = 1767225000
NEXT_SNAPSHOT_S = 1767226200
NOW_S = 1767226400.5
GRACE_PERIOD_S = 90
LAST_RESTORE = Path('/var/lib/cassandra/.last-restore')


@pytest.fixture
def entrypoint():
    return import_module(ENTRYPOINT)


@pytest.fixture
def mapping():
    return {
        'in_place': True,
        'host_map': {
            'target-0': {'source': ['source-node.example', 'other-node.example'], 'seed': False},
        },
        'pitr': {
            'target_timestamp': str(TARGET_TIMESTAMP_S),
            'node_backups': {
                'source-node.example': 'mapped-backup',
                '192.0.2.9': 'ip-backup',
                'configured-node.example': 'configured-backup',
            },
        },
    }


def write_config(files, enabled):
    files.ini.write_text(
        '[storage]\n'
        'storage_provider = local\n'
        'bucket_name = backups\n'
        'prefix = restore-tests\n'
        'fqdn = configured-node.example\n'
        '[cassandra]\n'
        f'config_file = {files.properties.parent / "cassandra.yaml"}\n'
        '[pitr]\n'
        f'enabled = {enabled}\n'
        f'commitlog_spool_dir = {files.spool}\n'
        f'commitlog_staging_dir = {files.staging}\n'
        f'commitlog_restore_grace_period_seconds = {GRACE_PERIOD_S}\n'
    )


@pytest.fixture
def files(tmp_path, monkeypatch, entrypoint):
    cassandra_config = tmp_path / 'cassandra-conf'
    cassandra_config.mkdir()
    files = SimpleNamespace(
        ini=tmp_path / 'medusa.ini',
        staging=tmp_path / 'staging',
        spool=tmp_path / 'spool',
        marker=tmp_path / '.last-restore',
        properties=cassandra_config / 'commitlog_archiving.properties',
    )
    write_config(files, enabled='true')

    for name in ('BACKUP_NAME', 'POD_NAME', 'POD_IP', 'RESTORE_KEY', 'RESTORE_MAPPING'):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(sys, 'argv', [ENTRYPOINT, str(files.ini), RESTORE_KEY])

    def path_in_test_volume(*parts):
        path = Path(*parts)
        return files.marker if path == LAST_RESTORE else path

    monkeypatch.setattr(entrypoint, 'Path', path_in_test_volume)
    monkeypatch.setattr(entrypoint, 'configure_console_logging', lambda config: None)
    return files


@pytest.fixture
def restore_io(entrypoint):
    with (
        patch.object(entrypoint, 'Storage', autospec=True) as storage_cls,
        patch.object(entrypoint, 'find_next_backup', autospec=True) as find_next,
        patch.object(entrypoint, 'find_commitlog_segments', autospec=True) as find_segments,
        patch.object(entrypoint, 'download_commitlog_segments', autospec=True) as download,
        patch.object(
            entrypoint, 'generate_commitlog_archiving_properties', autospec=True,
            side_effect=restore_logic.generate_commitlog_archiving_properties,
        ) as generate_properties,
    ):
        storage_context = storage_cls.return_value
        storage = storage_context.__enter__.return_value
        base = SimpleNamespace(started=BASE_STARTED_S, snapshot_time=BASE_STARTED_S + 30)
        storage.get_node_backup.return_value = base
        yield SimpleNamespace(
            storage_cls=storage_cls,
            storage_context=storage_context,
            storage=storage,
            base=base,
            find_next=find_next,
            find_segments=find_segments,
            download=download,
            generate_properties=generate_properties,
        )


def archive_command(files):
    return f'/bin/ln %path {files.spool}/%name'


def read_properties(path):
    return dict(line.split('=', 1) for line in path.read_text().splitlines() if line and not line.startswith('#'))


def assert_no_storage_access(restore_io):
    restore_io.storage_cls.assert_not_called()
    restore_io.find_next.assert_not_called()
    restore_io.find_segments.assert_not_called()
    restore_io.download.assert_not_called()


def test_entrypoint_reuses_restore_mapping_helpers(entrypoint):
    assert entrypoint.resolve_node_address is restore.resolve_node_address
    assert entrypoint.get_pitr_mapping is restore.get_pitr_mapping


@pytest.mark.parametrize(
    'pitr_requested,log_level',
    [pytest.param(False, logging.INFO, id='standard-startup'), pytest.param(True, logging.ERROR, id='pitr-request')],
)
def test_disabled_pitr_leaves_files_untouched(
    entrypoint, files, restore_io, monkeypatch, caplog, pitr_requested, log_level,
):
    write_config(files, enabled='false')
    files.staging.mkdir()
    old_segment = files.staging / 'old.log'
    old_segment.write_bytes(b'previous segment')
    if pitr_requested:
        monkeypatch.setenv('RESTORE_MAPPING', json.dumps({'in_place': True, 'pitr': {}}))

    with caplog.at_level(logging.INFO):
        if pitr_requested:
            with pytest.raises(SystemExit) as error:
                entrypoint.main()
        else:
            entrypoint.main()

    if pitr_requested:
        assert error.value.code == 1
    assert any(record.levelno == log_level and 'pitr' in record.getMessage().lower() for record in caplog.records)
    assert old_segment.read_bytes() == b'previous segment'
    assert not files.properties.exists()
    assert not files.marker.exists()
    assert_no_storage_access(restore_io)
    restore_io.generate_properties.assert_not_called()


@pytest.mark.parametrize(
    'next_snapshot,expected_upper_bound',
    [
        pytest.param(NEXT_SNAPSHOT_S, NEXT_SNAPSHOT_S + GRACE_PERIOD_S, id='next-snapshot-plus-grace'),
        pytest.param(None, NOW_S, id='current-time-without-next-snapshot'),
    ],
)
@pytest.mark.parametrize('in_place', [True, False], ids=['in-place', 'mapped-remote-pod'])
def test_active_restore_stages_segments_and_writes_replay_properties(
    entrypoint, files, restore_io, mapping, monkeypatch, next_snapshot, expected_upper_bound, in_place,
):
    mapping['in_place'] = in_place
    monkeypatch.setenv('POD_NAME', 'target-0')
    monkeypatch.setenv('POD_IP', '192.0.2.9')
    monkeypatch.setenv('BACKUP_NAME', 'legacy-backup')
    monkeypatch.setenv('RESTORE_MAPPING', json.dumps(mapping))
    monkeypatch.setattr(entrypoint, 'time', SimpleNamespace(time=lambda: NOW_S))
    config = restore.create_config(files.ini)
    restore_io.find_next.return_value = (
        SimpleNamespace(snapshot_time=next_snapshot, started=next_snapshot - 30)
        if next_snapshot is not None else None
    )
    segments = [SimpleNamespace(name='CommitLog-7-100.log'), SimpleNamespace(name='CommitLog-7-101.log')]
    restore_io.find_segments.return_value = segments
    stale_dir = files.staging / 'previous-attempt'
    stale_dir.mkdir(parents=True)
    (stale_dir / 'partial.log').touch()

    def stage_segments(storage, segments, destination_dir):
        destination = Path(destination_dir)
        assert destination.is_dir()
        assert list(destination.iterdir()) == []
        for segment in segments:
            (destination / segment.name).touch()

    restore_io.download.side_effect = stage_segments

    entrypoint.main()

    restore_io.storage_cls.assert_called_once_with(config=config.storage)
    restore_io.storage_context.__enter__.assert_called_once_with()
    restore_io.storage_context.__exit__.assert_called_once_with(None, None, None)
    restore_io.storage.get_node_backup.assert_called_once_with(fqdn='source-node.example', name='mapped-backup')
    restore_io.find_next.assert_called_once_with(restore_io.storage, 'source-node.example', restore_io.base)
    restore_io.find_segments.assert_called_once_with(
        restore_io.storage, 'restore-tests', 'source-node.example', BASE_STARTED_S, upper_bound_s=expected_upper_bound,
    )
    restore_io.download.assert_called_once_with(restore_io.storage, segments, str(files.staging))
    assert {file.name for file in files.staging.iterdir()} == {segment.name for segment in segments}
    assert read_properties(files.properties) == {
        'archive_command': archive_command(files),
        'restore_directories': str(files.staging),
        'restore_command': '/bin/cp -f %from %to',
        'restore_point_in_time': '2026:01:01 00:00:00.123',
    }
    assert not files.marker.exists()


@pytest.mark.parametrize(
    'pod_name,pod_ip,expected_address,expected_backup',
    [
        pytest.param('unmapped', '192.0.2.9', '192.0.2.9', 'ip-backup', id='pod-ip'),
        pytest.param(None, None, 'configured-node.example', 'configured-backup', id='configured-fqdn'),
    ],
)
def test_new_restore_uses_address_and_restore_key_fallbacks(
    entrypoint, files, restore_io, mapping, monkeypatch, pod_name, pod_ip, expected_address, expected_backup,
):
    if pod_name is not None:
        monkeypatch.setenv('POD_NAME', pod_name)
    if pod_ip is not None:
        monkeypatch.setenv('POD_IP', pod_ip)
    monkeypatch.setenv('RESTORE_MAPPING', json.dumps(mapping))
    monkeypatch.setenv('RESTORE_KEY', RESTORE_KEY)
    monkeypatch.setattr(sys, 'argv', [ENTRYPOINT, str(files.ini)])
    files.marker.write_text('restore-1\n')
    restore_io.find_next.return_value = SimpleNamespace(snapshot_time=NEXT_SNAPSHOT_S)

    entrypoint.main()

    restore_io.storage.get_node_backup.assert_called_once_with(fqdn=expected_address, name=expected_backup)
    restore_io.download.assert_called_once()
    assert files.marker.read_text() == 'restore-1\n'


@pytest.mark.parametrize(
    'restore_mapping,marker',
    [
        pytest.param('pitr', f'  {RESTORE_KEY}\n', id='already-restored'),
        pytest.param('standard', None, id='standard-restore'),
        pytest.param('absent', None, id='normal-startup'),
        pytest.param('remote', 'restore-1\n', id='unmapped-remote-pod'),
    ],
)
def test_skipped_replay_replaces_stale_properties_and_empties_staging(
    entrypoint, files, restore_io, mapping, monkeypatch, restore_mapping, marker,
):
    if restore_mapping == 'standard':
        mapping.pop('pitr')
    if restore_mapping == 'remote':
        mapping['in_place'] = False
        monkeypatch.setenv('POD_NAME', 'unmapped')
        monkeypatch.setenv('POD_IP', '192.0.2.9')
    if restore_mapping != 'absent':
        monkeypatch.setenv('RESTORE_MAPPING', json.dumps(mapping))
    if marker is not None:
        files.marker.write_text(marker)
    files.staging.mkdir()
    (files.staging / 'old.log').touch()
    files.properties.write_text(
        'archive_command=old\nrestore_directories=stale\nrestore_command=old\nrestore_point_in_time=stale\n'
    )

    entrypoint.main()

    assert files.staging.is_dir()
    assert list(files.staging.iterdir()) == []
    assert read_properties(files.properties) == {'archive_command': archive_command(files)}
    assert (files.marker.read_text() if files.marker.exists() else None) == marker
    assert_no_storage_access(restore_io)


@pytest.mark.parametrize('key_source', ['empty-argument', 'absent'])
def test_empty_restore_key_is_boolean_false(
    entrypoint, files, restore_io, mapping, monkeypatch, key_source,
):
    monkeypatch.setenv('RESTORE_MAPPING', json.dumps(mapping))
    argv = [ENTRYPOINT, str(files.ini)]
    if key_source == 'empty-argument':
        monkeypatch.setenv('RESTORE_KEY', RESTORE_KEY)
        argv.append('')
    monkeypatch.setattr(sys, 'argv', argv)
    decisions = []

    def capture_restore_decision(frame, event, _):
        if event == 'return' and frame.f_code is entrypoint.main.__code__:
            decisions.append(frame.f_locals.get('is_pitr_restore'))

    # The ticket requires an actual bool for this local decision. Both False and '' skip replay.
    previous_profile = sys.getprofile()
    sys.setprofile(capture_restore_decision)
    try:
        entrypoint.main()
    finally:
        sys.setprofile(previous_profile)

    assert len(decisions) == 1
    assert decisions[0] is False
    assert_no_storage_access(restore_io)
    assert read_properties(files.properties) == {'archive_command': archive_command(files)}
    assert not files.marker.exists()


@pytest.mark.parametrize(
    'pitr',
    [None, [], '', True, 1],
    ids=['null', 'array', 'string', 'boolean', 'number'],
)
def test_non_object_pitr_block_fails_before_restore(entrypoint, files, restore_io, monkeypatch, pitr):
    monkeypatch.setenv('RESTORE_MAPPING', json.dumps({'in_place': True, 'pitr': pitr}))
    files.staging.mkdir()
    old_segment = files.staging / 'old.log'
    old_segment.touch()

    with pytest.raises(ValueError, match="The 'pitr' field in RESTORE_MAPPING must be a JSON object"):
        entrypoint.main()

    assert old_segment.is_file()
    assert not files.properties.exists()
    assert not files.marker.exists()
    assert_no_storage_access(restore_io)


def test_module_runs_as_entrypoint(entrypoint, files, monkeypatch):
    write_config(files, enabled='false')
    monkeypatch.setenv('RESTORE_MAPPING', json.dumps({'in_place': True, 'pitr': {}}))

    with patch.object(restore, 'configure_console_logging'), pytest.raises(SystemExit) as error:
        runpy.run_path(entrypoint.__file__, run_name='__main__')

    assert error.value.code == 1
    assert not files.properties.exists()


@pytest.mark.parametrize(
    'timestamp,expected_cutoff',
    [
        pytest.param('2023-11-14T22:13:20Z', '2023:11:14 22:13:20.000', id='operator-rfc3339'),
        pytest.param('2026-01-01T01:00:00.123999999+01:00', '2026:01:01 00:00:00.123', id='offset-with-fraction'),
        pytest.param('1767225600.123999999', '2026:01:01 00:00:00.123', id='epoch-string'),
    ],
)
def test_active_restore_replays_to_operator_timestamp(
    entrypoint, files, restore_io, monkeypatch, timestamp, expected_cutoff,
):
    mapping = {
        'in_place': True,
        'pitr': {'target_timestamp': timestamp, 'node_backups': {'node1': 'backup1'}},
    }
    monkeypatch.setenv('RESTORE_MAPPING', json.dumps(mapping))
    monkeypatch.setenv('POD_IP', 'node1')
    restore_io.find_next.return_value = None
    restore_io.find_segments.return_value = []

    entrypoint.main()

    restore_io.storage.get_node_backup.assert_called_once_with(fqdn='node1', name='backup1')
    assert read_properties(files.properties)['restore_point_in_time'] == expected_cutoff
