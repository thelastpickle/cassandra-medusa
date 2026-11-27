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

    monkeypatch.setattr(entrypoint, 'LAST_RESTORE_DIR', files.marker.parent)
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


def test_disabled_pitr_leaves_files_untouched(entrypoint, files, restore_io, caplog):
    write_config(files, enabled='false')
    files.staging.mkdir()
    old_segment = files.staging / 'old.log'
    old_segment.write_bytes(b'previous segment')

    with caplog.at_level(logging.INFO):
        entrypoint.main()

    assert any(record.levelno == logging.INFO and 'pitr' in record.getMessage().lower() for record in caplog.records)
    assert old_segment.read_bytes() == b'previous segment'
    assert not files.properties.exists()
    assert not files.marker.exists()
    assert not files.spool.exists()
    assert_no_storage_access(restore_io)
    restore_io.generate_properties.assert_not_called()


@pytest.mark.parametrize('pitr_section', [True, False], ids=['disabled', 'section-absent'])
@pytest.mark.parametrize('restore_state', ['completed', 'unmapped-pod', 'no-restore-key'])
def test_disabled_pitr_without_pending_restore_leaves_files_untouched(
    entrypoint, files, restore_io, mapping, monkeypatch, pitr_section, restore_state,
):
    write_config(files, enabled='false')
    if not pitr_section:
        files.ini.write_text(files.ini.read_text().partition('[pitr]')[0])
    marker_value = f'{RESTORE_KEY}\n' if restore_state == 'completed' else 'restore-1\n'
    files.marker.write_text(marker_value)
    files.staging.mkdir()
    old_segment = files.staging / 'old.log'
    old_segment.write_bytes(b'previous segment')
    files.properties.write_text('previous properties\n')
    if restore_state == 'unmapped-pod':
        mapping['in_place'] = False
        monkeypatch.setenv('POD_NAME', 'unmapped')
    elif restore_state == 'no-restore-key':
        monkeypatch.setattr(sys, 'argv', [ENTRYPOINT, str(files.ini), ''])
    monkeypatch.setenv('RESTORE_MAPPING', json.dumps(mapping))

    entrypoint.main()

    assert files.marker.read_text() == marker_value
    assert old_segment.read_bytes() == b'previous segment'
    assert files.properties.read_text() == 'previous properties\n'
    assert not files.spool.exists()
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
    assert files.staging.stat().st_mode & 0o777 == 0o777
    assert read_properties(files.properties) == {
        'archive_command': archive_command(files),
        'restore_directories': str(files.staging),
        'restore_command': '/bin/mv -f %from %to',
        'restore_point_in_time': '2026:01:01 00:00:00.123',
    }
    assert files.spool.stat().st_mode & 0o777 == 0o777
    assert not files.marker.exists()


@pytest.mark.parametrize('pitr_section', [True, False], ids=['explicitly-disabled', 'section-absent'])
@pytest.mark.parametrize('in_place', [True, False], ids=['in-place', 'out-of-place'])
def test_pending_restore_writes_replay_only_properties(
    entrypoint, files, restore_io, mapping, monkeypatch, pitr_section, in_place,
):
    write_config(files, enabled='false')
    staging_dir = str(files.staging)
    if not pitr_section:
        files.ini.write_text(files.ini.read_text().partition('[pitr]')[0])
        staging_dir = '/var/lib/cassandra/medusa_commitlog_staging'
        path_in_test_volume = entrypoint.Path
        monkeypatch.setattr(
            entrypoint, 'Path', lambda path: files.staging if path == staging_dir else path_in_test_volume(path),
        )

    mapping['in_place'] = in_place
    monkeypatch.setenv('POD_NAME', 'target-0')
    monkeypatch.setenv('RESTORE_MAPPING', json.dumps(mapping))
    restore_io.find_next.return_value = None
    segments = [SimpleNamespace(name='CommitLog-7-100.log')]
    restore_io.find_segments.return_value = segments

    entrypoint.main()

    restore_io.storage.get_node_backup.assert_called_once_with(fqdn='source-node.example', name='mapped-backup')
    restore_io.download.assert_called_once_with(restore_io.storage, segments, staging_dir)
    assert files.staging.is_dir()
    assert read_properties(files.properties) == {
        'restore_directories': staging_dir,
        'restore_command': '/bin/mv -f %from %to',
        'restore_point_in_time': '2026:01:01 00:00:00.123',
    }


def test_pending_restore_with_missing_cutoff_keeps_staging(entrypoint, files, restore_io, mapping, monkeypatch):
    write_config(files, enabled='false')
    mapping['pitr'].pop('target_timestamp')
    monkeypatch.setenv('RESTORE_MAPPING', json.dumps(mapping))
    files.staging.mkdir()
    old_segment = files.staging / 'old.log'
    old_segment.touch()

    with pytest.raises(ValueError, match='target_timestamp'):
        entrypoint.main()

    assert old_segment.is_file()
    assert not files.properties.exists()
    assert not files.marker.exists()
    assert_no_storage_access(restore_io)
    restore_io.generate_properties.assert_not_called()


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
    assert files.spool.is_dir()
    assert (files.marker.read_text() if files.marker.exists() else None) == marker
    assert_no_storage_access(restore_io)


@pytest.mark.parametrize('restore_requested', [False, True], ids=['normal-startup', 'restore'])
def test_existing_spool_is_kept(entrypoint, files, restore_io, mapping, monkeypatch, restore_requested):
    if restore_requested:
        monkeypatch.setenv('POD_NAME', 'target-0')
        monkeypatch.setenv('RESTORE_MAPPING', json.dumps(mapping))
        restore_io.find_next.return_value = None
    files.spool.mkdir()
    pending = files.spool / 'CommitLog-7-99.log'
    pending.write_bytes(b'not yet uploaded')

    entrypoint.main()

    assert pending.read_bytes() == b'not yet uploaded'
    assert read_properties(files.properties)['archive_command'] == archive_command(files)
    assert restore_io.download.called == restore_requested


@pytest.mark.parametrize(
    'marker_key', [None, 'restore-1', RESTORE_KEY], ids=['absent', 'previous-job', 'completed-job'],
)
def test_entrypoint_uses_marker_path_from_argument(entrypoint, files, restore_io, mapping, monkeypatch, marker_key):
    custom_marker = files.ini.parent / 'custom restore marker'
    marker_value = None if marker_key is None else f'  {marker_key}\n'
    if marker_value is not None:
        custom_marker.write_text(marker_value)
    default_value = 'restore-1\n' if marker_key == RESTORE_KEY else f'{RESTORE_KEY}\n'
    files.marker.write_text(default_value)
    monkeypatch.setenv('RESTORE_MAPPING', json.dumps(mapping))
    monkeypatch.setenv('POD_IP', '192.0.2.9')
    monkeypatch.setattr(sys, 'argv', [ENTRYPOINT, str(files.ini), RESTORE_KEY, str(custom_marker)])
    restore_io.find_next.return_value = None
    restore_io.find_segments.return_value = []

    entrypoint.main()

    if marker_key == RESTORE_KEY:
        assert_no_storage_access(restore_io)
        assert read_properties(files.properties) == {'archive_command': archive_command(files)}
    else:
        restore_io.storage.get_node_backup.assert_called_once_with(fqdn='192.0.2.9', name='ip-backup')
        assert 'restore_point_in_time' in read_properties(files.properties)
    assert (custom_marker.read_text() if custom_marker.exists() else None) == marker_value
    assert files.marker.read_text() == default_value


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

    with patch.object(restore, 'configure_console_logging'), pytest.raises(ValueError, match='target_timestamp'):
        runpy.run_path(entrypoint.__file__, run_name='__main__')

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


@pytest.mark.parametrize('kind', ['absolute', 'traversal', 'symlink'])
def test_marker_outside_cassandra_directory_is_rejected(entrypoint, tmp_path, monkeypatch, kind):
    allowed = tmp_path / 'cassandra'
    allowed.mkdir()
    outside = tmp_path / 'outside'
    outside.write_text(RESTORE_KEY)
    monkeypatch.setattr(entrypoint, 'LAST_RESTORE_DIR', allowed)
    marker = outside if kind == 'absolute' else allowed / '..' / outside.name
    if kind == 'symlink':
        marker = allowed / '.last-restore'
        marker.symlink_to(outside)

    with pytest.raises(ValueError, match='Restore marker must be directly inside'):
        entrypoint.read_last_restore_key(marker)
