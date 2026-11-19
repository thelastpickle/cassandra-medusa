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

"""Exercise the restore-mode shell entrypoint without running Cassandra."""

import json
import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
ENTRYPOINT = REPO_ROOT / 'k8s' / 'docker-entrypoint.sh'
MARKER = '.last-restore'
FAILURE_STATUS = 42

POETRY_STUB = r"""#!/bin/bash
if [[ "$1" != run || "$2" != python ]]; then
    exit 2
fi
if [[ "$3" == -c ]]; then
    exec "$TEST_PYTHON" "${@:3}"
fi
if [[ "$3" != -m || $# -ne 6 || "$5" != /etc/medusa/medusa.ini ]]; then
    exit 2
fi
case "$4" in
    medusa.service.grpc.restore | medusa.service.grpc.pitr_restore) step="${4##*.}" ;;
    *) exit 2 ;;
esac
marker=absent
if [[ -f .last-restore ]]; then
    marker=$(cat .last-restore)
fi
printf '%s|%s|%s\n' "$step" "$6" "$marker" >> calls
if [[ "$step" == "$TEST_FAIL_STEP" ]]; then
    exit 42
fi
"""


@pytest.fixture
def pitr_mapping():
    return {
        'in_place': True,
        'host_map': {'target-0': {'source': ['source-0'], 'seed': False}},
        'pitr': {'target_timestamp': '1767225600', 'node_backups': {'source-0': 'base-0'}},
    }


@pytest.fixture
def restore_container(tmp_path):
    # Keep dispatch and set -e intact while redirecting the mounted-volume marker to the test directory.
    script = tmp_path / ENTRYPOINT.name
    script.write_text(ENTRYPOINT.read_text().replace(f'/var/lib/cassandra/{MARKER}', MARKER))
    stub = tmp_path / 'poetry'
    stub.write_text(POETRY_STUB)
    stub.chmod(0o755)
    calls = tmp_path / 'calls'
    python_paths = [str(REPO_ROOT)]
    if os.environ.get('PYTHONPATH'):
        python_paths.append(os.environ['PYTHONPATH'])

    def run(fail_step='', **restore_env):
        calls.unlink(missing_ok=True)
        env = {
            'PATH': os.pathsep.join((str(tmp_path), os.defpath)),
            'PYTHONPATH': os.pathsep.join(python_paths),
            'MEDUSA_MODE': 'RESTORE',
            'DEBUG_SLEEP': '0',
            'TEST_PYTHON': sys.executable,
            'TEST_FAIL_STEP': fail_step,
            **restore_env,
        }
        result = subprocess.run(
            ['/bin/bash', str(script)], cwd=tmp_path, env=env, capture_output=True, text=True, timeout=15,
        )
        steps = calls.read_text().splitlines() if calls.exists() else []
        return result, steps

    return SimpleNamespace(run=run, marker=tmp_path / MARKER)


def test_docker_entrypoint_has_valid_bash_syntax():
    result = subprocess.run(['/bin/bash', '-n', str(ENTRYPOINT)], capture_output=True, text=True, timeout=15)

    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize(
    'restore_kind', ['standard', 'pitr', 'mapped-remote-pitr', 'unmapped-in-place-pitr', 'escaped-pitr-key'],
)
def test_records_key_after_both_steps_and_still_runs_pitr_step_on_restart(
    restore_container, pitr_mapping, restore_kind,
):
    if restore_kind == 'standard':
        env = {'BACKUP_NAME': 'legacy-backup'}
    else:
        pitr_mapping['in_place'] = restore_kind != 'mapped-remote-pitr'
        mapping_json = json.dumps(pitr_mapping)
        if restore_kind == 'escaped-pitr-key':
            mapping_json = mapping_json.replace('"pitr"', r'"\u0070itr"')
        pod_name = 'unmapped-target' if restore_kind == 'unmapped-in-place-pitr' else 'target-0'
        env = {'RESTORE_MAPPING': mapping_json, 'POD_NAME': pod_name, 'POD_IP': 'source-0'}

    first, first_steps = restore_container.run(RESTORE_KEY='restore-1', **env)

    assert first.returncode == 0, first.stderr
    assert first_steps == ['restore|restore-1|absent', 'pitr_restore|restore-1|absent']
    assert restore_container.marker.read_text().strip() == 'restore-1'

    restart, restart_steps = restore_container.run(RESTORE_KEY='restore-1', **env)

    assert restart.returncode == 0, restart.stderr
    assert restart_steps == ['pitr_restore|restore-1|restore-1']
    assert restore_container.marker.read_text().strip() == 'restore-1'

    new_job, new_job_steps = restore_container.run(RESTORE_KEY='restore-2', **env)

    assert new_job.returncode == 0, new_job.stderr
    assert new_job_steps == ['restore|restore-2|restore-1', 'pitr_restore|restore-2|restore-1']
    assert restore_container.marker.read_text().strip() == 'restore-2'


@pytest.mark.parametrize(
    'failed_step,expected_steps',
    [
        pytest.param('restore', ['restore'], id='sstable-restore-fails'),
        pytest.param('pitr_restore', ['restore', 'pitr_restore'], id='commitlog-staging-fails'),
    ],
)
@pytest.mark.parametrize('previous_key', [None, 'restore-1'], ids=['no-marker', 'previous-restore'])
def test_failed_restore_keeps_marker_and_retries_both_steps(
    restore_container, pitr_mapping, failed_step, expected_steps, previous_key,
):
    if previous_key is not None:
        restore_container.marker.write_text(f'{previous_key}\n')
    marker_contents = previous_key or 'absent'
    env = {'RESTORE_KEY': 'restore-2', 'RESTORE_MAPPING': json.dumps(pitr_mapping), 'POD_NAME': 'target-0'}

    failed, failed_steps = restore_container.run(fail_step=failed_step, **env)

    assert failed.returncode == FAILURE_STATUS, failed.stderr
    assert failed_steps == [f'{step}|restore-2|{marker_contents}' for step in expected_steps]
    if previous_key is None:
        assert not restore_container.marker.exists()
    else:
        assert restore_container.marker.read_text() == f'{previous_key}\n'

    retry, retry_steps = restore_container.run(**env)

    assert retry.returncode == 0, retry.stderr
    assert retry_steps == [f'restore|restore-2|{marker_contents}', f'pitr_restore|restore-2|{marker_contents}']
    assert restore_container.marker.read_text().strip() == 'restore-2'


@pytest.mark.parametrize(
    'restore_env',
    [
        pytest.param({}, id='no-restore-environment'),
        pytest.param({'BACKUP_NAME': 'legacy-backup'}, id='legacy-backup-without-key'),
        pytest.param({'RESTORE_KEY': '', 'RESTORE_MAPPING': '{"in_place": true, "pitr": {}}'}, id='empty-pitr-key'),
        pytest.param(
            {
                'RESTORE_KEY': 'restore-2',
                'RESTORE_MAPPING': json.dumps({
                    'in_place': True,
                    'host_map': {'node-pitr-0': {'source': ['node-0']}, 'pitr': {'source': ['node-1']}},
                }),
            },
            id='pitr-in-host-map-only',
        ),
    ],
)
def test_no_restore_requested_runs_archiving_step_without_writing_marker(restore_container, restore_env):
    result, steps = restore_container.run(**restore_env)

    assert result.returncode == 0, result.stderr
    assert steps == [f'pitr_restore|{restore_env.get("RESTORE_KEY", "")}|absent']
    assert not restore_container.marker.exists()


@pytest.mark.parametrize('previous_key', [None, 'restore-1'], ids=['no-marker', 'previous-restore'])
@pytest.mark.parametrize('pitr', [True, False], ids=['pitr', 'standard'])
def test_unmapped_remote_pod_runs_archiving_step_without_recording_restore(
    restore_container, pitr_mapping, previous_key, pitr,
):
    pitr_mapping['in_place'] = False
    env = {}
    if not pitr:
        pitr_mapping.pop('pitr')
        env['BACKUP_NAME'] = 'legacy-backup'
    if previous_key is not None:
        restore_container.marker.write_text(f'{previous_key}\n')

    result, steps = restore_container.run(
        RESTORE_KEY='restore-2', RESTORE_MAPPING=json.dumps(pitr_mapping), POD_NAME='unmapped-target', **env,
    )

    assert result.returncode == 0, result.stderr
    # PITR-09 already verifies that restore.py can skip an unmapped pod itself.
    marker_contents = previous_key or 'absent'
    archive_step = f'pitr_restore|restore-2|{marker_contents}'
    assert steps in ([archive_step], [f'restore|restore-2|{marker_contents}', archive_step])
    if previous_key is None:
        assert not restore_container.marker.exists()
    else:
        assert restore_container.marker.read_text() == f'{previous_key}\n'


@pytest.mark.parametrize(
    'pitr_json',
    ['null', '[]', '""', 'false', '0', '{'],
    ids=['null', 'array', 'string', 'boolean', 'number', 'malformed-json'],
)
def test_invalid_pitr_mapping_aborts_before_either_step(restore_container, pitr_json):
    restore_container.marker.write_text('restore-1\n')
    mapping = f'{{"in_place": true, "pitr": {pitr_json}}}'

    result, steps = restore_container.run(
        RESTORE_MAPPING=mapping, RESTORE_KEY='restore-2', BACKUP_NAME='legacy-backup',
    )

    assert result.returncode != 0
    assert steps == []
    assert restore_container.marker.read_text() == 'restore-1\n'


def test_operator_timestamp_allows_both_restore_steps(restore_container):
    # Serialized form from the operator's PITR mapping test.
    mapping = '{"in_place":true,"pitr":{"target_timestamp":"2023-11-14T22:13:20Z","node_backups":{"node1":"backup1"}}}'

    result, steps = restore_container.run(RESTORE_MAPPING=mapping, RESTORE_KEY='restore-2', POD_IP='node1')

    assert result.returncode == 0, result.stderr
    assert steps == ['restore|restore-2|absent', 'pitr_restore|restore-2|absent']
    assert restore_container.marker.read_text().strip() == 'restore-2'


@pytest.mark.parametrize(
    'timestamp_fields',
    [
        pytest.param({}, id='missing'),
        pytest.param({'target_timestamp_s': 1767225600}, id='removed-numeric-field'),
        pytest.param({'target_timestamp': None}, id='null'),
        pytest.param({'target_timestamp': 1767225600}, id='not-a-string'),
        pytest.param({'target_timestamp': 'yesterday'}, id='invalid-string'),
        pytest.param({'target_timestamp': 'NaN'}, id='non-finite'),
        pytest.param({'target_timestamp': '253402300800'}, id='out-of-range'),
    ],
)
def test_bad_replay_timestamp_aborts_before_sstable_restore(restore_container, timestamp_fields):
    restore_container.marker.write_text('restore-1\n')
    mapping = json.dumps({
        'in_place': True,
        'pitr': {'node_backups': {'node1': 'backup1'}, **timestamp_fields},
    })

    result, steps = restore_container.run(RESTORE_MAPPING=mapping, RESTORE_KEY='restore-2', POD_IP='node1')

    assert result.returncode != 0
    assert 'target_timestamp' in result.stderr
    assert steps == []
    assert restore_container.marker.read_text() == 'restore-1\n'
