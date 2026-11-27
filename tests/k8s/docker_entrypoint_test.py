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

"""Exercise the shell entrypoint and the image entrypoint without running Cassandra or Medusa."""

import json
import os
import re
import signal
import subprocess
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
ENTRYPOINT = REPO_ROOT / 'k8s' / 'docker-entrypoint.sh'
DOCKERFILE = REPO_ROOT / 'k8s' / 'Dockerfile'
MARKER = '.last-restore'
FAILURE_STATUS = 42

POETRY_STUB = r"""#!/bin/bash
if [[ "$1" != run || "$2" != python ]]; then
    exit 2
fi
if [[ "$3" == -c ]]; then
    exec "$TEST_PYTHON" "${@:3}"
fi
if [[ "$3" != -m || "$5" != "$TEST_CONFIG_FILE" ]]; then
    exit 2
fi
case "$4" in
    medusa.service.grpc.restore)
        [[ $# -eq 6 ]] || exit 2
        step=restore ;;
    medusa.service.grpc.pitr_restore)
        [[ $# -eq 7 && "$7" == "$TEST_MARKER_FILE" ]] || exit 2
        step=pitr_restore ;;
    *) exit 2 ;;
esac
marker=absent
if [[ -f "$TEST_MARKER_FILE" ]]; then
    marker=$(cat "$TEST_MARKER_FILE")
fi
printf '%s|%s|%s\n' "$step" "$6" "$marker" >> calls
if [[ "$step" == "$TEST_FAIL_STEP" ]]; then
    exit 42
fi
if [[ "$step" == restore ]]; then
    printf 'base snapshot data\n' > "$TEST_DATA_FILE"
elif [[ "$TEST_RUN_PITR" == true ]]; then
    exec "$TEST_PYTHON" -c '
import sys
from medusa.service.grpc.pitr_restore import main
sys.argv = sys.argv[1:]
main()
' "${@:4}"
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
    # Keep dispatch and set -e intact while redirecting mounted-volume paths to the test directory.
    marker = tmp_path / 'restore marker'
    staging = tmp_path / 'staging'
    properties = tmp_path / 'commitlog_archiving.properties'
    config = tmp_path / 'medusa.ini'
    config.write_text(
        '[storage]\n'
        'storage_provider = local\n'
        'bucket_name = backups\n'
        'fqdn = source-0\n'
        '[cassandra]\n'
        f'config_file = {tmp_path / "cassandra.yaml"}\n'
        '[pitr]\n'
        'enabled = true\n'
        f'commitlog_staging_dir = {staging}\n'
        f'commitlog_spool_dir = {tmp_path / "spool"}\n'
    )
    data = tmp_path / 'table-data.db'
    data.write_bytes(b'existing table data')
    script = tmp_path / ENTRYPOINT.name
    script.write_text(
        ENTRYPOINT.read_text().replace(f'/var/lib/cassandra/{MARKER}', str(marker))
        .replace('/etc/medusa/medusa.ini', str(config))
    )
    stub = tmp_path / 'poetry'
    stub.write_text(POETRY_STUB)
    stub.chmod(0o755)
    calls = tmp_path / 'calls'
    # Redirect the trusted Cassandra directory in the subprocess, too.
    (tmp_path / 'sitecustomize.py').write_text(
        'from pathlib import Path\n'
        'from medusa.service.grpc import pitr_restore\n'
        f'pitr_restore.LAST_RESTORE_DIR = Path({str(tmp_path)!r})\n'
    )
    python_paths = [str(tmp_path), str(REPO_ROOT)]
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
            'TEST_CONFIG_FILE': str(config),
            'TEST_DATA_FILE': str(data),
            'TEST_MARKER_FILE': str(marker),
            **restore_env,
        }
        result = subprocess.run(
            ['/bin/bash', str(script)], cwd=tmp_path, env=env, capture_output=True, text=True, timeout=15,
        )
        steps = calls.read_text().splitlines() if calls.exists() else []
        return result, steps

    return SimpleNamespace(run=run, marker=marker, config=config, data=data, staging=staging, properties=properties)


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
    assert restore_container.data.read_bytes() == b'base snapshot data\n'
    assert restore_container.marker.read_text().strip() == 'restore-1'

    restart, restart_steps = restore_container.run(RESTORE_KEY='restore-1', **env)

    assert restart.returncode == 0, restart.stderr
    assert restart_steps == ['pitr_restore|restore-1|restore-1']
    assert restore_container.marker.read_text().strip() == 'restore-1'

    new_job, new_job_steps = restore_container.run(RESTORE_KEY='restore-2', **env)

    assert new_job.returncode == 0, new_job.stderr
    assert new_job_steps == ['restore|restore-2|restore-1', 'pitr_restore|restore-2|restore-1']
    assert restore_container.marker.read_text().strip() == 'restore-2'


def test_shell_and_python_share_completed_restore_marker(restore_container, pitr_mapping):
    restore_container.marker.write_text('restore-2\n')
    restore_container.staging.mkdir()
    (restore_container.staging / 'stale.log').touch()
    original_data = restore_container.data.read_bytes()

    result, steps = restore_container.run(
        RESTORE_KEY='restore-2', RESTORE_MAPPING=json.dumps(pitr_mapping), POD_NAME='target-0',
        TEST_RUN_PITR='true',
    )

    assert result.returncode == 0, result.stderr
    assert steps == ['pitr_restore|restore-2|restore-2']
    properties = dict(line.split('=', 1) for line in restore_container.properties.read_text().splitlines())
    assert properties == {'archive_command': f'/bin/ln %path {restore_container.config.parent / "spool"}/%name'}
    assert list(restore_container.staging.iterdir()) == []
    assert restore_container.data.read_bytes() == original_data
    assert restore_container.marker.read_text() == 'restore-2\n'


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
    marker_contents = previous_key or 'absent'
    archive_step = f'pitr_restore|restore-2|{marker_contents}'
    assert steps == [archive_step]
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


@pytest.mark.parametrize(
    'pitr_config',
    [
        pytest.param('[pitr]\nenabled = false\n', id='explicitly-disabled'),
        pytest.param('[pitr]\n', id='enabled-unset'),
        pytest.param('', id='pitr-section-absent'),
    ],
)
@pytest.mark.parametrize('in_place', [True, False], ids=['in-place', 'out-of-place'])
@pytest.mark.parametrize('previous_key', [None, 'restore-1'], ids=['no-marker', 'previous-restore'])
def test_disabled_pitr_aborts_before_changing_data_on_each_attempt(
    restore_container, pitr_mapping, pitr_config, in_place, previous_key,
):
    config = restore_container.config
    config.write_text(config.read_text().partition('[pitr]')[0] + pitr_config)
    pitr_mapping['in_place'] = in_place
    if previous_key is not None:
        restore_container.marker.write_text(f'{previous_key}\n')
    original_data = restore_container.data.read_bytes()

    for _ in range(2):
        result, steps = restore_container.run(
            RESTORE_KEY='restore-2', RESTORE_MAPPING=json.dumps(pitr_mapping), POD_NAME='target-0',
        )

        assert result.returncode == 1, result.stderr
        assert 'PITR restore requested, but PITR is disabled' in result.stderr
        assert steps == []
        assert restore_container.data.read_bytes() == original_data
        if previous_key is None:
            assert not restore_container.marker.exists()
        else:
            assert restore_container.marker.read_text() == f'{previous_key}\n'


@pytest.mark.parametrize(
    'pitr_config',
    [
        pytest.param('[pitr]\nenabled = false\n', id='explicitly-disabled'),
        pytest.param('[pitr]\n', id='enabled-unset'),
        pytest.param('', id='pitr-section-absent'),
    ],
)
def test_standard_restore_still_runs_when_pitr_is_disabled(restore_container, pitr_config):
    config = restore_container.config
    config.write_text(config.read_text().partition('[pitr]')[0] + pitr_config)

    result, steps = restore_container.run(RESTORE_KEY='restore-1', BACKUP_NAME='legacy-backup')

    assert result.returncode == 0, result.stderr
    assert steps == ['restore|restore-1|absent', 'pitr_restore|restore-1|absent']
    assert restore_container.data.read_bytes() == b'base snapshot data\n'
    assert restore_container.marker.read_text() == 'restore-1\n'


@pytest.mark.parametrize('pitr_section', [True, False], ids=['disabled', 'section-absent'])
@pytest.mark.parametrize('restore_state', ['completed', 'unmapped-pod', 'no-restore-key'])
def test_disabled_pitr_without_pending_restore_allows_startup(
    restore_container, pitr_mapping, pitr_section, restore_state,
):
    config = restore_container.config
    pitr_config = '[pitr]\nenabled = false\n' if pitr_section else ''
    config.write_text(config.read_text().partition('[pitr]')[0] + pitr_config)
    restore_key = '' if restore_state == 'no-restore-key' else 'restore-2'
    marker_value = 'restore-2' if restore_state == 'completed' else 'restore-1'
    restore_container.marker.write_text(f'{marker_value}\n')
    restore_container.staging.mkdir()
    old_segment = restore_container.staging / 'old.log'
    old_segment.write_bytes(b'previous segment')
    restore_container.properties.write_text('previous properties\n')
    if restore_state == 'unmapped-pod':
        pitr_mapping['in_place'] = False
    original_data = restore_container.data.read_bytes()

    result, steps = restore_container.run(
        RESTORE_KEY=restore_key, RESTORE_MAPPING=json.dumps(pitr_mapping),
        POD_NAME='unmapped' if restore_state == 'unmapped-pod' else 'target-0', TEST_RUN_PITR='true',
    )

    assert result.returncode == 0, result.stderr
    assert steps == [f'pitr_restore|{restore_key}|{marker_value}']
    assert restore_container.data.read_bytes() == original_data
    assert restore_container.marker.read_text() == f'{marker_value}\n'
    assert old_segment.read_bytes() == b'previous segment'
    assert restore_container.properties.read_text() == 'previous properties\n'


# Stands in for the gRPC server: `poetry run python -m medusa.service.grpc.server server.py` runs this instead.
FAKE_MEDUSA = r"""
import os, signal, sys, time

def stop(*_):
    time.sleep(1)  # a clean stop takes a moment; the entrypoint must wait for it
    open('stopped', 'w').write('clean')
    sys.exit(int(os.environ['TEST_TERM_STATUS']))

signal.signal(signal.SIGTERM, signal.SIG_IGN if os.environ['TEST_IGNORE_TERM'] else stop)
open('started', 'w').write('yes')
if os.environ['TEST_EXIT_STATUS']:
    sys.exit(int(os.environ['TEST_EXIT_STATUS']))
while True:
    time.sleep(0.1)
"""


@pytest.fixture
def grpc_container(tmp_path):
    config = tmp_path / 'medusa.ini'
    config.write_text('[grpc]\nenabled = true\n')
    script = tmp_path / ENTRYPOINT.name
    # a marker leaked into /tmp by an earlier test (backup_node_test) would postpone the config-change restart forever
    script.write_text(ENTRYPOINT.read_text()
                      .replace('/etc/medusa/medusa.ini', str(config))
                      .replace('/tmp/medusa_backup_in_progress', str(tmp_path / 'medusa_backup_in_progress'))
                      .replace('MEDUSA_STOP_TIMEOUT_S=120', 'MEDUSA_STOP_TIMEOUT_S=3'))
    (tmp_path / 'fake_medusa.py').write_text(FAKE_MEDUSA)
    stub = tmp_path / 'poetry'
    stub.write_text(
        '#!/bin/bash\n'
        '[[ "$*" == "run python -m medusa.service.grpc.server server.py" ]] || exit 2\n'
        'exec "$TEST_PYTHON" fake_medusa.py\n'
    )
    stub.chmod(0o755)
    started = []

    def start(term_status=0, exit_status='', ignore_term=False):
        env = {
            'PATH': os.pathsep.join((str(tmp_path), os.environ['PATH'])),
            'MEDUSA_MODE': 'GRPC',
            'DEBUG_SLEEP': '0',
            'TEST_PYTHON': sys.executable,
            'TEST_TERM_STATUS': str(term_status),
            'TEST_EXIT_STATUS': str(exit_status),
            'TEST_IGNORE_TERM': 'yes' if ignore_term else '',
        }
        # own process group, as under tini; `tini -g` signals that whole group
        proc = subprocess.Popen(['/bin/bash', str(script)], cwd=tmp_path, env=env, start_new_session=True,
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        started.append(proc)
        deadline = time.monotonic() + 15
        while not (tmp_path / 'started').exists():
            assert proc.poll() is None or exit_status, proc.stdout.read()
            assert time.monotonic() < deadline, 'fake Medusa did not start'
            time.sleep(0.05)
        return proc

    yield SimpleNamespace(start=start, config=config, stopped=tmp_path / 'stopped')
    for proc in started:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        proc.wait()


@pytest.mark.parametrize('stop_signal', [signal.SIGTERM, signal.SIGINT], ids=['SIGTERM', 'SIGINT'])
@pytest.mark.parametrize('term_status', [0, 3])
def test_stop_signal_to_process_group_waits_for_medusa_clean_stop(grpc_container, term_status, stop_signal):
    proc = grpc_container.start(term_status=term_status)

    os.killpg(proc.pid, stop_signal)
    output, _ = proc.communicate(timeout=5)

    assert proc.returncode == term_status, output
    assert grpc_container.stopped.read_text() == 'clean'


def test_medusa_exit_status_is_entrypoint_exit_status(grpc_container):
    proc = grpc_container.start(exit_status=7)

    output, _ = proc.communicate(timeout=15)

    assert proc.returncode == 7, output


def test_config_change_stops_medusa_and_exits_zero(grpc_container):
    proc = grpc_container.start(term_status=3)

    grpc_container.config.write_text('[grpc]\nenabled = false\n')
    proc.wait(timeout=15)
    # checked as the entrypoint exits: under tini, Medusa dies with it
    assert grpc_container.stopped.read_text() == 'clean'
    output, _ = proc.communicate(timeout=5)

    assert proc.returncode == 0, output
    assert 'Detected change in medusa.ini' in output


def test_config_change_kills_medusa_that_does_not_stop_in_time(grpc_container):
    proc = grpc_container.start(ignore_term=True)

    grpc_container.config.write_text('[grpc]\nenabled = false\n')
    # returns once the fake Medusa, which holds the stdout pipe, is gone
    output, _ = proc.communicate(timeout=15)

    assert proc.returncode == 0, output
    assert not grpc_container.stopped.exists()


def test_image_runs_entrypoint_under_tini_signalling_the_group():
    dockerfile = DOCKERFILE.read_text()

    assert re.findall(r'^ENTRYPOINT .*$', dockerfile, re.M) == [
        'ENTRYPOINT ["/usr/bin/tini", "-g", "--", "/home/cassandra/docker-entrypoint.sh"]'
    ]
