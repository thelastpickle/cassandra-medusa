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
"""
Pure helpers of the PITR end-to-end integration test (see the @pitr scenario).

They live outside of the behave steps module because that module changes the working directory when it is imported,
which makes it unusable from unit tests.
"""
import math
import shutil
import time
import typing as t
from pathlib import Path

from medusa.pitr_utils import generate_commitlog_archiving_properties

# tests/integration/pitr_helpers.py -> repository root. Resolved at import time: behave changes the working directory.
REPO_ROOT = Path(__file__).resolve().parents[2]

SPOOL_DIR_NAME = 'pitr_commitlog_spool'
STAGING_DIR_NAME = 'pitr_commitlog_staging'
POD_NAME = 'pitr-e2e-sts-0'

ARCHIVER_POLL_INTERVAL_S = 2
ARCHIVER_DRAIN_TIMEOUT_S = 60
ARCHIVE_INTERVAL_S = 2


def archive_command(spool_dir: t.Union[Path, str]) -> str:
    """The archive_command the operator configures: a hardlink of the segment into the spool dir."""
    return '/bin/ln %path {}/%name'.format(spool_dir)


def archiving_properties(spool_dir: t.Union[Path, str]) -> str:
    """Content of commitlog_archiving.properties for a node that only archives (no restore)."""
    return generate_commitlog_archiving_properties(archive_command=archive_command(spool_dir))


def format_target_timestamp(epoch_s: float) -> str:
    """
    Epoch seconds as the decimal string of PreparePitrRestoreRequest.targetTimestamp, truncated to milliseconds
    (the resolution the restore uses), so the string is never later than the instant it was taken at.
    """
    if not math.isfinite(epoch_s):
        raise ValueError('Invalid timestamp: {}'.format(epoch_s))
    return '{:.3f}'.format(math.floor(epoch_s * 1000) / 1000)


def enable_pitr_in_ini(parser, spool_dir, staging_dir, archive_interval_s: int = ARCHIVE_INTERVAL_S):
    """
    Turns on [pitr] in a ConfigParser. [pitr] enabled is the one setting that cannot come from the command line
    arguments (see medusa.config._override_config_with_args), so it has to be in the ini file.
    """
    if not parser.has_section('pitr'):
        parser.add_section('pitr')
    parser.set('pitr', 'enabled', 'true')
    parser.set('pitr', 'commitlog_archive_interval_seconds', str(archive_interval_s))
    parser.set('pitr', 'commitlog_spool_dir', str(spool_dir))
    parser.set('pitr', 'commitlog_staging_dir', str(staging_dir))
    return parser


def enable_kubernetes_in_ini(parser):
    """
    The restore init containers run with [kubernetes] enabled: Medusa then leaves the Cassandra lifecycle to the
    caller instead of stopping and starting it.
    """
    if not parser.has_section('kubernetes'):
        parser.add_section('kubernetes')
    parser.set('kubernetes', 'enabled', 'True')
    return parser


def build_restore_mapping(node_base_backups: t.Mapping[str, str], target_timestamp: str,
                          pod_name: str = POD_NAME) -> dict:
    """
    The RESTORE_MAPPING the operator builds from a PreparePitrRestore response, for a single pod that restores in
    place from the (only) node of the response.
    """
    if len(node_base_backups) != 1:
        raise ValueError('Expected the base backup of exactly one node, got {}'.format(len(node_base_backups)))
    source = next(iter(node_base_backups))
    return {
        'in_place': True,
        'host_map': {pod_name: {'source': [source], 'seed': False}},
        'pitr': {
            'target_timestamp': target_timestamp,
            'node_backups': dict(node_base_backups),
        },
    }


def restore_environment(base_env: t.Mapping[str, str], mapping_json: str, restore_key: str,
                        pod_name: str = POD_NAME, repo_root: t.Union[Path, str] = REPO_ROOT) -> dict:
    """
    Environment of the restore subprocesses. PYTHONPATH must be absolute, the working directory of the test run is
    not the repository root. POD_IP is dropped: it would override storage.fqdn.
    """
    env = {key: value for key, value in base_env.items() if key != 'POD_IP'}
    python_path = [str(repo_root)]
    if env.get('PYTHONPATH'):
        python_path.append(env['PYTHONPATH'])
    env.update({
        'RESTORE_MAPPING': mapping_json,
        'RESTORE_KEY': restore_key,
        'POD_NAME': pod_name,
        'PYTHONPATH': ':'.join(python_path),
        'LOCAL_JMX': 'yes',
    })
    return env


def archiver_is_drained(status, min_upload_time: int = 0) -> bool:
    """
    True when the archiver runs, has nothing left in the spool and uploaded something at or after min_upload_time.
    An empty spool alone proves nothing: the segments may not have been sealed yet.
    """
    if status is None:
        return False
    last_upload_time = status.lastUploadTime
    return bool(
        status.running
        and status.pendingCount == 0
        and last_upload_time > 0
        and last_upload_time >= min_upload_time
    )


def wait_for_archiver_drain(get_status: t.Callable[[], t.Any], min_upload_time: int = 0,
                            timeout_s: float = ARCHIVER_DRAIN_TIMEOUT_S, interval_s: float = ARCHIVER_POLL_INTERVAL_S,
                            required_consecutive: int = 2,
                            clock: t.Callable[[], float] = time.monotonic,
                            sleep: t.Callable[[float], None] = time.sleep):
    """
    Polls get_status until archiver_is_drained() held on required_consecutive polls in a row, and returns that status.

    :raises TimeoutError: if that did not happen within timeout_s
    """
    deadline = clock() + timeout_s
    consecutive = 0
    status = None
    while True:
        status = get_status()
        if archiver_is_drained(status, min_upload_time):
            consecutive += 1
            if consecutive >= required_consecutive:
                return status
        else:
            consecutive = 0
        if clock() >= deadline:
            raise TimeoutError(
                'The commitlog archiver did not drain within {}s (last status: {})'.format(timeout_s, status))
        sleep(interval_s)


def remove_paths(paths: t.Iterable[t.Union[Path, str]]) -> None:
    """Removes the directories and files, ignoring the ones that do not exist."""
    for path in paths:
        path = Path(path)
        if path.is_dir() and not path.is_symlink():
            shutil.rmtree(path, ignore_errors=True)
        elif path.exists() or path.is_symlink():
            path.unlink()
