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
import logging
import subprocess
from unittest import mock

from medusa.config import CassandraConfig, _namedtuple_from_dict
from medusa.service.snapshot.nodetool_snapshot_service import NodetoolSnapshotService

NODETOOL_PASSWORD = 'n0detool-s3cret'
REDACTED_NODETOOL = 'nodetool -u cassandra -pw *** -pwf /etc/cassandra/jmx.password'


def _snapshot_service():
    cassandra_config = _namedtuple_from_dict(CassandraConfig, {
        'nodetool_executable': 'nodetool',
        'nodetool_username': 'cassandra',
        'nodetool_password': NODETOOL_PASSWORD,
        'nodetool_password_file_path': '/etc/cassandra/jmx.password',
    })
    return NodetoolSnapshotService(cassandra_config)


def _failing_nodetool(cmd, **kwargs):
    raise subprocess.CalledProcessError(2, cmd, output='nodetool: Failed to connect')


def _assert_nodetool_got_the_password(check_output):
    # Only what is logged gets masked, nodetool itself still receives the password
    cmd = check_output.call_args.args[0]
    assert cmd[cmd.index('-pw') + 1] == NODETOOL_PASSWORD


def test_create_snapshot_does_not_log_nodetool_password(caplog):
    caplog.set_level(logging.DEBUG)
    with mock.patch.object(subprocess, 'check_output', return_value='') as check_output:
        _snapshot_service().create_snapshot(tag='medusa-backup1')

    _assert_nodetool_got_the_password(check_output)
    assert NODETOOL_PASSWORD not in caplog.text
    assert 'Executing: {} snapshot -t medusa-backup1'.format(REDACTED_NODETOOL) in caplog.text


def test_failed_create_snapshot_does_not_log_nodetool_password(caplog):
    caplog.set_level(logging.DEBUG)
    with mock.patch.object(subprocess, 'check_output', side_effect=_failing_nodetool) as check_output:
        _snapshot_service().create_snapshot(tag='medusa-backup1')

    _assert_nodetool_got_the_password(check_output)
    assert NODETOOL_PASSWORD not in caplog.text
    assert 'nodetool output: nodetool: Failed to connect' in caplog.text


def test_delete_snapshot_does_not_log_nodetool_password(caplog):
    caplog.set_level(logging.DEBUG)
    with mock.patch.object(subprocess, 'check_output', return_value='') as check_output:
        _snapshot_service().delete_snapshot(tag='medusa-backup1')

    _assert_nodetool_got_the_password(check_output)
    assert NODETOOL_PASSWORD not in caplog.text
    assert 'Executing: {} clearsnapshot -t medusa-backup1'.format(REDACTED_NODETOOL) in caplog.text


def test_failed_delete_snapshot_does_not_log_nodetool_password(caplog):
    caplog.set_level(logging.DEBUG)
    with mock.patch.object(subprocess, 'check_output', side_effect=_failing_nodetool) as check_output:
        _snapshot_service().delete_snapshot(tag='medusa-backup1')

    _assert_nodetool_got_the_password(check_output)
    assert NODETOOL_PASSWORD not in caplog.text
    warnings = [record.getMessage() for record in caplog.records if record.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert 'by running: {} clearsnapshot -t medusa-backup1'.format(REDACTED_NODETOOL) in warnings[0]
