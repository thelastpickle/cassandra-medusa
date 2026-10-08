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
import configparser
import logging
import unittest
from builtins import staticmethod
from enum import IntEnum
from unittest.mock import create_autospec, Mock

import pssh.clients.native.single
import pssh.clients.ssh.single
from pssh.clients.ssh import ParallelSSHClient

from medusa.config import (_namedtuple_from_dict, MedusaConfig, CassandraConfig, SSHConfig)
from medusa.orchestration import Orchestration, display_output


class ExitCode(IntEnum):
    SUCCESS = 0
    ERROR = 1


class HostOutputMock(Mock):
    """Mimic part of pssh.output.HostOutput to deceive Orchestration.pssh_run()"""

    @property
    def stdout(self):
        return ['fake stdout']

    @property
    def stderr(self):
        return ['fake stderr']


class OrchestrationTest(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def setUp(self):
        self.hosts = {'127.0.0.1': ExitCode.SUCCESS}
        self.config = self._build_config_parser()
        self.medusa_config = self._build_medusa_config(self.config)
        self.orchestration = Orchestration(self.medusa_config)
        self.mock_pssh = create_autospec(ParallelSSHClient)

    def fake_ssh_client_factory(self, *args, **kwargs):
        return self.mock_pssh

    @staticmethod
    def _build_config_parser():
        """Build and return a mutable config"""

        config = configparser.ConfigParser(interpolation=None)
        config['cassandra'] = {
            'use_sudo': 'True',
        }
        config['ssh'] = {
            'username': '',
            'key_file': '',
            'port': '22',
            'cert_file': '',
            'keepalive_seconds': '60',
            'use_pty': 'False',
            'login_shell': 'False',
        }
        return config

    @staticmethod
    def _build_medusa_config(config):
        return MedusaConfig(
            file_path=None,
            storage=None,
            monitoring={},
            cassandra=_namedtuple_from_dict(CassandraConfig, config['cassandra']),
            ssh=_namedtuple_from_dict(SSHConfig, config['ssh']),
            checks=None,
            logging=None,
            grpc=None,
            kubernetes=None,
        )

    def test_pssh_with_sudo(self):
        """Ensure that Parallel SSH honors configuration when we want to use sudo in commands"""
        output = [HostOutputMock(host=host, exit_code=exit_code) for host, exit_code in self.hosts.items()]
        self.mock_pssh.run_command.return_value = output
        assert self.orchestration.pssh_run(list(self.hosts.keys()), 'fake command',
                                           ssh_client=self.fake_ssh_client_factory)
        self.mock_pssh.run_command.assert_called_with(
            'fake command',
            host_args=None, use_pty=False, shell=None, sudo=True
        )

    def test_pssh_without_sudo(self):
        """Ensure that Parallel SSH honors configuration when we don't want to use sudo in commands"""
        conf = self.config
        conf['cassandra']['use_sudo'] = 'False'
        conf['ssh']['login_shell'] = 'True'
        medusa_conf = self._build_medusa_config(conf)
        orchestration_no_sudo = Orchestration(medusa_conf)

        output = [HostOutputMock(host=host, exit_code=exit_code) for host, exit_code in self.hosts.items()]
        self.mock_pssh.run_command.return_value = output
        assert orchestration_no_sudo.pssh_run(list(self.hosts.keys()), 'fake command',
                                              ssh_client=self.fake_ssh_client_factory)

        self.mock_pssh.run_command.assert_called_with(
            'fake command',
            host_args=None, use_pty=False, shell='$SHELL -cl', sudo=False
        )

    def test_pssh_run_failure(self):
        """Ensure that Parallel SSH detects a failed command on a host"""
        hosts = {
            '127.0.0.1': ExitCode.SUCCESS,
            '127.0.0.2': ExitCode.ERROR,
            '127.0.0.3': ExitCode.SUCCESS,
        }
        output = [HostOutputMock(host=host, exit_code=exit_code) for host, exit_code in hosts.items()]
        self.mock_pssh.run_command.return_value = output
        assert not self.orchestration.pssh_run(list(self.hosts.keys()), 'fake command',
                                               ssh_client=self.fake_ssh_client_factory)
        self.mock_pssh.run_command.assert_called_with(
            'fake command',
            host_args=None, use_pty=False, shell=None, sudo=True
        )


NODETOOL_PASSWORD = 'n0detool-s3cret'
SNAPSHOT_COMMAND = 'nodetool -u cassandra -pw {} snapshot -t medusa-backup1'.format(NODETOOL_PASSWORD)
REDACTED_SNAPSHOT_COMMAND = 'nodetool -u cassandra -pw *** snapshot -t medusa-backup1'


def _pssh_run_snapshot_command(caplog, hosts):
    caplog.set_level(logging.DEBUG)
    mock_pssh = create_autospec(ParallelSSHClient)
    mock_pssh.run_command.return_value = [HostOutputMock(host=host, exit_code=exit_code)
                                          for host, exit_code in hosts.items()]
    config = OrchestrationTest._build_medusa_config(OrchestrationTest._build_config_parser())

    pssh_run_success = Orchestration(config).pssh_run(list(hosts.keys()), SNAPSHOT_COMMAND,
                                                      ssh_client=lambda *args, **kwargs: mock_pssh)

    # Only what is logged gets masked, the nodes still run the command with the password
    mock_pssh.run_command.assert_called_with(SNAPSHOT_COMMAND, host_args=None, use_pty=False, shell=None, sudo=True)
    return pssh_run_success


def test_pssh_run_does_not_log_nodetool_password(caplog):
    assert _pssh_run_snapshot_command(caplog, {'127.0.0.1': ExitCode.SUCCESS})

    assert NODETOOL_PASSWORD not in caplog.text
    assert 'Executing "{}" on following nodes'.format(REDACTED_SNAPSHOT_COMMAND) in caplog.text
    assert 'Running "{}"'.format(REDACTED_SNAPSHOT_COMMAND) in caplog.text
    assert 'Job executing "{}" ran and finished Successfully'.format(REDACTED_SNAPSHOT_COMMAND) in caplog.text


def test_failed_pssh_run_does_not_log_nodetool_password(caplog):
    assert not _pssh_run_snapshot_command(caplog, {'127.0.0.1': ExitCode.SUCCESS, '127.0.0.2': ExitCode.ERROR})

    assert NODETOOL_PASSWORD not in caplog.text
    errors = [record.getMessage() for record in caplog.records if record.levelno == logging.ERROR]
    assert len(errors) == 1
    assert 'Job executing "{}" ran and finished with errors'.format(REDACTED_SNAPSHOT_COMMAND) in errors[0]


def test_display_output_does_not_log_nodetool_password(caplog):
    caplog.set_level(logging.DEBUG)
    # The output of failed nodes gets relayed, e.g. the DEBUG logs of `medusa -vvv backup-node` that ran there
    relayed_line = 'DEBUG: Executing: nodetool -u cassandra -pw {} clearsnapshot -t medusa-backup1'.format(
        NODETOOL_PASSWORD)
    display_output([Mock(host='127.0.0.2', stdout=[relayed_line], stderr=[relayed_line])])

    assert NODETOOL_PASSWORD not in caplog.text
    for stream in ('stdout', 'stderr'):
        assert '127.0.0.2-{}: DEBUG: Executing: nodetool -u cassandra -pw *** clearsnapshot -t medusa-backup1'.format(
            stream) in caplog.text


def test_pssh_debug_log_does_not_contain_nodetool_password(caplog):
    caplog.set_level(logging.DEBUG)
    # parallel-ssh logs every command it executes at DEBUG level, wrapped in sudo/shell and encoded
    executed_command = "sudo -S $SHELL -c '{}'".format(SNAPSHOT_COMMAND).encode('utf-8')
    for pssh_logger in (pssh.clients.native.single.logger, pssh.clients.ssh.single.logger):
        pssh_logger.debug("Executing command '%s'", executed_command)

    assert NODETOOL_PASSWORD not in caplog.text
    assert caplog.text.count(REDACTED_SNAPSHOT_COMMAND) == 2


if __name__ == '__main__':
    unittest.main()
