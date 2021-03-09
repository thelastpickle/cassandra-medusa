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
import unittest
from builtins import staticmethod
from enum import IntEnum
from unittest.mock import create_autospec, Mock

from pssh.clients.ssh import ParallelSSHClient

from medusa.config import (_namedtuple_from_dict, MedusaConfig, CassandraConfig, SSHConfig)
from medusa.orchestration import Orchestration


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
            'cert_file': ''
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
        self.mock_pssh.run_command.assert_called_with('fake command', host_args=None, sudo=True)

    def test_pssh_without_sudo(self):
        """Ensure that Parallel SSH honors configuration when we don't want to use sudo in commands"""
        conf = self.config
        conf['cassandra']['use_sudo'] = 'False'
        medusa_conf = self._build_medusa_config(conf)
        orchestration_no_sudo = Orchestration(medusa_conf)

        output = [HostOutputMock(host=host, exit_code=exit_code) for host, exit_code in self.hosts.items()]
        self.mock_pssh.run_command.return_value = output
        assert orchestration_no_sudo.pssh_run(list(self.hosts.keys()), 'fake command',
                                              ssh_client=self.fake_ssh_client_factory)

        self.mock_pssh.run_command.assert_called_with('fake command', host_args=None, sudo=False)

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
        self.mock_pssh.run_command.assert_called_with('fake command', host_args=None, sudo=True)


if __name__ == '__main__':
    unittest.main()
