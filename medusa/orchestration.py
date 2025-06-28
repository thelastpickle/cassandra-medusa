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

import logging

from pssh.clients.native.parallel import ParallelSSHClient as PsshNativeClient
from pssh.clients.ssh.parallel import ParallelSSHClient as PsshSSHClient

import medusa.utils
from medusa.storage import divide_chunks


def display_output(host_outputs):
    for host_out in host_outputs:
        for line in host_out.stdout:
            logging.info("{}-stdout: {}".format(host_out.host, line))
        for line in host_out.stderr:
            logging.info("{}-stderr: {}".format(host_out.host, line))


class OrchestrationError(RuntimeError):
    """Raised when an unexpected error occurs during orchestration of commands across nodes."""
    pass


class Orchestration(object):
    def __init__(self, config, pool_size=10):
        self.pool_size = pool_size
        self.config = config

    def pssh_run(self, hosts, command, hosts_variables=None, ssh_client=None):
        """
        Runs a command on hosts list using pssh under the hood
        Return: True (success) or False (error)
        """
        username = self.config.ssh.username if self.config.ssh.username != '' else None
        port = int(self.config.ssh.port)
        pkey = self.config.ssh.key_file if self.config.ssh.key_file != '' else None
        cert_file = self.config.ssh.cert_file if self.config.ssh.cert_file != '' else None
        keepalive_seconds = int(self.config.ssh.keepalive_seconds)
        use_pty = medusa.utils.evaluate_boolean(self.config.ssh.use_pty)
        use_login_shell = medusa.utils.evaluate_boolean(self.config.ssh.login_shell)

        if ssh_client is None:
            if cert_file is None:
                ssh_client = PsshNativeClient
            else:
                ssh_client = PsshSSHClient
        pssh_run_success = False
        success = []
        error = []
        i = 1

        logging.info('Executing "{command}" on following nodes {hosts} with a parallelism/pool size of {pool_size}'
                     .format(command=command, hosts=hosts, pool_size=self.pool_size))

        for parallel_hosts in divide_chunks(hosts, self.pool_size):
            client = self._init_ssh_client(parallel_hosts, ssh_client, cert_file, username, port, pkey,
                                           keepalive_seconds)

            logging.debug(f'Batch #{i}: Running "{command}" nodes={parallel_hosts} parallelism={len(parallel_hosts)} '
                          f'login_shell={use_login_shell}')

            shell = '$SHELL -cl' if use_login_shell else None

            output = client.run_command(command, host_args=hosts_variables, use_pty=use_pty, shell=shell,
                                        sudo=medusa.utils.evaluate_boolean(self.config.cassandra.use_sudo))
            client.join(output)

            success = success + list(filter(lambda host_output: host_output.exit_code == 0, output))
            error = error + list(filter(lambda host_output: host_output.exit_code != 0, output))

        # Report on execution status
        if len(success) == len(hosts):
            logging.info('Job executing "{}" ran and finished Successfully on all nodes.'
                         .format(command))
            pssh_run_success = True
        elif len(error) > 0:
            logging.error('Job executing "{}" ran and finished with errors on following nodes: {}'
                          .format(command, sorted({host_output.host for host_output in error})))
            display_output(error)
        else:
            err_msg = 'Something unexpected happened while running pssh command'
            logging.error(err_msg)
            raise OrchestrationError(err_msg)

        return pssh_run_success

    def _init_ssh_client(self, parallel_hosts, ssh_client, cert_file, username, port, pkey, keepalive_seconds):
        if cert_file is None:
            return ssh_client(parallel_hosts,
                              forward_ssh_agent=True,
                              pool_size=len(parallel_hosts),
                              user=username,
                              port=port,
                              pkey=pkey,
                              keepalive_seconds=keepalive_seconds)
        else:
            logging.debug('The ssh parameter "cert_file" is defined. Due to limitations in parallel-ssh '
                          '"keep_alive" will be ignored and no ServerAlive messages will be generated')
            return ssh_client(parallel_hosts,
                              forward_ssh_agent=True,
                              pool_size=len(parallel_hosts),
                              user=username,
                              port=port,
                              pkey=pkey,
                              cert_file=cert_file)
