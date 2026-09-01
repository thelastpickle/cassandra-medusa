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

import asyncio
import logging

import asyncssh

import medusa.utils
from medusa.storage import divide_chunks


def display_output(host_results):
    """Log stdout/stderr for a list of (host, SSHCompletedProcess) pairs."""
    for host, result in host_results:
        if isinstance(result, BaseException):
            logging.info("{}-error: {}".format(host, result))
            continue
        for line in result.stdout.splitlines():
            logging.info("{}-stdout: {}".format(host, line))
        for line in result.stderr.splitlines():
            logging.info("{}-stderr: {}".format(host, line))


class OrchestrationError(RuntimeError):
    """Raised when an unexpected error occurs during orchestration of commands across nodes."""
    pass


async def _run_one(host, command, connect_kwargs):
    """Connect to a single host and run one command. Returns SSHCompletedProcess."""
    async with asyncssh.connect(host, **connect_kwargs) as conn:
        # check=False: non-zero exit returns SSHCompletedProcess; we inspect returncode ourselves.
        return await conn.run(command, check=False)


async def _run_on_hosts(hosts, commands, connect_kwargs):
    """Fan out _run_one across all hosts in a batch concurrently.

    return_exceptions=True means a single unreachable host does not abort the batch.
    Returns a list of (host, SSHCompletedProcess | Exception) pairs.
    """
    coros = [_run_one(host, cmd, connect_kwargs) for host, cmd in zip(hosts, commands)]
    results = await asyncio.gather(*coros, return_exceptions=True)
    return list(zip(hosts, results))


class Orchestration(object):
    def __init__(self, config, pool_size=10):
        self.pool_size = pool_size
        self.config = config

    def pssh_run(self, hosts, command, hosts_variables=None, ssh_client=None):
        """Run command on hosts in parallel via asyncssh. Returns True on full success, False on any error.

        hosts_variables: list of tuples for per-host %s substitution, or None/empty for a uniform command.
        ssh_client: accepted for API compatibility but ignored.
        Must be called from a synchronous context — asyncio.run() is used internally.
        """
        username = self.config.ssh.username if self.config.ssh.username != '' else None
        port = int(self.config.ssh.port)
        pkey = self.config.ssh.key_file if self.config.ssh.key_file != '' else None
        cert_file = self.config.ssh.cert_file if self.config.ssh.cert_file != '' else None
        keepalive_seconds = int(self.config.ssh.keepalive_seconds)
        use_pty = medusa.utils.evaluate_boolean(self.config.ssh.use_pty)
        use_login_shell = medusa.utils.evaluate_boolean(self.config.ssh.login_shell)
        use_sudo = medusa.utils.evaluate_boolean(self.config.cassandra.use_sudo)
        forward_agent = medusa.utils.evaluate_boolean(self.config.ssh.forward_agent)

        # asyncssh connect options; known_hosts defaults to ~/.ssh/known_hosts (not bypassed here).
        connect_kwargs = {
            'port': port,
            'agent_forwarding': forward_agent,
            'keepalive_interval': keepalive_seconds,
            'request_pty': use_pty,
            'connect_timeout': 30,
        }
        known_hosts = self.config.ssh.known_hosts if self.config.ssh.known_hosts else None
        if known_hosts is not None:
            connect_kwargs['known_hosts'] = known_hosts
        if username is not None:
            connect_kwargs['username'] = username
        if pkey is not None:
            connect_kwargs['client_keys'] = [(pkey, cert_file)] if cert_file is not None else [pkey]

        pssh_run_success = False
        success = []
        error = []
        i = 1

        logging.info('Executing "{command}" on following nodes {hosts} with a parallelism/pool size of {pool_size}'
                     .format(command=command, hosts=hosts, pool_size=self.pool_size))

        for parallel_hosts in divide_chunks(hosts, self.pool_size):
            # `{}` (empty dict, backup path) and None both mean no substitution.
            if hosts_variables and isinstance(hosts_variables, list):
                batch_start = (i - 1) * self.pool_size
                commands = [
                    command % tuple(hosts_variables[batch_start + j])
                    for j in range(len(parallel_hosts))
                ]
            else:
                commands = [command] * len(parallel_hosts)

            # sudo wraps the command; login shell wraps everything (sudo is inside the shell).
            processed_commands = [
                "$SHELL -cl '{}'".format(('sudo ' if use_sudo else '') + cmd) if use_login_shell
                else ('sudo ' if use_sudo else '') + cmd
                for cmd in commands
            ]

            logging.debug(f'Batch #{i}: Running on nodes={parallel_hosts} parallelism={len(parallel_hosts)} '
                          f'login_shell={use_login_shell} sudo={use_sudo}')

            batch_results = asyncio.run(
                _run_on_hosts(parallel_hosts, processed_commands, connect_kwargs)
            )

            for host, result in batch_results:
                if isinstance(result, BaseException):
                    logging.error("{}: connection/execution error: {}".format(host, result))
                    error.append((host, result))
                elif result.returncode == 0:
                    success.append((host, result))
                else:
                    error.append((host, result))

            i += 1

        # Report on execution status
        if len(success) == len(hosts):
            logging.info('Job executing "{}" ran and finished Successfully on all nodes.'
                         .format(command))
            pssh_run_success = True
        elif len(error) > 0:
            logging.error('Job executing "{}" ran and finished with errors on following nodes: {}'
                          .format(command, sorted({host for host, _ in error})))
            display_output(error)
        else:
            err_msg = 'Something unexpected happened while running SSH command'
            logging.error(err_msg)
            raise OrchestrationError(err_msg)

        return pssh_run_success
