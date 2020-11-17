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

import paramiko
import logging
from pssh.clients.miko import ParallelSSHClient
from medusa.storage import divide_chunks


def display_output(host_outputs):
    for host_out in host_outputs:
        for line in host_out.stdout:
            logging.info("{}-stdout: {}".format(host_out.host, line))
        for line in host_out.stderr:
            logging.info("{}-stderr: {}".format(host_out.host, line))


class Orchestration(object):
    def __init__(self, cassandra_config, pool_size=10):
        self.pool_size = pool_size
        self.cassandra_config = cassandra_config

    def pssh_run(self, hosts, command, hosts_variables=None):
        """
        Runs a command on hosts list using pssh under the hood
        Return: True (success) or False (error)
        """
        pssh_run_success = False
        success = []
        error = []
        i = 1

        username = self.cassandra_config.ssh.username if self.cassandra_config.ssh.username != '' else None
        port = self.cassandra_config.ssh.port
        pkey = None
        if self.cassandra_config.ssh.key_file is not None and self.cassandra_config.ssh.key_file != '':
            pkey = paramiko.RSAKey.from_private_key_file(self.cassandra_config.ssh.key_file)

        logging.info('Executing "{command}" on following nodes {hosts} with a parallelism/pool size of {pool_size}'
                     .format(command=command, hosts=hosts, pool_size=self.pool_size))

        for parallel_hosts in divide_chunks(hosts, self.pool_size):

            client = ParallelSSHClient(parallel_hosts,
                                       forward_ssh_agent=True,
                                       pool_size=len(parallel_hosts),
                                       user=username,
                                       port=port,
                                       pkey=pkey)
            logging.debug('Batch #{i}: Running "{command}" on nodes {hosts} parallelism of {pool_size}'
                          .format(i=i, command=command, hosts=parallel_hosts, pool_size=len(parallel_hosts)))
            output = client.run_command(command, host_args=hosts_variables, sudo=True)
            client.join(output)

            success = success + list(filter(lambda host_output: host_output.exit_code == 0,
                                            list(map(lambda host_output: host_output[1], output.items()))))
            error = error + list(filter(lambda host_output: host_output.exit_code != 0,
                                        list(map(lambda host_output: host_output[1], output.items()))))

        # Report on execution status
        if len(success) == len(hosts):
            logging.info('Job executing "{}" ran and finished Successfully on all nodes.'
                         .format(command))
            pssh_run_success = True
        elif len(error) > 0:
            logging.error('Job executing "{}" ran and finished with errors on following nodes: {}'
                          .format(command, sorted(set(map(lambda host_output: host_output.host, error)))))
            display_output(error)
        else:
            err_msg = 'Something unexpected happened while running pssh command'
            logging.error(err_msg)
            raise Exception(err_msg)

        return pssh_run_success
