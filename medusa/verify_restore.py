# -*- coding: utf-8 -*-
# Copyright 2019 Spotify AB. All rights reserved.
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

import json
import logging

from medusa.cassandra_utils import wait_for_node_to_come_up, CqlSessionProvider


def verify_restore(hosts, backups, config):
    """
    This function checks that each node is up as specified by health_check. If the backup includes a verification query
    it will then create a CQL session and execute the verification query. This function simply iterates over the list
    of backups and will use the first verification query that it finds.

    :param hosts: The hostnames for the Cassandra nodes
    :param backups: A list of backups to check for verification queries.
    :param config: The medusa config
    :return:
    """

    logging.info('Verifying the restore')
    for host in hosts:
        wait_for_node_to_come_up(config.restore.health_check, host)
    restore_verify_query = _get_restore_verify_query(backups)
    if restore_verify_query:
        logging.info('Executing restore verify query: %s', restore_verify_query['query'])
        session_provider = CqlSessionProvider(hosts,
                                              username=config.cassandra.cql_username,
                                              password=config.cassandra.cql_password)
        with session_provider.new_session(retry=True) as cql_session:
            cql_session.session.execute(restore_verify_query['query'])
            logging.info('Restore verify query completed successfully')
    logging.info('The restore verification is complete')


def _get_restore_verify_query(backups):
    """
    The verification query is currently stored per node. This function searches each individual
    backup and returns the first verification query that it finds.

    :return The verify query as a python object or None if there is query
    """
    for backup in backups:
        verify_query_str = backup.restore_verify_query
        if verify_query_str:
            verify_query = json.loads(verify_query_str)
            return verify_query
    return None
