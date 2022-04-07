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

import logging
import sys

from medusa.cassandra_utils import wait_for_node_to_come_up, CqlSessionProvider


def verify_restore(hosts, config):
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
        wait_for_node_to_come_up(config, host)

    restore_verify_query = config.checks.query

    if len(restore_verify_query) > 0:
        logging.info('Executing restore verify query: {}'.format(restore_verify_query))

        session_provider = _get_cql_session_provider(config, hosts)
        with session_provider.new_session(retry=True) as cql_session:
            results = cql_session.session.execute(restore_verify_query)
            rows, actual_row_count = _consume_results(results)
            logging.info('Restore verify query completed successfully')

            # if config tells us to check for expected row count, we check that
            expected_row_count = int(config.checks.expected_rows)
            if expected_row_count:
                if actual_row_count == int(config.checks.expected_rows):
                    logging.info('Restore verify query returned expected number of rows {}'.format(expected_row_count))
                else:
                    logging.error('Restore verify query returned unexpected number of rows')
                    logging.error('Expected {} but got {}'.format(expected_row_count, actual_row_count))
                    sys.exit(1)

            # if the config tells us to check for actual results, we check that too
            if config.checks.expected_result:
                # expected result allows just one row
                expected_result = config.checks.expected_result
                actual_result = ','.join(rows[0] if len(rows) >= 1 else [])
                if actual_result == expected_result:
                    logging.info('Restore verify query returned expected result {}'.format(expected_result))
                else:
                    logging.error('Restore verify query did not return expected result')
                    logging.error('Expected: {}'.format(expected_result))
                    logging.error('But got : {}'.format(actual_result))
                    sys.exit(1)

    logging.info('The restore verification is complete')


def _get_cql_session_provider(config, hosts):

    if int(config.cassandra.is_ccm) == 1:
        cql_hosts = ['localhost']
    else:
        cql_hosts = hosts

    return CqlSessionProvider(cql_hosts, config)


def _consume_results(cql_results):
    """
    To facilitate verification, we map str() to each value in each row

    :param cql_results: result set returned from cassandra driver
    :return: tuple of processed rows and their count
    """

    rows = [[str(column) for column in row] for row in cql_results]
    return rows, len(rows)
