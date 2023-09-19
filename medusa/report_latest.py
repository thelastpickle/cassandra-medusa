#! /usr/bin/env python
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

import datetime
import logging
import time
import medusa.index
from medusa.monitoring import Monitoring
from medusa.storage import Storage


def report_latest(config, push_metrics):
    MAX_RETRIES = 3
    SLEEP_TIME = 15
    retry = 0

    monitoring = Monitoring(config=config.monitoring)

    for retry in range(MAX_RETRIES):
        try:
            logging.debug('Trying to report about existing backups ({}/{})...'.format(
                retry + 1,
                MAX_RETRIES
            ))
            with Storage(config=config.storage) as storage:
                fqdn = config.storage.fqdn
                backup_index = storage.list_backup_index_blobs()
                check_node_backup(config, storage, fqdn, push_metrics, monitoring)
                check_complete_cluster_backup(storage, push_metrics, monitoring, backup_index)
                check_latest_cluster_backup(storage, push_metrics, monitoring, backup_index)
            break
        except Exception as e:
            if (retry + 1) < MAX_RETRIES:
                logging.debug('Report attempt {} failed, waiting {} seconds to retry'.format(
                    retry + 1,
                    SLEEP_TIME
                ))
                time.sleep(SLEEP_TIME)
                continue
            else:
                logging.error('This error happened during the check: {}'.format(e), exc_info=True)
                if push_metrics:
                    # Set latest known complete backup to ~ 10 years ago to attract the attention
                    # of the operator on the broken monitoring.
                    logging.info("Sending a big value to 'seconds-since-backup' metric to trigger alerts.")
                    long_time_flag_value = 315365400
                    tags = ['medusa-cluster-backup', 'seconds-since-backup', 'TRACKING-ERROR']
                    monitoring.send(tags, long_time_flag_value)


def check_node_backup(config, storage, fqdn, push_metrics, monitoring):
    latest_node_backup = storage.latest_node_backup(fqdn=fqdn)

    if latest_node_backup is None:
        logging.info('Did not find the latest node backup. Will try rebuild the index')
        medusa.index.build_indices(config, noop=False)
        latest_node_backup = storage.latest_node_backup(fqdn=fqdn)

    if latest_node_backup is None:
        logging.info('This node has not been backed up yet')
        return

    finished = latest_node_backup.finished
    now = int(datetime.datetime.now().timestamp())
    node_backup_finished_seconds_ago = int(now - finished)
    logging.info('Latest node backup '
                 'finished {} seconds ago'.format(node_backup_finished_seconds_ago))

    if push_metrics:
        logging.debug('Sending time since last node backup to them monitoring backend')
        tags = ['medusa-node-backup', 'seconds-since-backup', latest_node_backup.name]
        monitoring.send(tags, node_backup_finished_seconds_ago)


def check_complete_cluster_backup(storage, push_metrics, monitoring, backup_index):
    latest_complete_cluster_backup = storage.latest_complete_cluster_backup(backup_index=backup_index)

    if latest_complete_cluster_backup is None:
        logging.info('The cluster this node belongs to has no complete backup yet')
        return

    logging.info('Latest complete backup:')
    logging.info('- Name: {}'.format(latest_complete_cluster_backup.name))

    finished = latest_complete_cluster_backup.finished
    now = int(datetime.datetime.now().timestamp())
    cluster_backup_finished_seconds_ago = int(now - finished)
    logging.info('- Finished: {} seconds ago'.format(cluster_backup_finished_seconds_ago))

    if push_metrics:
        logging.debug("Sending time since last complete cluster backup to monitoring backend")
        tags = ['medusa-cluster-backup', 'seconds-since-backup', latest_complete_cluster_backup.name]
        monitoring.send(tags, cluster_backup_finished_seconds_ago)


def check_latest_cluster_backup(storage, push_metrics, monitoring, backup_index):
    latest_cluster_backup = storage.latest_cluster_backup(backup_index=backup_index)

    if latest_cluster_backup is None:
        logging.info('The cluster this node belongs to has not started a backup yet')
        return

    logging.info('Latest backup:')
    logging.info('- Name: {}'.format(latest_cluster_backup.name))
    # Boolean showing completion - ie all nodes' backup succeeded
    is_complete = latest_cluster_backup.is_complete()
    logging.info('- Finished: {}'.format(is_complete))

    logging.info('- Details - Node counts')

    # Node count for successful backups
    complete_nodes_count = len(latest_cluster_backup.complete_nodes())
    logging.info('- Complete backup: {} nodes have completed the backup'.format(complete_nodes_count))

    # Node count for incomplete backups
    incomplete_nodes = latest_cluster_backup.incomplete_nodes()
    incomplete_nodes_count = len(incomplete_nodes)
    if incomplete_nodes_count > 0:
        logging.info('- Incomplete backup: {} nodes have not completed the backup yet'.format(incomplete_nodes_count))
        logging.info('  These nodes are:')
        for node_backup in incomplete_nodes:
            logging.info('     {}'.format(node_backup.fqdn))

    # Known hosts not having backups
    missing_nodes = latest_cluster_backup.missing_nodes()
    missing_nodes_count = len(missing_nodes)
    if missing_nodes_count > 0:
        logging.info('- Missing backup: {} nodes are not running backups'.format(missing_nodes_count))
        logging.info('  These nodes are:')
        for missing_node in missing_nodes:
            logging.info('     {}'.format(missing_node))

    # Total size used for this backup (all nodes sum) and the corresponding number of files
    latest_cluster_backup_size = latest_cluster_backup.size()
    readable_backup_size = human_readable_size(latest_cluster_backup_size)
    logging.info('- Total size: {}'.format(readable_backup_size))
    number_of_files = latest_cluster_backup.num_objects()
    logging.info('- Total files: {}'.format(number_of_files))

    if push_metrics:
        tags = ['medusa-cluster-backup', 'complete-backups-node-count', latest_cluster_backup.name]
        monitoring.send(tags, complete_nodes_count)

        tags = ['medusa-cluster-backup', 'incomplete-backups-node-count', latest_cluster_backup.name]
        monitoring.send(tags, incomplete_nodes_count)

        tags = ['medusa-cluster-backup', 'missing-backups-node-count', latest_cluster_backup.name]
        monitoring.send(tags, missing_nodes_count)

        tags = ['medusa-cluster-backup', 'backup-total-size', latest_cluster_backup.name]
        monitoring.send(tags, latest_cluster_backup_size)

        tags = ['medusa-cluster-backup', 'backup-total-file-count', latest_cluster_backup.name]
        monitoring.send(tags, number_of_files)


def human_readable_size(num, suffix='B'):
    for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
        if abs(num) < 1024.0:
            return "{:.2f} {}{}".format(num, unit, suffix)
        num /= 1024.0
    return "{:.2f} {}{}".format(num, 'Yi', suffix)


def get_latest_complete_cluster_backup(config):
    with Storage(config=config.storage) as storage:
        return storage.latest_complete_cluster_backup()
