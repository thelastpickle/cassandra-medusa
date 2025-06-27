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
import shlex
import traceback

import medusa.storage

from medusa.cassandra_utils import Cassandra


def update_backup_index(storage, node_backup):
    """
    Called when a backup happens, this method adds an entry about this backup to the backup index
    and sets this backups as the latest backup.
    """
    add_backup_start_to_index(storage, node_backup)
    add_backup_finish_to_index(storage, node_backup)
    set_latest_backup_in_index(storage, node_backup)


def build_indices(config, noop):
    """
    One-off function to construct the backup index.
    This function lists all complete cluster backups and all node backups in them.
    For all node backups found this way, it will find the latest one per node and update index accordingly.
    """
    try:
        with medusa.storage.Storage(config=config.storage) as storage:
            is_ccm = int(shlex.split(config.cassandra.is_ccm)[0])
            all_backups = []

            if is_ccm != 1:
                cassandra = Cassandra(config)
                with cassandra.new_session() as cql_session:
                    tokenmap = cql_session.tokenmap()
                for fqdn in tokenmap.keys():
                    logging.info("processing {}".format(fqdn))
                    all_backups.extend(storage.discover_node_backups(fqdn=fqdn))
            else:
                all_backups = list(storage.discover_node_backups())

            latest_node_backups = {}
            if noop:
                logging.info('--noop was set, will only print the indices')

            for node_backup in all_backups:
                latest_node_backups = process_backup(node_backup, latest_node_backups, storage, noop)

            if not noop:
                for fqdn, node_backup in latest_node_backups.items():
                    logging.debug('Latest backup {} is {}'.format(fqdn, node_backup.name))
                    set_latest_backup_in_index(storage, node_backup)

    except Exception:
        traceback.print_exc()
        sys.exit(1)


def process_backup(node_backup, latest_node_backups, storage, noop):
    """
    Process a single node backup and update the indices accordingly.
    """
    if node_backup.finished is None:
        return latest_node_backups

    logging.debug('Found backup {} from {}'.format(node_backup.name, node_backup.fqdn))

    latest = latest_node_backups.get(node_backup.fqdn, node_backup)
    if node_backup.finished >= latest.finished:
        latest_node_backups[node_backup.fqdn] = node_backup

    if not noop:
        add_backup_start_to_index(storage, node_backup)
        add_backup_finish_to_index(storage, node_backup)

    return latest_node_backups


def add_backup_start_to_index(storage, node_backup):
    dst = '{}index/backup_index/{}/tokenmap_{}.json'.format(
        storage.prefix_path, node_backup.name, node_backup.fqdn)
    storage.storage_driver.upload_blob_from_string(dst, node_backup.tokenmap)
    dst = '{}index/backup_index/{}/schema_{}.cql'.format(
        storage.prefix_path, node_backup.name, node_backup.fqdn)
    storage.storage_driver.upload_blob_from_string(dst, node_backup.schema)
    dst = '{}index/backup_index/{}/started_{}_{}.timestamp'.format(
        storage.prefix_path, node_backup.name, node_backup.fqdn, node_backup.started
    )

    storage.storage_driver.upload_blob_from_string(dst, str(node_backup.started))

    if node_backup.is_differential is True:
        dst = '{}index/backup_index/{}/differential_{}'.format(storage.prefix_path, node_backup.name, node_backup.fqdn)
        storage.storage_driver.upload_blob_from_string(dst, 'differential')


def add_backup_finish_to_index(storage, node_backup):
    dst = '{}index/backup_index/{}/manifest_{}.json'.format(storage.prefix_path, node_backup.name, node_backup.fqdn)
    storage.storage_driver.upload_blob_from_string(dst, node_backup.manifest)
    dst = '{}index/backup_index/{}/finished_{}_{}.timestamp'.format(
        storage.prefix_path, node_backup.name, node_backup.fqdn, node_backup.finished
    )
    storage.storage_driver.upload_blob_from_string(dst, str(node_backup.finished))


def set_latest_backup_in_index(storage, node_backup):
    dst = '{}index/latest_backup/{}/tokenmap.json'.format(storage.prefix_path, node_backup.fqdn)
    storage.storage_driver.upload_blob_from_string(dst, node_backup.tokenmap)
    dst = '{}index/latest_backup/{}/backup_name.txt'.format(storage.prefix_path, node_backup.fqdn)
    storage.storage_driver.upload_blob_from_string(dst, node_backup.name)


def clean_backup_from_index(storage, node_backup):
    index_files = storage.storage_driver.list_objects(
        "{}index/backup_index/{}".format(storage.prefix_path, node_backup.name))
    for obj in index_files:
        if "_" + node_backup.fqdn in obj.name:
            logging.debug("Cleaning from backup index: {}".format(obj.name))
            storage.storage_driver.delete_object(obj)


def index_exists(storage):
    return len(storage.storage_driver.list_objects(path='{}index'.format(storage.prefix_path))) > 0
