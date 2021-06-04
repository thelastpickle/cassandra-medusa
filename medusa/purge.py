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
import sys
import traceback

from datetime import datetime, timedelta

import medusa.utils
from medusa.index import clean_backup_from_index
from medusa.monitoring import Monitoring
from medusa.storage import Storage, format_bytes_str


def main(config, max_backup_age=0, max_backup_count=0):
    backups_to_purge = set()
    monitoring = Monitoring(config=config.monitoring)

    try:
        logging.info('Starting purge')
        storage = Storage(config=config.storage)
        # Get all backups for the local node
        logging.info('Listing backups for {}'.format(config.storage.fqdn))
        backup_index = storage.list_backup_index_blobs()
        backups = list(storage.list_node_backups(fqdn=config.storage.fqdn, backup_index_blobs=backup_index))
        # list all backups to purge based on date conditions
        backups_to_purge |= set(backups_to_purge_by_age(backups, max_backup_age))
        # list all backups to purge based on count conditions
        backups_to_purge |= set(backups_to_purge_by_count(backups, max_backup_count))
        # purge all candidate backups
        purge_backups(storage, backups_to_purge, config.storage.backup_grace_period_in_days)

        logging.debug('Emitting metrics')
        tags = ['medusa-node-backup', 'purge-error', 'PURGE-ERROR']
        monitoring.send(tags, 0)
    except Exception as e:
        traceback.print_exc()
        tags = ['medusa-node-backup', 'purge-error', 'PURGE-ERROR']
        monitoring.send(tags, 1)
        logging.error('This error happened during the purge: {}'.format(str(e)))
        sys.exit(1)


def backups_to_purge_by_age(backups, max_backup_age):
    if max_backup_age > 0:
        max_date = datetime.now() - timedelta(days=max_backup_age)
        return list(filter(lambda backup: backup.started < max_date.timestamp(), backups))
    return list()


def backups_to_purge_by_count(backups, max_backup_count):
    if max_backup_count > 0 and len(backups) > max_backup_count:
        # once we have all the backups, we sort them by their start time. we get oldest ones first
        sorted_node_backups = sorted(
            # before sorting the backups, ensure we can work out at least their start time
            filter(lambda nb: nb.started is not None, backups),
            key=lambda nb: nb.started
        )
        backups_to_remove_count = len(sorted_node_backups) - max_backup_count
        return sorted_node_backups[:backups_to_remove_count]
    return list()

# def backups_to_purge_by_name(backups, backup_name):
#     """
#     Return a list
#     Returns the list of the backups to delete for a given name (1 name = 1 backup, but on N nodes).
#     """
#     return list(filter(lambda backup: backup.name = backup_name, backups)) or list()


def purge_backups(storage, backups, backup_grace_period_in_days):
    """
    Core function to purge a set of node_backups
    Used for node purge and backup delete (using a specific backup_name)
    """
    logging.info("{} backups are candidate to be purged".format(len(backups)))
    fqdns = set()
    nb_objects_purged = 0
    total_purged_size = 0

    for backup in backups:
        (purged_objects, purged_size) = purge_backup(storage, backup)
        nb_objects_purged += purged_objects
        total_purged_size += purged_size
        fqdns.add(backup.fqdn)

    for fqdn in fqdns:
        (cleaned_objects_count, cleaned_objects_size) = cleanup_obsolete_files(storage,
                                                                               fqdn,
                                                                               backup_grace_period_in_days)
        nb_objects_purged += cleaned_objects_count
        total_purged_size += cleaned_objects_size

    logging.info("Purged {} objects with a total size of {}".format(
        nb_objects_purged,
        format_bytes_str(total_purged_size))
    )


def purge_backup(storage, backup):
    purged_objects = 0
    purged_size = 0
    logging.info("Purging backup {} from node {}..."
                 .format(backup.name, backup.fqdn))
    objects = storage.storage_driver.list_objects(backup.backup_path)

    for obj in objects:
        logging.debug("Purging {}".format(obj.name))
        purged_objects += 1
        purged_size += obj.size
        storage.storage_driver.delete_object(obj)

    clean_backup_from_index(storage, backup)

    return (purged_objects, purged_size)


def cleanup_obsolete_files(storage, fqdn, backup_grace_period_in_days):
    logging.info("Cleaning up orphaned files for {}...".format(fqdn))
    nb_objects_purged = 0
    total_purged_size = 0

    backups = storage.list_node_backups(fqdn=fqdn)
    paths_in_manifest = get_file_paths_from_manifests_for_differential_backups(backups)
    paths_in_storage = get_file_paths_from_storage(storage, fqdn, backup_grace_period_in_days)

    for path in paths_in_storage - paths_in_manifest:
        logging.debug("  - [{}] exists in storage, but not in manifest".format(path))
        obj = storage.storage_driver.get_blob(path)
        if obj is not None:
            nb_objects_purged += 1
            total_purged_size += int(obj.size)
            storage.storage_driver.delete_object(obj)

    return nb_objects_purged, total_purged_size


def get_file_paths_from_storage(storage, fqdn, backup_grace_period_in_days):
    data_directory = "{}{}/data".format(storage.prefix_path, fqdn)
    data_files = {
        blob.name: blob
        for blob in filter_files_within_gc_grace(storage.storage_driver,
                                                 storage.storage_driver.list_objects(str(data_directory)),
                                                 backup_grace_period_in_days)
    }

    return set(data_files.keys())


def filter_files_within_gc_grace(storage_driver, blobs, backup_grace_period_in_days):
    return list(
        filter(lambda blob:
               is_older_than_gc_grace(storage_driver.get_object_datetime(blob), backup_grace_period_in_days), blobs))


def is_older_than_gc_grace(blob_datetime, gc_grace) -> bool:
    return datetime.timestamp(blob_datetime) <= datetime.timestamp(datetime.now()) - (int(gc_grace) * 86400)


def get_file_paths_from_manifests_for_differential_backups(backups):
    differential_backups = filter_differential_backups(backups)

    manifests = list(map(lambda backup: json.loads(backup.manifest), differential_backups))

    objects_in_manifests = [
        obj
        for manifest in manifests
        for columnfamily_manifest in manifest
        for obj in columnfamily_manifest['objects']
    ]

    paths_in_manifest = {
        "{}".format(obj['path'])
        for obj in objects_in_manifests
    }

    return paths_in_manifest


def filter_differential_backups(backups):
    return list(filter(lambda backup: backup.is_differential is True, backups))


def delete_backup(config, backup_name, all_nodes):
    backups_to_purge = list()
    monitoring = Monitoring(config=config.monitoring)

    try:
        storage = Storage(config=config.storage)
        cluster_backup = storage.get_cluster_backup(backup_name)
        backups_to_purge = cluster_backup.node_backups.values()

        if not all_nodes:
            backups_to_purge = [nb for nb in backups_to_purge if storage.config.fqdn in nb.fqdn]

        logging.info('Deleting Backup {}...'.format(backup_name))
        purge_backups(storage, backups_to_purge, config.storage.backup_grace_period_in_days)

        logging.debug('Emitting metrics')
        tags = ['medusa-node-backup', 'delete-error', 'DELETE-ERROR']
        monitoring.send(tags, 0)
    except Exception as e:
        tags = ['medusa-node-backup', 'delete-error', 'DELETE-ERROR']
        monitoring.send(tags, 1)
        medusa.utils.handle_exception(
            e,
            'This error happened during the delete of backup "{}": {}'.format(backup_name, str(e)),
            config
        )
