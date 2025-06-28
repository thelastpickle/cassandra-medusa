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
        with Storage(config=config.storage) as storage:
            # Get all backups for the local node
            logging.info('Listing backups for {}'.format(config.storage.fqdn))
            backup_index = storage.list_backup_index_blobs()
            backups = list(storage.list_node_backups(fqdn=config.storage.fqdn, backup_index_blobs=backup_index))
            # list all backups to purge based on date conditions
            backups_to_purge |= set(backups_to_purge_by_age(backups, max_backup_age))
            # list all backups to purge based on count conditions
            backups_to_purge |= set(backups_to_purge_by_count(backups, max_backup_count))
            # purge all candidate backups
            object_counts = purge_backups(
                storage, backups_to_purge, config.storage.backup_grace_period_in_days, config.storage.fqdn
            )
            nb_objects_purged, total_purged_size, total_objects_within_grace = object_counts

            logging.debug('Emitting metrics')
            tags = ['medusa-node-backup', 'purge-error', 'PURGE-ERROR']
            monitoring.send(tags, 0)

        return nb_objects_purged, total_purged_size, total_objects_within_grace, len(backups_to_purge)

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
    return []


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
    return []


def purge_backups(storage, backups, backup_grace_period_in_days, local_fqdn):
    """
    Core function to purge a set of node_backups
    Used for node purge and backup delete (using a specific backup_name)
    """
    logging.info("{} backups are candidate to be purged".format(len(backups)))
    fqdns = set()
    nb_objects_purged = 0
    total_purged_size = 0
    total_objects_within_grace = 0

    for backup in backups:
        (purged_objects, purged_size) = purge_backup(storage, backup)
        nb_objects_purged += purged_objects
        total_purged_size += purged_size
        fqdns.add(backup.fqdn)

    if len(fqdns) == 0:
        # If we didn't purge any backup, we still want to cleanup obsolete files for the local node
        fqdns.add(local_fqdn)

    for fqdn in fqdns:
        (cleaned_objects_count, cleaned_objects_size, nb_objects_within_grace) \
            = cleanup_obsolete_files(storage,
                                     fqdn,
                                     backup_grace_period_in_days)
        nb_objects_purged += cleaned_objects_count
        total_purged_size += cleaned_objects_size
        total_objects_within_grace += nb_objects_within_grace

    logging.info("Purged {} objects with a total size of {}".format(
        nb_objects_purged,
        format_bytes_str(total_purged_size)))
    if total_objects_within_grace > 0:
        logging.info("{} objects within {} days grace period were not deleted".format(
            total_objects_within_grace,
            backup_grace_period_in_days
        ))

    return (nb_objects_purged, total_purged_size, total_objects_within_grace)


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
    paths_in_manifest = get_file_paths_from_manifests_for_complete_differential_backups(backups)
    paths_in_storage = get_file_paths_from_storage(storage, fqdn)

    deletion_candidates = set(paths_in_storage.keys()) - paths_in_manifest
    objects_to_delete = filter_files_within_gc_grace(storage,
                                                     deletion_candidates,
                                                     paths_in_storage,
                                                     backup_grace_period_in_days)
    for path in objects_to_delete:
        logging.debug("  - [{}] exists in storage, but not in manifest".format(path))
        obj = storage.storage_driver.get_blob(path)
        if obj is not None:
            nb_objects_purged += 1
            total_purged_size += int(obj.size)
            storage.storage_driver.delete_object(obj)

    nb_objects_within_grace = len(set(deletion_candidates) - set(objects_to_delete))

    return nb_objects_purged, total_purged_size, nb_objects_within_grace


def get_file_paths_from_storage(storage, fqdn):
    data_directory = "{}{}/data".format(storage.prefix_path, fqdn)
    data_files = {
        blob.name: blob
        for blob in storage.storage_driver.list_objects(str(data_directory))
    }

    return data_files


def filter_files_within_gc_grace(storage, blobs, paths_in_storage, backup_grace_period_in_days):
    return list(
        filter(lambda blob:
               is_older_than_gc_grace(storage.storage_driver.get_object_datetime(paths_in_storage[blob]),
                                      backup_grace_period_in_days),
               blobs))


def is_older_than_gc_grace(blob_datetime, gc_grace) -> bool:
    return datetime.timestamp(blob_datetime) <= datetime.timestamp(datetime.now()) - (int(gc_grace) * 86400)


def get_file_paths_from_manifests_for_complete_differential_backups(backups):
    differential_backups = filter_differential_backups(backups)
    complete_differential_backups = list(filter(lambda backup: backup.manifest is not None, differential_backups))

    manifests = [json.loads(backup.manifest) for backup in complete_differential_backups]

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


def backups_to_purge_by_name(storage, cluster_backups, backup_names_to_purge, all_nodes):
    """
    select NodeBackups that should be purged by backup name.
    Raises KeyError exception if some of the backup_names_to_purge do not exist.
    :param storage: storage object
    :param cluster_backups: list of ClusterBackups for cluster
    :param backup_names_to_purge: names of backups to purge
    :param all_nodes: purge for all nodes if true, otherwise for current node only
    :return: list of NodeBackups that should be purged
    """
    backups_to_purge = []
    cluster_backups_by_name = {bk.name: bk for bk in cluster_backups}
    for backup_name in backup_names_to_purge:
        if backup_name in cluster_backups_by_name:
            backups_to_purge.extend(cluster_backups_by_name[backup_name].node_backups.values())
        else:
            raise KeyError('The backup {} does not exist'.format(backup_name))

    if not all_nodes:
        backups_to_purge = [nb for nb in backups_to_purge if storage.config.fqdn == nb.fqdn]

    return backups_to_purge


def delete_backup(config, backup_names, all_nodes):
    monitoring = Monitoring(config=config.monitoring)

    try:
        with Storage(config=config.storage) as storage:
            cluster_backups = storage.list_cluster_backups()
            backups_to_purge = backups_to_purge_by_name(storage, cluster_backups, backup_names, all_nodes)

            logging.info('Deleting Backup(s) {}...'.format(",".join(backup_names)))
            purge_backups(storage, backups_to_purge, config.storage.backup_grace_period_in_days, storage.config.fqdn)

            logging.debug('Emitting metrics')
            tags = ['medusa-node-backup', 'delete-error', 'DELETE-ERROR']
            monitoring.send(tags, 0)
    except Exception as e:
        tags = ['medusa-node-backup', 'delete-error', 'DELETE-ERROR']
        monitoring.send(tags, 1)
        medusa.utils.handle_exception(
            e,
            'This error happened during the delete of backup(s) "{}": {}'.format(",".join(backup_names), str(e)),
            config
        )
