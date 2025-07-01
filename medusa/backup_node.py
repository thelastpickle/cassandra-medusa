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
import json
import logging
import os
import pathlib
import time
import traceback
import typing as t
import psutil

from retrying import retry

import medusa.utils
from medusa.backup_manager import BackupMan
from medusa.cassandra_utils import Cassandra
from medusa.index import add_backup_start_to_index, add_backup_finish_to_index, set_latest_backup_in_index
from medusa.monitoring import Monitoring
from medusa.storage import Storage, format_bytes_str, NodeBackup
from medusa.storage.abstract_storage import ManifestObject


def throttle_backup():
    """
    Makes sure to only use idle IO for backups
    """
    p = psutil.Process(os.getpid())
    p.ionice(psutil.IOPRIO_CLASS_IDLE)
    p.nice(19)
    logging.debug("Process {} was set to use only idle IO and CPU resources".format(p))


def stagger(fqdn, storage, tokenmap):
    """
    Checks whether the previous node in the tokenmap has completed a backup.

    :param tokenmap:
    :param storage:
    :param fqdn:
    :return: True if this host has sufficiently been staggered, False otherwise.
    """
    # If we already have a backup for ourselves, bail early.
    previous_backups = storage.list_node_backups(fqdn=fqdn)
    if any(backup.finished for backup in previous_backups):
        return True

    ordered_tokenmap = sorted(tokenmap.items(), key=lambda item: item[1]['tokens'])
    index = ordered_tokenmap.index((fqdn, tokenmap[fqdn]))
    if index == 0:  # Always run if we're the first node
        return True

    previous_host = ordered_tokenmap[index - 1][0]
    previous_host_backups = storage.list_node_backups(fqdn=previous_host)
    has_backup = any(backup.finished for backup in previous_host_backups)
    if not has_backup:
        logging.info('Still waiting for {} to finish a backup.'.format(previous_host))

    return has_backup


# Called by async thread for backup, called in main thread as synchronous backup.
# Kicks off the node backup unit of work and registers for backup queries.
# No return value for async mode, throws back exception for failed kickoff.
def handle_backup(config, backup_name_arg, stagger_time, enable_md5_checks_flag, mode, keep_snapshot=False,
                  use_existing_snapshot=False):
    start = datetime.datetime.now()
    backup_name = backup_name_arg or start.strftime('%Y%m%d%H%M')
    monitoring = Monitoring(config=config.monitoring)

    # externalise the fact a backup is running.
    # we need a way to tell a backup is running without invoking Meudsa to prevent concurrency and abrupt termination
    backup_in_progress_marker = medusa.utils.MedusaTempFile()
    if backup_in_progress_marker.exists():
        marker_path = backup_in_progress_marker.get_path()
        raise IOError(
            f'Error: Backup already in progress. Please delete f{marker_path} if that is not the case to continue.'
        )
    else:
        backup_in_progress_marker.create()

    with Storage(config=config.storage) as storage:
        try:
            logging.debug("Starting backup preparations with Mode: {}".format(mode))
            cassandra = Cassandra(config)

            differential_mode = False
            if mode == "differential":
                differential_mode = True

            node_backup = storage.get_node_backup(
                fqdn=config.storage.fqdn,
                name=backup_name,
                differential_mode=differential_mode
            )
            if node_backup.exists() and not use_existing_snapshot:
                raise IOError('Error: Backup {} already exists'.format(backup_name))

            # Starting the backup
            logging.info("Starting backup using Stagger: {} Mode: {} Name: {}".format(stagger_time, mode, backup_name))
            BackupMan.update_backup_status(backup_name, BackupMan.STATUS_IN_PROGRESS)
            info = start_backup(storage, node_backup, cassandra, differential_mode, stagger_time, start, mode,
                                enable_md5_checks_flag, backup_name, config, monitoring, keep_snapshot,
                                use_existing_snapshot)
            BackupMan.update_backup_status(backup_name, BackupMan.STATUS_SUCCESS)

            logging.debug("Done with backup, returning backup result information")
            return (info["actual_backup_duration"], info["actual_start_time"], info["end_time"],
                    info["node_backup"], info["num_files"], info["num_replaced"], info["num_kept"],
                    info["start_time"], info["backup_name"])

        except Exception as e:
            logging.error("Issue occurred inside handle_backup Name: {} Error: {}".format(backup_name, str(e)))
            BackupMan.update_backup_status(backup_name, BackupMan.STATUS_FAILED)

            tags = ['medusa-node-backup', 'backup-error', backup_name]
            monitoring.send(tags, 1)
            medusa.utils.handle_exception(
                e,
                "Error occurred during backup: {}".format(str(e)),
                config
            )
        finally:
            backup_in_progress_marker.delete()


def start_backup(storage, node_backup, cassandra, differential_mode, stagger_time, start, mode,
                 enable_md5_checks_flag, backup_name, config, monitoring, keep_snapshot=False,
                 use_existing_snapshot=False):

    if use_existing_snapshot and not cassandra.snapshot_exists(backup_name):
        raise IOError(
            "Error: Snapshot {} does not exist and use_existing_snapshot is True. Please create the snapshot first."
            .format(backup_name)
        )
    try:
        # Make sure that priority remains to Cassandra/limiting backups resource usage
        throttle_backup()
    except Exception:
        logging.warning("Throttling backup impossible. It's probable that ionice is not available.")

    logging.info('Saving tokenmap and schema')
    schema, tokenmap = get_schema_and_tokenmap(cassandra)
    node_backup.schema = schema
    node_backup.tokenmap = json.dumps(tokenmap)

    logging.info('Saving server version')
    server_type, release_version = get_server_type_and_version(cassandra)
    node_backup.server_version = json.dumps({'server_type': server_type, 'release_version': release_version})

    if differential_mode is True:
        node_backup.differential = mode

    add_backup_start_to_index(storage, node_backup)

    if stagger_time:
        stagger_end = start + stagger_time
        logging.info('Staggering backup run, trying until {}'.format(stagger_end))
        while not stagger(config.storage.fqdn, storage, tokenmap):
            if datetime.datetime.now() < stagger_end:
                logging.info('Staggering this backup run...')
                time.sleep(60)
            else:
                raise IOError('Backups on previous nodes did not complete'
                              ' within our stagger time.')

    # Perform the actual backup
    actual_start = datetime.datetime.now()
    enable_md5 = enable_md5_checks_flag or medusa.utils.evaluate_boolean(config.checks.enable_md5_checks)
    num_files, num_replaced, num_kept = do_backup(
        cassandra, node_backup, storage, enable_md5, backup_name, keep_snapshot, use_existing_snapshot
    )
    end = datetime.datetime.now()
    actual_backup_duration = end - actual_start

    print_backup_stats(actual_backup_duration, actual_start, end, node_backup, num_files, num_replaced, num_kept, start)
    update_monitoring(actual_backup_duration, backup_name, monitoring, node_backup)
    return {
        "actual_backup_duration": actual_backup_duration,
        "actual_start_time": actual_start,
        "end_time": end,
        "node_backup": node_backup,
        "num_files": num_files,
        "num_replaced": num_replaced,
        "num_kept": num_kept,
        "start_time": start,
        "backup_name": backup_name
    }


# Wait 2^i * 10 seconds between each retry, up to 2 minutes between attempts, which is right after the
# attempt on which it waited for 60 seconds
@retry(stop_max_attempt_number=7, wait_exponential_multiplier=10000, wait_exponential_max=120000)
def get_schema_and_tokenmap(cassandra):
    with cassandra.new_session() as cql_session:
        schema = cql_session.dump_schema()
        tokenmap = cql_session.tokenmap()
    return schema, tokenmap


@retry(stop_max_attempt_number=7, wait_exponential_multiplier=10000, wait_exponential_max=120000)
def get_server_type_and_version(cassandra):
    with cassandra.new_session() as cql_session:
        server_type, release_version = cql_session.get_server_type_and_release_version()
    return server_type, release_version


def do_backup(cassandra, node_backup, storage, enable_md5_checks, backup_name, keep_snapshot=False,
              use_existing_snapshot=False):

    if use_existing_snapshot:
        logging.debug('Skipping snapshot creation')
        logging.debug("Getting snapshot")
        snapshot = cassandra.get_snapshot(backup_name, keep_snapshot)
    else:
        logging.debug("Creating snapshot")
        snapshot = cassandra.create_snapshot(backup_name, keep_snapshot)

    # the cassandra snapshot we use defines __exit__ that cleans up the snapshot
    # so even if exception is thrown, a new snapshot will be created on the next run
    # this is not too good and we will use just one snapshot in the future
    with snapshot:
        manifest = []
        num_files, num_replaced, num_kept = backup_snapshots(
            storage, manifest, node_backup, snapshot, enable_md5_checks
        )

    if node_backup.is_dse:
        logging.info('Creating DSE snapshot')
        with cassandra.create_dse_snapshot(backup_name) as snapshot:
            dse_num_files, dse_replaced, dse_kept = backup_snapshots(
                storage, manifest, node_backup, snapshot, enable_md5_checks
            )
            num_files += dse_num_files
            num_replaced += dse_replaced
            num_kept += dse_kept

    logging.info('Updating backup index')
    node_backup.manifest = json.dumps(manifest)
    add_backup_finish_to_index(storage, node_backup)
    set_latest_backup_in_index(storage, node_backup)
    return num_files, num_replaced, num_kept


def print_backup_stats(
        actual_backup_duration, actual_start, end, node_backup, num_files, num_replaced, num_kept, start
):
    logging.info('Backup done')

    logging.info("""- Started: {:%Y-%m-%d %H:%M:%S}
                        - Started extracting data: {:%Y-%m-%d %H:%M:%S}
                        - Finished: {:%Y-%m-%d %H:%M:%S}""".format(start, actual_start, end))

    logging.info('- Real duration: {} (excludes time waiting '
                 'for other nodes)'.format(actual_backup_duration))

    logging.info('- {} files, {}'.format(
        num_files + num_kept,
        format_bytes_str(node_backup.size())
    ))

    logging.info('- {} files copied from host ({} new, {} reuploaded)'.format(
        num_files, num_files - num_replaced, num_replaced
    ))

    if node_backup.name is not None:
        logging.info('- {} kept from previous backup ({})'.format(
            num_kept,
            node_backup.name
        ))


def update_monitoring(actual_backup_duration, backup_name, monitoring, node_backup):
    logging.debug('Emitting metrics')

    tags = ['medusa-node-backup', 'backup-duration', backup_name]
    monitoring.send(tags, actual_backup_duration.total_seconds())

    tags = ['medusa-node-backup', 'backup-size', backup_name]
    monitoring.send(tags, node_backup.size())

    tags = ['medusa-node-backup', 'backup-error', backup_name]
    monitoring.send(tags, 0)

    logging.debug('Done emitting metrics')


def backup_snapshots(storage, manifest, node_backup, snapshot, enable_md5_checks):
    try:
        num_files = 0
        replaced = 0
        kept = 0
        multipart_threshold = storage.config.multi_part_upload_threshold

        if node_backup.is_differential:
            logging.info(f'Listing already backed up files for node {node_backup.fqdn}')
            files_in_storage = storage.list_files_per_table()
        else:
            files_in_storage = {}

        for snapshot_path in snapshot.find_dirs():
            fqtn = f"{snapshot_path.keyspace}.{snapshot_path.columnfamily}"
            logging.info(f"Backing up {fqtn}")

            needs_backup, needs_reupload, already_backed_up = check_already_uploaded(
                storage=storage,
                node_backup=node_backup,
                files_in_storage=files_in_storage,
                multipart_threshold=multipart_threshold,
                enable_md5_checks=enable_md5_checks,
                keyspace=snapshot_path.keyspace,
                srcs=list(snapshot_path.list_files()))

            replaced += len(needs_reupload)
            kept += len(already_backed_up)
            num_files += len(needs_backup) + len(needs_reupload)

            dst_path = str(node_backup.datapath(
                keyspace=snapshot_path.keyspace,
                columnfamily=snapshot_path.columnfamily)
            )
            logging.debug("Snapshot destination path: {}".format(dst_path))

            manifest_objects = []
            needs_upload = needs_backup + needs_reupload
            if len(needs_upload) > 0:
                manifest_objects += storage.storage_driver.upload_blobs(needs_upload, dst_path)

            # inform about fixing backups
            if len(needs_reupload) > 0:
                logging.info(
                    f"Re-uploaded {len(needs_reupload)} files in {fqtn} because they were not found in storage"
                )

            # Reintroducing already backed up objects in the manifest in differential
            if len(already_backed_up) > 0 and node_backup.is_differential:
                logging.info(
                    f"Skipping upload of {len(already_backed_up)} files in {fqtn} because they are already in storage"
                )
                for obj in already_backed_up:
                    manifest_objects.append(obj)

            manifest.append(make_manifest_object(node_backup.fqdn, snapshot_path, manifest_objects, storage))

        return num_files, replaced, kept
    except Exception as e:
        logging.error('Error occurred during backup: {}'.format(str(e)))
        traceback.print_exc()
        raise e


def check_already_uploaded(
        storage: Storage,
        node_backup: NodeBackup,
        multipart_threshold: int,
        enable_md5_checks: bool,
        files_in_storage: t.Dict[str, t.Dict[str, t.Dict[str, ManifestObject]]],
        keyspace: str,
        srcs: t.List[pathlib.Path]
) -> tuple[t.List[pathlib.Path], t.List[pathlib.Path], t.List[ManifestObject]]:

    NEVER_BACKED_UP = ['manifest.json', 'schema.cql']
    needs_backup = []
    needs_reupload = []
    already_backed_up = []

    # in full mode we upload always everything
    if node_backup.is_differential is False:
        return [src for src in srcs if src.name not in NEVER_BACKED_UP], needs_reupload, already_backed_up

    keyspace_files_in_storage = files_in_storage.get(keyspace, {})

    for src in srcs:
        if src.name in NEVER_BACKED_UP:
            continue
        else:
            # safe_table_name is either a table, or a "table.2i_name"
            _, safe_table_name = Storage.sanitize_keyspace_and_table_name(src)
            item_in_storage = keyspace_files_in_storage.get(safe_table_name, {}).get(src.name, None)
            # object is not in storage
            if item_in_storage is None:
                needs_backup.append(src)
                continue
            # object is in storage but with different size or digest
            storage_driver = storage.storage_driver
            if not storage_driver.file_matches_storage(src, item_in_storage, multipart_threshold, enable_md5_checks):
                needs_reupload.append(src)
                continue
            # object is in storage with correct size and digest
            already_backed_up.append(item_in_storage)

    return needs_backup, needs_reupload, already_backed_up


def make_manifest_object(fqdn, snapshot_path, manifest_objects, storage):
    return {
        'keyspace': snapshot_path.keyspace,
        'columnfamily': snapshot_path.columnfamily,
        'objects': [{
            'path': url_to_path(manifest_object.path, fqdn, storage),
            'MD5': manifest_object.MD5,
            'size': manifest_object.size,
        } for manifest_object in manifest_objects]
    }


def url_to_path(url, fqdn, storage):
    # the path with store in the manifest starts with the fqdn, but we can get longer urls
    # depending on the storage provider and type of backup
    # Full backup path is : (<prefix>/)<fqdn>/<backup_name>/data/<keyspace>/<table>/...
    # Differential backup path is : (<prefix>/)<fqdn>/data/<keyspace>/<table>/...
    url_parts = url.split('/')
    return storage.prefix_path + ('/'.join(url_parts[url_parts.index(fqdn):]))
