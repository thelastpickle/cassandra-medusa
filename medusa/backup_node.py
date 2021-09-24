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
import psutil
import time
import traceback

from libcloud.storage.providers import Provider
from retrying import retry

import medusa.utils
from medusa.cassandra_utils import Cassandra
from medusa.index import add_backup_start_to_index, add_backup_finish_to_index, set_latest_backup_in_index
from medusa.monitoring import Monitoring
from medusa.storage.google_storage import GSUTIL_MAX_FILES_PER_CHUNK
from medusa.storage import Storage, format_bytes_str, ManifestObject, divide_chunks


class NodeBackupCache(object):
    NEVER_BACKED_UP = ['manifest.json', 'schema.cql']

    def __init__(self, *, node_backup, differential_mode, enable_md5_checks,
                 storage_driver, storage_provider, storage_config):
        if node_backup:
            self._node_backup_cache_is_differential = node_backup.is_differential
            self._backup_name = node_backup.name
            self._bucket_name = node_backup.storage.config.bucket_name
            self._data_path = node_backup.data_path
            self._cached_objects = {
                (section['keyspace'], section['columnfamily']): {
                    self._sanitize_file_path(pathlib.Path(object['path'])): object
                    for object in section['objects']
                }
                for section in json.loads(node_backup.manifest)
            }
            self._differential_mode = differential_mode
        else:
            self._node_backup_cache_is_differential = False
            self._backup_name = None
            self._bucket_name = None
            self._data_path = ''
            self._cached_objects = {}
            self._differential_mode = False
        self._replaced = 0
        self._storage_driver = storage_driver
        self._storage_provider = storage_provider
        self._storage_config = storage_config
        self._enable_md5_checks = enable_md5_checks

    def _sanitize_file_path(self, path):
        # Secondary indexes are stored as subdirectories to the base table, starting with a dot.
        # In order to avoid mixing 2i sstables with the base table sstables, the file name isn't enough
        # to perform the comparison on differential backups. We need to retain the subdir name for 2i tables.
        if path.parts[-2].startswith('.'):
            return os.path.join(path.parts[-2], path.parts[-1])
        else:
            return path.name

    @property
    def replaced(self):
        return self._replaced

    @property
    def backup_name(self):
        return self._backup_name

    def replace_or_remove_if_cached(self, *, keyspace, columnfamily, srcs):
        retained = list()
        skipped = list()
        path_prefix = self._storage_driver.get_path_prefix(self._data_path)
        for src in srcs:
            if src.name in self.NEVER_BACKED_UP:
                pass
            else:
                fqtn = (keyspace, columnfamily)
                cached_item = None
                if self._storage_provider == Provider.GOOGLE_STORAGE or self._differential_mode is True:
                    cached_item = self._cached_objects.get(fqtn, {}).get(self._sanitize_file_path(src))

                threshold = self._storage_config.multi_part_upload_threshold
                if cached_item is None or not self._storage_driver.file_matches_cache(src,
                                                                                      cached_item,
                                                                                      threshold,
                                                                                      self._enable_md5_checks):
                    # We have no matching object in the cache matching the file
                    retained.append(src)
                else:
                    # File was already present in the previous backup
                    # In case the backup isn't differential or the cache backup isn't differential, copy from cache
                    if self._differential_mode is False and self._node_backup_cache_is_differential is False:
                        prefixed_path = '{}{}'.format(path_prefix, cached_item['path'])
                        cached_item_path = self._storage_driver.get_cache_path(prefixed_path)
                        retained.append(cached_item_path)
                    # This backup is differential, but the cached one wasn't
                    # We must re-upload the files according to the differential format
                    elif self._differential_mode is True and self._node_backup_cache_is_differential is False:
                        retained.append(src)
                    else:
                        # in case the backup is differential, we want to rule out files, not copy them from cache
                        manifest_object = self._make_manifest_object(path_prefix, cached_item)
                        logging.debug("Skipping upload of {} which was already part of the previous backup"
                                      .format(cached_item['path']))
                        skipped.append(manifest_object)
                    self._replaced += 1

        return retained, skipped

    def _make_manifest_object(self, path_prefix, cached_item):
        return ManifestObject('{}{}'.format(path_prefix, cached_item['path']), cached_item['size'], cached_item['MD5'])


def throttle_backup():
    """
    Makes sure to only us idle IO for backups
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


def main(config, backup_name_arg, stagger_time, enable_md5_checks_flag, mode):
    start = datetime.datetime.now()
    backup_name = backup_name_arg or start.strftime('%Y%m%d%H%M')
    monitoring = Monitoring(config=config.monitoring)

    try:
        storage = Storage(config=config.storage)
        cassandra = Cassandra(config)

        differential_mode = False
        if mode == "differential":
            differential_mode = True

        node_backup = storage.get_node_backup(
            fqdn=config.storage.fqdn,
            name=backup_name,
            differential_mode=differential_mode
        )

        if node_backup.exists():
            raise IOError('Error: Backup {} already exists'.format(backup_name))

        # Make sure that priority remains to Cassandra/limiting backups resource usage
        try:
            throttle_backup()
        except Exception:
            logging.warning("Throttling backup impossible. It's probable that ionice is not available.")

        logging.info('Saving tokenmap and schema')
        schema, tokenmap = get_schema_and_tokenmap(cassandra)

        node_backup.schema = schema
        node_backup.tokenmap = json.dumps(tokenmap)
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

        actual_start = datetime.datetime.now()

        enable_md5 = enable_md5_checks_flag or medusa.utils.evaluate_boolean(config.checks.enable_md5_checks)
        num_files, node_backup_cache = do_backup(
            cassandra, node_backup, storage, differential_mode, enable_md5, config, backup_name)

        end = datetime.datetime.now()
        actual_backup_duration = end - actual_start

        print_backup_stats(actual_backup_duration, actual_start, end, node_backup, node_backup_cache, num_files, start)

        update_monitoring(actual_backup_duration, backup_name, monitoring, node_backup)
        return (actual_backup_duration, actual_start, end, node_backup, node_backup_cache, num_files, start)

    except Exception as e:
        tags = ['medusa-node-backup', 'backup-error', backup_name]
        monitoring.send(tags, 1)
        medusa.utils.handle_exception(
            e,
            "This error happened during the backup: {}".format(str(e)),
            config
        )


# Wait 2^i * 10 seconds between each retry, up to 2 minutes between attempts, which is right after the
# attempt on which it waited for 60 seconds
@retry(stop_max_attempt_number=7, wait_exponential_multiplier=10000, wait_exponential_max=120000)
def get_schema_and_tokenmap(cassandra):
    with cassandra.new_session() as cql_session:
        schema = cql_session.dump_schema()
        tokenmap = cql_session.tokenmap()
    return schema, tokenmap


def do_backup(cassandra, node_backup, storage, differential_mode, enable_md5_checks,
              config, backup_name):

    # Load last backup as a cache
    node_backup_cache = NodeBackupCache(
        node_backup=storage.latest_node_backup(fqdn=config.storage.fqdn),
        differential_mode=differential_mode,
        enable_md5_checks=enable_md5_checks,
        storage_driver=storage.storage_driver,
        storage_provider=storage.storage_provider,
        storage_config=config.storage
    )

    logging.info('Starting backup')

    # the cassandra snapshot we use defines __exit__ that cleans up the snapshot
    # so even if exception is thrown, a new snapshot will be created on the next run
    # this is not too good and we will use just one snapshot in the future
    logging.info('Creating snapshot')
    with cassandra.create_snapshot(backup_name) as snapshot:
        manifest = []
        num_files = backup_snapshots(storage, manifest, node_backup, node_backup_cache, snapshot)

    logging.info('Updating backup index')
    node_backup.manifest = json.dumps(manifest)
    add_backup_finish_to_index(storage, node_backup)
    set_latest_backup_in_index(storage, node_backup)

    return num_files, node_backup_cache


def print_backup_stats(actual_backup_duration, actual_start, end, node_backup, node_backup_cache, num_files, start):
    logging.info('Backup done')

    logging.info("""- Started: {:%Y-%m-%d %H:%M:%S}
                        - Started extracting data: {:%Y-%m-%d %H:%M:%S}
                        - Finished: {:%Y-%m-%d %H:%M:%S}""".format(start, actual_start, end))

    logging.info('- Real duration: {} (excludes time waiting '
                 'for other nodes)'.format(actual_backup_duration))

    logging.info('- {} files, {}'.format(
        num_files,
        format_bytes_str(node_backup.size())
    ))

    logging.info('- {} files copied from host'.format(
        num_files - node_backup_cache.replaced
    ))

    if node_backup_cache.backup_name is not None:
        logging.info('- {} copied from previous backup ({})'.format(
            node_backup_cache.replaced,
            node_backup_cache.backup_name
        ))


def update_monitoring(actual_backup_duration, backup_name, monitoring, node_backup):
    logging.debug('Emitting metrics')

    tags = ['medusa-node-backup', 'backup-duration', backup_name]
    monitoring.send(tags, actual_backup_duration.seconds)

    tags = ['medusa-node-backup', 'backup-size', backup_name]
    monitoring.send(tags, node_backup.size())

    tags = ['medusa-node-backup', 'backup-error', backup_name]
    monitoring.send(tags, 0)

    logging.debug('Done emitting metrics')


def backup_snapshots(storage, manifest, node_backup, node_backup_cache, snapshot):
    try:
        num_files = 0
        for snapshot_path in snapshot.find_dirs():
            logging.debug("Backing up {}".format(snapshot_path))

            (needs_backup, already_backed_up) = node_backup_cache.replace_or_remove_if_cached(
                keyspace=snapshot_path.keyspace,
                columnfamily=snapshot_path.columnfamily,
                srcs=list(snapshot_path.list_files()))

            num_files += len(needs_backup) + len(already_backed_up)

            dst_path = str(node_backup.datapath(keyspace=snapshot_path.keyspace,
                                                columnfamily=snapshot_path.columnfamily))
            logging.debug("destination path: {}".format(dst_path))

            manifest_objects = list()
            if len(needs_backup) > 0:
                # If there is a plenty of files to upload it should be
                # splitted to batches due to 'gsutil cp' which
                # can't handle too much source files via STDIN.
                for src_batch in divide_chunks(needs_backup, GSUTIL_MAX_FILES_PER_CHUNK):
                    manifest_objects += storage.storage_driver.upload_blobs(src_batch, dst_path)

            # Reintroducing already backed up objects in the manifest in differential
            for obj in already_backed_up:
                manifest_objects.append(obj)

            manifest.append(make_manifest_object(node_backup.fqdn, snapshot_path, manifest_objects, storage))

        return num_files
    except Exception as e:
        logging.error('This error happened during the backup: {}'.format(str(e)))
        traceback.print_exc()
        raise e


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
