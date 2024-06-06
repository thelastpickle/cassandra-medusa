# -*- coding: utf-8 -*-
# Copyright 2018 Spotify AB
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

import itertools
import logging
import operator
import pathlib
import re
import typing as t

from retrying import retry

import medusa.index

from medusa.storage.cluster_backup import ClusterBackup
from medusa.storage.node_backup import NodeBackup
from medusa.storage.abstract_storage import ManifestObject, AbstractBlob
from medusa.storage.google_storage import GoogleStorage
from medusa.storage.local_storage import LocalStorage
from medusa.storage.s3_storage import S3Storage
from medusa.storage.s3_rgw import S3RGWStorage
from medusa.storage.azure_storage import AzureStorage
from medusa.storage.s3_base_storage import S3BaseStorage
from medusa.utils import evaluate_boolean


# pattern meant to match just the blob name, not the entire path
# the path is covered by the initial .*
# also retains extension if the name has any
INDEX_BLOB_NAME_PATTERN = re.compile('.*(tokenmap|schema|manifest|differential|incremental)_(.*)$')
INDEX_BLOB_WITH_TIMESTAMP_PATTERN = re.compile('.*(started|finished)_(.*)_([0-9]+).timestamp$')


def divide_chunks(values, step):
    """
    Yield successive step-sized chunks from values.
    :param values: A list of items to split into sub-lists
    :param step: The size of sub-lists
    :return: A list of lists of maximum 'step' size.
    """
    for i in range(0, len(values), step):
        yield values[i:i + step]


def format_bytes_str(value):
    for unit_shift, unit in enumerate(['B', 'KB', 'MB', 'GB', 'TB']):
        if value >> (unit_shift * 10) < 1024:
            break
    return '{:.2f} {}'.format(value / (1 << (unit_shift * 10)), unit)


class Storage(object):
    def __init__(self, *, config):
        self._config = config
        # Used to bypass dependency checks when running in Kubernetes
        self._k8s_mode = evaluate_boolean(config.k8s_mode) if config.k8s_mode else False
        self._prefix = pathlib.Path(config.prefix or '.')
        self.prefix_path = str(self._prefix) + '/' if len(str(self._prefix)) > 1 else ''
        self.storage_driver = self._load_storage()
        self.storage_provider = self._config.storage_provider

    def __enter__(self):
        self.storage_driver.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.storage_driver.disconnect()

    def _load_storage(self):
        logging.debug('Loading storage_provider: {}'.format(self._config.storage_provider))
        if self._config.storage_provider.lower() == 'google_storage':
            google_storage = GoogleStorage(self._config)
            return google_storage
        elif self._config.storage_provider.lower() == 'azure_blobs':
            azure_storage = AzureStorage(self._config)
            return azure_storage
        elif self._config.storage_provider.lower() == 's3_rgw':
            return S3RGWStorage(self._config)
        elif self._config.storage_provider.lower() == "s3_compatible":
            s3_storage = S3BaseStorage(self._config)
            return s3_storage
        elif self._config.storage_provider.lower().startswith('s3'):
            s3_storage = S3Storage(self._config)
            return s3_storage
        elif self._config.storage_provider.lower() == 'local':
            return LocalStorage(self._config)
        elif self._config.storage_provider.lower() == "ibm_storage":
            s3_storage = S3BaseStorage(self._config)
            return s3_storage

        raise NotImplementedError("Unsupported storage provider")

    @property
    def config(self):
        return self._config

    @retry(stop_max_attempt_number=7, wait_exponential_multiplier=10000, wait_exponential_max=120000)
    def get_node_backup(self, *, fqdn, name, differential_mode=False):
        return NodeBackup(
            storage=self,
            name=name,
            fqdn=fqdn,
            differential_mode=differential_mode
        )

    def discover_node_backups(self, *, fqdn=None):
        """
        Discovers nodes backups by traversing data folders.
        This operation is very taxing for cloud backends and should be avoided.
        We keep it in the codebase for the sole reason of allowing the compute-backup-indices to work.
        """

        def get_backup_name_from_blob(blob):
            blob_path = pathlib.Path(blob.name)
            if self.prefix_path == '':
                fqdn, name, *_ = blob_path.parts
            else:
                _, fqdn, name, *_ = blob_path.parts
            return fqdn, name

        def is_schema_blob(blob):
            return blob.name.endswith('/schema.cql')

        def includes_schema_blob(blobs):
            return any(map(is_schema_blob, blobs))

        prefix_path = fqdn if fqdn else ''

        logging.debug("Listing blobs with prefix '{}'".format(prefix_path))

        storage_objects = filter(
            lambda blob: "meta" in blob.name,
            self.storage_driver.list_objects(path=prefix_path)
        )

        all_blobs = sorted(storage_objects, key=operator.attrgetter('name'))

        logging.debug("Finished listing blobs")

        for (fqdn, backup_name), blobs in itertools.groupby(all_blobs, key=get_backup_name_from_blob):
            # consume the _blobs_ iterator into a list because we need to traverse it twice
            backup_blobs = list(blobs)
            if includes_schema_blob(backup_blobs):
                logging.debug("Found backup {}.{}".format(fqdn, backup_name))
                yield NodeBackup(storage=self, fqdn=fqdn, name=backup_name, preloaded_blobs=backup_blobs)

    def list_node_backups(self, *, fqdn=None, backup_index_blobs=None):
        """
        Lists node backups using the index.
        If there is no backup index, no backups will be found.
        Use discover_node_backups to discover backups from the data folders.
        """

        def is_tokenmap_file(blob):
            return "tokenmap" in blob.name

        def get_blob_name(blob):
            return blob.name

        def get_all_backup_blob_names(blobs):
            # if the tokenmap file exists, we assume the whole backup exists too
            all_backup_blobs = filter(is_tokenmap_file, blobs)
            return list(map(get_blob_name, all_backup_blobs))

        def get_blobs_for_fqdn(blobs, fqdn):
            return list(filter(lambda b: f'_{fqdn}.' in b, blobs))

        if backup_index_blobs is None:
            backup_index_blobs = self.list_backup_index_blobs()

        blobs_by_backup = self.group_backup_index_by_backup_and_node(backup_index_blobs)

        all_backup_blob_names = get_all_backup_blob_names(backup_index_blobs)

        if len(all_backup_blob_names) == 0:
            logging.info('No backups found in index. Consider running "medusa build-index" if you have some backups')

        # possibly filter out backups only for given fqdn
        if fqdn is not None:
            relevant_backup_names = get_blobs_for_fqdn(all_backup_blob_names, fqdn)
        else:
            relevant_backup_names = all_backup_blob_names

        # use the backup names and fqdns from index entries to construct NodeBackup objects
        node_backups = list()
        for backup_index_entry in relevant_backup_names:
            if self.prefix_path == '':
                # no prefix in the buckets
                _, _, backup_name, tokenmap_file = backup_index_entry.split('/')
            else:
                # prefix is being used for multi tenancy in the cluster
                _, _, _, backup_name, tokenmap_file = backup_index_entry.split('/')

            # tokenmap file is in format 'tokenmap_fqdn.json'
            tokenmap_fqdn = self.get_fqdn_from_any_index_blob(tokenmap_file)
            manifest_blob, schema_blob, tokenmap_blob = None, None, None
            started_blob, finished_blob = None, None
            started_timestamp, finished_timestamp = None, None
            if tokenmap_fqdn in blobs_by_backup[backup_name]:
                manifest_blob = self.lookup_blob(blobs_by_backup, backup_name, tokenmap_fqdn, 'manifest')
                schema_blob = self.lookup_blob(blobs_by_backup, backup_name, tokenmap_fqdn, 'schema')
                tokenmap_blob = self.lookup_blob(blobs_by_backup, backup_name, tokenmap_fqdn, 'tokenmap')
                started_blob = self.lookup_blob(blobs_by_backup, backup_name, tokenmap_fqdn, 'started')
                finished_blob = self.lookup_blob(blobs_by_backup, backup_name, tokenmap_fqdn, 'finished')
                differential_blob = self.lookup_blob(blobs_by_backup, backup_name, tokenmap_fqdn, 'differential')
                # Should be removed after while. Here for backwards compatibility.
                incremental_blob = self.lookup_blob(blobs_by_backup, backup_name, tokenmap_fqdn, 'incremental')
                if started_blob is not None:
                    started_timestamp = self.get_timestamp_from_blob_name(started_blob.name)
                else:
                    started_timestamp = None
                if finished_blob is not None:
                    finished_timestamp = self.get_timestamp_from_blob_name(finished_blob.name)
                else:
                    finished_timestamp = None

            nb = NodeBackup(storage=self, fqdn=tokenmap_fqdn, name=backup_name,
                            manifest_blob=manifest_blob, schema_blob=schema_blob, tokenmap_blob=tokenmap_blob,
                            started_timestamp=started_timestamp, started_blob=started_blob,
                            finished_timestamp=finished_timestamp, finished_blob=finished_blob,
                            differential_blob=differential_blob if differential_blob is not None else incremental_blob)
            node_backups.append(nb)

        # once we have all the backups, we sort them by their start time. we get oldest ones first
        sorted_node_backups = sorted(
            # before sorting the backups, ensure we can work out at least their start time
            filter(lambda nb: nb.started is not None, node_backups),
            key=lambda nb: nb.started
        )

        # then, before returning the backups, we pick only the existing ones
        previous_existed = False
        for node_backup in sorted_node_backups:
            # we try to be smart here - once we have seen an existing one, we assume all later ones exist too
            if previous_existed:
                yield node_backup
                continue

            # the idea is to save .exist() calls as they actually go to the storage backend and cost something
            # this is mostly meant to handle the transition period when backups expire before the index does,
            # which is a consequence of the transition period and running the build-index command

            if node_backup.exists():
                previous_existed = True
                yield node_backup
            else:
                logging.debug('Backup {} for fqdn {} present only in index'.format(node_backup.name, node_backup.fqdn))
                # if a backup doesn't exist, we should remove its entry from the index too
                try:
                    self.remove_backup_from_index(node_backup)
                except Exception:
                    logging.debug(
                        'This account cannot perform the cleanup_storage'
                        '{} for fqdn {} present only in index.'
                        'Ignoring and continuing...'
                        .format(node_backup.name, node_backup.fqdn))

    def list_backup_index_blobs(self):
        path = '{}index/backup_index'.format(self.prefix_path)
        return self.storage_driver.list_objects(path)

    def list_root_blobs(self):
        return self.storage_driver.list_objects(self.prefix_path)

    def group_backup_index_by_backup_and_node(self, backup_index_blobs):

        def get_backup_name(blob):
            blob_name_chunks = blob.name.split('/')
            return blob_name_chunks[2] if len(str(self.prefix_path)) <= 1 else blob_name_chunks[3]

        def name_and_fqdn(blob):
            return get_backup_name(blob), Storage.get_fqdn_from_any_index_blob(blob)

        def group_by_backup_name(blobs):
            return itertools.groupby(blobs, get_backup_name)

        def group_by_fqdn(blobs):
            return itertools.groupby(blobs, Storage.get_fqdn_from_any_index_blob)

        def has_proper_name(blob):
            blob_name_chunks = blob.name.split('/')
            is_proper = len(blob_name_chunks) == 4 if len(str(self.prefix_path)) <= 1 else len(blob_name_chunks) == 5
            if not is_proper:
                logging.warning('File {} in backup index has improper name'.format(blob.name))
            return is_proper

        blobs_by_backup = {}
        properly_named_index_blobs = filter(
            has_proper_name,
            backup_index_blobs
        )
        sorted_backup_index_blobs = sorted(
            properly_named_index_blobs,
            key=name_and_fqdn
        )

        for backup_name, blobs in group_by_backup_name(sorted_backup_index_blobs):
            blobs_by_node = {}
            for fqdn, node_blobs in group_by_fqdn(blobs):
                blobs_by_node[fqdn] = list(node_blobs)
            blobs_by_backup[backup_name] = blobs_by_node

        return blobs_by_backup

    @staticmethod
    def get_fqdn_from_any_index_blob(blob):
        if not isinstance(blob, str):
            blob_name = blob.name
        else:
            blob_name = blob
        # it's important to check in this order, because the 2nd pattern is more generic
        match = INDEX_BLOB_WITH_TIMESTAMP_PATTERN.match(blob_name)
        if match is None:
            match = INDEX_BLOB_NAME_PATTERN.match(blob_name)
        assert match is not None, 'Encountered malformed index blob name {}'.format(blob_name)
        return Storage.remove_extension(match.group(2))

    @staticmethod
    def remove_extension(fqdn_with_extension):
        replaces = {
            '.json': '',
            '.cql': '',
            '.txt': '',
            '.timestamp': ''
        }
        r = fqdn_with_extension
        for old, new in replaces.items():
            r = r.replace(old, new)
        return r

    @staticmethod
    def get_timestamp_from_blob_name(blob_name):
        match = INDEX_BLOB_WITH_TIMESTAMP_PATTERN.match(blob_name)
        assert match is not None, 'Encountered malformed index blob name with timestamp {}'.format(blob_name)
        return int(match.group(3))

    def lookup_blob(self, blobs_by_backup, backup_name, fqdn, blob_name_chunk):
        """
        This function looks up blobs in blobs_by_backup, which is a double dict (k->k->v).
        The blob_name_chunk tells which blob for given backup and fqdn we want.
        It can be 'schema', 'manifest', 'started', 'finished'
        """
        blob_list = list(filter(lambda blob: blob_name_chunk in blob.name,
                                blobs_by_backup[backup_name][fqdn]))
        return blob_list[0] if len(blob_list) > 0 else None

    def list_cluster_backups(self, backup_index=None):
        node_backups = sorted(
            self.list_node_backups(backup_index_blobs=backup_index),
            key=lambda b: (b.name, b.started)
        )

        for name, node_backups in itertools.groupby(node_backups, key=operator.attrgetter('name')):
            yield ClusterBackup(name, node_backups)

    def latest_node_backup(self, *, fqdn):
        index_path = '{}index/latest_backup/{}/backup_name.txt'.format(self.prefix_path, fqdn)
        try:
            latest_backup_name = self.storage_driver.get_blob_content_as_string(index_path)
            differential_blob = self.storage_driver.get_blob(
                '{}{}/{}/meta/differential'.format(self.prefix_path, fqdn, latest_backup_name))
            # Should be removed after while. Here for backwards compatibility.
            incremental_blob = self.storage_driver.get_blob(
                '{}{}/{}/meta/incremental'.format(self.prefix_path, fqdn, latest_backup_name))

            node_backup = NodeBackup(
                storage=self,
                fqdn=fqdn,
                name=latest_backup_name,
                differential_blob=differential_blob if differential_blob is not None else incremental_blob
            )

            if not node_backup.exists():
                logging.warning('Latest backup points to non-existent backup. Deleting the marker')
                self.remove_latest_backup_marker(fqdn)
                raise Exception

            return node_backup

        except Exception:
            logging.info('Node {} does not have latest backup'.format(fqdn))
            return None

    def latest_cluster_backup(self, backup_index=None):
        """
        Get the latest backup attempted (successful or not)
        """
        last_started = max(
            self.list_cluster_backups(backup_index=backup_index),
            key=operator.attrgetter('started'),
            default=None
        )

        logging.debug("Last cluster backup : {}".format(last_started))
        return last_started

    def latest_complete_cluster_backup(self, backup_index=None):
        """
        Get the latest *complete* backup (ie successful on all nodes)
        """
        finished_backups = filter(
            operator.attrgetter('finished'),
            self.list_cluster_backups(backup_index=backup_index)
        )

        last_finished = max(finished_backups, key=operator.attrgetter('finished'), default=None)
        return last_finished

    def get_cluster_backup(self, backup_name):
        for cluster_backup in self.list_cluster_backups():
            if cluster_backup.name == backup_name:
                return cluster_backup
        raise KeyError('The backup {} does not exist'.format(backup_name))

    def remove_backup_from_index(self, node_backup):
        """
        Takes a node backup and tries to remove corresponding items from the index.
        This usually happens when the node_backup.exists() returns false, which means it's schema in the
        meta folder does not exist.
        We are checking and deleting each blob separately because there is no easy way to list and get the objects.
        """
        medusa.index.clean_backup_from_index(self, node_backup)

    def remove_latest_backup_marker(self, fqdn):
        """
        Removes the markers of the latest backup for a fqdn.
        Unlike remove_backup_from_index, here we do call list because the path is not ambiguous, and because we can't
        get the blobs from anywhere.
        Then we can call the delete object on the results.
        """
        markers = self.storage_driver.list_objects('{}index/latest_backup/{}/'.format(self.prefix_path, fqdn))
        for marker in markers:
            self.storage_driver.delete_object(marker)

    def delete_objects(self, objects, concurrent_transfers=None):
        self.storage_driver.delete_objects(objects, concurrent_transfers)

    @staticmethod
    def sanitize_keyspace_and_table_name(path: pathlib.Path) -> t.Tuple[str, str]:
        """
        This functions makes sure that for a given path (being it a local or storage one), we identify a SSTable file
        together with its parent, which might sometime be a secondary index.
        Secondary indices live as hidden folder in the regular data folder, and contain regular SSTable files.
        Similar logic applies to DSE (6.8) internal files.

        When dealing with local paths, the path is a string like
        /some/path/to/data/folder/keyspace/table-cfid/snapshots/snapshot-name/nb-5-big-CompressionInfo.db
        or
        /some/path/to/data/folder/keyspace/table-cfid/snapshots/snapshot-name/.index_name/nb-5-big-CompressionInfo.db
        """
        # 2i tables or the dse internal folder, we merge table and index name as a new table
        # we're dealing with local path, which features a snapshot
        if str(path).startswith('/'):
            is_2i_or_dse = path.parent.name.startswith('.') or path.parent.name.endswith('nodes')
            chunks = str(path).split('/')
            if (len(chunks) < 7 and is_2i_or_dse) or len(chunks) < 6:
                raise RuntimeError(f'Path {path} does not look like a correct SSTable location')
            if is_2i_or_dse:
                k, t, index_name = chunks[-6], chunks[-5], chunks[-2]
                keyspace, table = k, f"{t}.{index_name}"
            else:
                keyspace, table = chunks[-5], chunks[-4]
        # it's a path in a storage, without a snapshot
        else:
            if path.parent.name.startswith('.') or path.parent.name.endswith('nodes'):
                keyspace, table = path.parent.parent.parent.name, f"{path.parent.parent.name}.{path.parent.name}"
            else:
                keyspace, table = path.parent.parent.name, path.parent.name
        return keyspace, table

    @staticmethod
    def get_keyspace_and_table(manifest_object: ManifestObject) -> t.Tuple[str, str, ManifestObject]:
        p = pathlib.Path(manifest_object.path)
        # 2i tables or the dse internal folder, we merge table and index name as a new table
        keyspace, table = Storage.sanitize_keyspace_and_table_name(p)
        return keyspace, table, manifest_object

    @staticmethod
    def _get_table_prefix(config_prefix, fqdn):
        if config_prefix is not None and config_prefix != '':
            prefix = f"{config_prefix}/"
        else:
            prefix = ""
        return f"{prefix}{fqdn}/data/"

    def list_files_per_table(self) -> t.Dict[str, t.Dict[str, t.Set[ManifestObject]]]:
        fdns_data_prefix = self._get_table_prefix(self.config.prefix, self.config.fqdn)
        all_blobs: t.List[AbstractBlob] = self.storage_driver.list_blobs(prefix=fdns_data_prefix)
        all_files = [ManifestObject(blob.name, blob.size, blob.hash) for blob in all_blobs]
        keyspace_table_mo_tuples = map(Storage.get_keyspace_and_table, all_files)

        files_by_keyspace_and_table = dict()
        for ks, ks_files in itertools.groupby(keyspace_table_mo_tuples, lambda t: t[0]):
            files_by_keyspace_and_table[ks] = dict()
            for tt, t_files in itertools.groupby(ks_files, lambda tf: tf[1]):
                files_by_keyspace_and_table[ks][tt] = {pathlib.Path(tf[2].path).name: tf[2] for tf in t_files}

        return files_by_keyspace_and_table
