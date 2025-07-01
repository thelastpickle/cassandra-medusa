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
import json
import pathlib
import shutil
import sys

from medusa.storage import Storage
from medusa.storage.abstract_storage import AbstractStorage
from medusa.filtering import filter_fqtns


def download_data(storageconfig, backup, fqtns_to_restore, destination):

    manifest = json.loads(backup.manifest)

    _check_available_space(manifest, destination)

    with Storage(config=storageconfig) as storage:

        for section in manifest:

            fqtn = "{}.{}".format(section['keyspace'], section['columnfamily'])
            dst = destination / section['keyspace'] / section['columnfamily']
            srcs = ['{}{}'.format(storage.storage_driver.get_path_prefix(backup.data_path), obj['path'])
                    for obj in section['objects']]

            if len(srcs) > 0 and (len(fqtns_to_restore) == 0 or fqtn in fqtns_to_restore):
                logging.debug('Downloading  %s files to %s', len(srcs), dst)

                dst.mkdir(parents=True)

                # check for hidden sub-folders in the table directory
                # (e.g. secondary indices which live in table/.table_idx)
                dst_subfolders = {dst / src.parent.name
                                  for src in map(pathlib.Path, srcs)
                                  if src.parent.name.startswith('.')}
                # create the sub-folders so the downloads actually work
                for subfolder in dst_subfolders:
                    subfolder.mkdir(parents=False)

                storage.storage_driver.download_blobs(srcs, dst)

            elif len(srcs) == 0 and (len(fqtns_to_restore) == 0 or fqtn in fqtns_to_restore):
                logging.debug('There is nothing to download for {}'.format(fqtn))
            else:
                logging.debug('Download of {} was not requested, skipping'.format(fqtn))

        logging.info('Downloading backup metadata...')
        storage.storage_driver.download_blobs(
            srcs=['{}'.format(path)
                  for path in [backup.manifest_path,
                               backup.schema_path,
                               backup.tokenmap_path]],
            dest=destination
        )


def download_cmd(config, backup_name, download_destination, keyspaces, tables, ignore_system_keyspaces):

    with Storage(config=config.storage) as storage:

        if not download_destination.is_dir():
            logging.error('{} is not a directory'.format(download_destination))
            sys.exit(1)

        node_backup = storage.get_node_backup(fqdn=storage.config.fqdn, name=backup_name)
        if not node_backup.exists():
            logging.error('No such backup')
            sys.exit(1)

        fqtns_to_download, _ = filter_fqtns(keyspaces, tables, node_backup.manifest, ignore_system_keyspaces)
        download_data(config.storage, node_backup, fqtns_to_download, download_destination)


def _check_available_space(manifest, destination):
    download_size = _get_download_size(manifest)
    available_space = _get_available_size(destination)
    logging.debug(f'Download size: {download_size}, available space: {available_space}')
    if download_size > available_space:
        missing = int(download_size) - int(available_space)
        logging.error(
            f'Directory {destination} does not have enough space to download backup of size {download_size}'
            f'(Missing roughly {AbstractStorage.human_readable_size(missing)})'
        )
        logging.error(
            'Please add --temp-dir pointing to a directory with enough space to your restore command '
            '(or change where --download-destination of your download command points to).'
        )
        raise RuntimeError('Not enough space available')


def _get_download_size(manifest):
    return sum([int(obj['size']) for section in manifest for obj in section['objects']])


def _get_available_size(destination_dir):
    pathlib.Path(destination_dir).mkdir(parents=True, exist_ok=True)
    return shutil.disk_usage(destination_dir).free
