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
import sys

from medusa.storage import Storage, divide_chunks
from medusa.storage.google_storage import GSUTIL_MAX_FILES_PER_CHUNK
from medusa.filtering import filter_fqtns


def download_data(storageconfig, backup, fqtns_to_restore, destination):
    storage = Storage(config=storageconfig)
    manifest = json.loads(backup.manifest)

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

            for src_batch in divide_chunks(srcs, GSUTIL_MAX_FILES_PER_CHUNK):
                storage.storage_driver.download_blobs(src_batch, dst)
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
    storage = Storage(config=config.storage)

    if not download_destination.is_dir():
        logging.error('{} is not a directory'.format(download_destination))
        sys.exit(1)

    node_backup = storage.get_node_backup(fqdn=storage.config.fqdn, name=backup_name)
    if not node_backup.exists():
        logging.error('No such backup')
        sys.exit(1)

    fqtns_to_download, _ = filter_fqtns(keyspaces, tables, node_backup.manifest, ignore_system_keyspaces)
    download_data(config.storage, node_backup, fqtns_to_download, download_destination)
