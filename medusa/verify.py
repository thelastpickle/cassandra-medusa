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
import medusa.utils

from medusa.storage import Storage


def verify(config, backup_name, enable_md5_checks_flag):
    with Storage(config=config.storage) as storage:
        enable_md5 = enable_md5_checks_flag or medusa.utils.evaluate_boolean(config.checks.enable_md5_checks)

        try:
            cluster_backup = storage.get_cluster_backup(backup_name)
        except KeyError:
            logging.error('No such backup')
            raise RuntimeError("Manifest validation failed")

        print('Validating {0.name} ...'.format(cluster_backup))

        if cluster_backup.is_complete():
            print('- Completion: OK!')
        else:
            print('- Completion: Not complete!')
            for incomplete_node in cluster_backup.incomplete_nodes():
                print('  - [{0.fqdn}] Backup started at {0.started}, but not finished yet'.format(incomplete_node))
            for fqdn in cluster_backup.missing_nodes():
                print('  - [{}] Backup missing'.format(fqdn))
            raise RuntimeError("Backup is incomplete")

        consistency_errors = [
            consistency_error
            for node_backup in cluster_backup.node_backups.values()
            for consistency_error in validate_manifest(storage, node_backup, enable_md5)
        ]

        if consistency_errors:
            print("- Manifest validation: Failed!")
            for error in consistency_errors:
                print(error)
            raise RuntimeError("Manifest validation failed")
        else:
            print("- Manifest validated: OK!!")


def validate_manifest(storage, node_backup, enable_md5_checks):
    """
    Goes through all files in the manifest for given backup.

    :return: iterable of errors (meaning problematic objects)
    """

    try:
        manifest = json.loads(node_backup.manifest)
    except Exception:
        logging.error('Unable to read manifest from storage')
        return

    data_path_prefix = storage.storage_driver.get_path_prefix(node_backup.data_path)

    objects_in_storage = {
        blob.name: blob
        for blob in storage.storage_driver.list_objects(node_backup.data_path)
        if '-Statistics.db' not in blob.name
    }

    objects_in_manifest = [
        obj
        for columnfamily_manifest in manifest
        for obj in columnfamily_manifest['objects']
        if '-Statistics.db' not in obj["path"]
    ]

    for object_in_manifest in objects_in_manifest:

        blob = objects_in_storage.get('{}{}'.format(data_path_prefix, object_in_manifest['path']))

        if blob is None:
            yield("  - [{}] Doesn't exists".format(object_in_manifest['path']))
            continue

        if not storage.storage_driver.blob_matches_manifest(blob, object_in_manifest, enable_md5_checks):
            if '-Summary.db' in blob.name:
                m = f"Blob [{blob.name}] mismatches manifest. "
                m += f"It's a Summary.db file which might be re-written once Cassandra rebuilds index summaries. "
                m += f"Therefore we are not causing this mismatch to fail the verification. "
                m += f"Cassandra will (re-)write this file during bootstrap if needed. "
                logging.warning(m)
                continue
            logging.error("Expected {} but got {}".format(object_in_manifest, blob))
            yield("  - [{}] Blob different".format(object_in_manifest['path']))

    # Checking for files existing in storage, but in not in the manifest
    # Relevant for full backups only because
    # Differential backups can have more files in data dir than in manifest
    if node_backup.is_differential is False:

        paths_in_storage = set(objects_in_storage.keys())

        paths_in_manifest = {
            "{}{}".format(data_path_prefix, obj['path'])
            for obj in objects_in_manifest
        }

        for path in paths_in_storage - paths_in_manifest:
            yield("  - [{}] exists in storage, but not in manifest".format(path))
