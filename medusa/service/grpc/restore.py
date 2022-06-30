# -*- coding: utf-8 -*-
# Copyright 2020- Datastax, Inc. All rights reserved.
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
import os
import sys
from collections import defaultdict
from pathlib import Path

import medusa.config
import medusa.restore_node
import medusa.listing
from medusa.service.grpc.server import RESTORE_MAPPING_LOCATION


def create_config(config_file_path):
    config_file = Path(config_file_path)
    conf = medusa.config.load_config(defaultdict(lambda: None), config_file)
    return conf


def configure_console_logging(config):
    root_logger = logging.getLogger('')
    root_logger.setLevel(logging.DEBUG)

    log_format = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s')

    console_handler = logging.StreamHandler()
    console_handler.setLevel(getattr(logging, config.level))
    console_handler.setFormatter(log_format)
    root_logger.addHandler(console_handler)

    if console_handler.level > logging.DEBUG:
        # Disable debugging logging for external libraries
        for logger_name in 'urllib3', 'google_cloud_storage.auth.transport.requests', 'paramiko', 'cassandra':
            logging.getLogger(logger_name).setLevel(logging.WARN)


if __name__ == '__main__':
    if len(sys.argv) > 3:
        config_file_path = sys.argv[2]
        restore_key = sys.argv[3]
    else:
        logging.error("Usage: {} <config_file_path> <restore_key>".format(sys.argv[0]))
        sys.exit(1)

    in_place = True
    if os.path.exists(f"{RESTORE_MAPPING_LOCATION}/{restore_key}"):
        logging.info(f"Reading mapping file {RESTORE_MAPPING_LOCATION}/{restore_key}")
        with open(f"{RESTORE_MAPPING_LOCATION}/{restore_key}", 'r') as f:
            mapping = json.load(f)
            # Mapping json structure will look like:
            # {'in_place': true,
            #  'host_map':
            #       {'172.24.0.3': {'source': ['172.24.0.3'], 'seed': False},
            #        '127.0.0.1': {'source': ['172.24.0.4'], 'seed': False},
            #        '172.24.0.6': {'source': ['172.24.0.6'], 'seed': False}}}
            # As each mapping is specific to a Cassandra node, we're looking for the node that maps to 127.0.0.1,
            # which will be different for each pod.
            # If hostname resolving is turned on, we're looking for the localhost key instead.
            print(f"Mapping: {mapping}")
            if "localhost" in mapping["host_map"].keys():
                os.environ["POD_IP"] = mapping["host_map"]["localhost"]["source"][0]
            elif "127.0.0.1" in mapping["host_map"].keys():
                os.environ["POD_IP"] = mapping["host_map"]["127.0.0.1"]["source"][0]
            elif "::1" in mapping["host_map"].keys():
                os.environ["POD_IP"] = mapping["host_map"]["::1"]["source"][0]
            in_place = mapping["in_place"]
            if not in_place and "POD_IP" not in os.environ.keys():
                print("Could not find target node mapping for this pod while performing remote restore. Exiting.")
                sys.exit(1)

    config = create_config(config_file_path)
    configure_console_logging(config.logging)

    backup_name = os.environ["BACKUP_NAME"]
    tmp_dir = Path("/tmp") if "MEDUSA_TMP_DIR" not in os.environ else Path(os.environ["MEDUSA_TMP_DIR"])
    print(f"Downloading backup {backup_name} to {tmp_dir}")
    keep_auth = False if in_place else True
    seeds = None
    verify = False
    keyspaces = {}
    tables = {}
    use_sstableloader = False

    cluster_backups = list(medusa.listing.get_backups(config, True))
    logging.info(f"Found {len(cluster_backups)} backups in the cluster")
    backup_found = False
    # Checking if the backup exists for the node we're restoring.
    # Skipping restore if it doesn't exist.
    for cluster_backup in cluster_backups:
        if cluster_backup.name == backup_name:
            backup_found = True
            logging.info("Starting restore of backup {}".format(backup_name))
            medusa.restore_node.restore_node(config, tmp_dir, backup_name, in_place, keep_auth,
                                             seeds, verify, keyspaces, tables, use_sstableloader)
            logging.info("Finished restore of backup {}".format(backup_name))
            break

    if not backup_found:
        logging.info("Skipped restore of missing backup {}".format(backup_name))
