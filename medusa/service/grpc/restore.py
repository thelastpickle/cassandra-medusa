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
from medusa.storage import Storage


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


def apply_mapping_env():
    # By default we consider that we're restoring in place.
    in_place = True
    mapping = {}
    if "RESTORE_MAPPING" in os.environ.keys():
        logging.info("Reading restore mapping from environment variable")
        mapping = json.loads(os.environ["RESTORE_MAPPING"])
        # Mapping json structure will look like:
        # {'in_place': true,
        #  'host_map':
        #       {'test-dc1-sts-0': {'source': ['172.24.0.3'], 'seed': False},
        #        'test-dc1-sts-1': {'source': ['172.24.0.4'], 'seed': False},
        #        'test-dc1-sts-2': {'source': ['172.24.0.6'], 'seed': False}}}
        # As each mapping is specific to a Cassandra node, we're looking for
        # the node that maps to the value of the POD_NAME var.
        in_place = mapping["in_place"]
        if not in_place:
            print(f"Mapping: {mapping}")
            # While POD_IP isn't a great name, it's the env variable that is used to enforce the fqdn of the node.
            # This allows us to specify which node we're restoring from.
            if os.environ["POD_NAME"] in mapping["host_map"].keys():
                os.environ["POD_IP"] = mapping["host_map"][os.environ["POD_NAME"]]["source"][0]
                print(f"Restoring from {os.environ['POD_IP']}")
            else:
                print(f"POD_NAME {os.environ['POD_NAME']} not found in mapping")
                return None
    return in_place, mapping


def resolve_node_address(mapping: dict, config) -> str:
    pod_name = os.getenv("POD_NAME")
    pod_ip = os.getenv("POD_IP")

    host_map = (mapping or {}).get("host_map", {})
    if pod_name in host_map:
        sources = host_map[pod_name].get("source", [])
        if sources:
            return sources[0]

    if pod_ip:
        return pod_ip

    return config.storage.fqdn


def get_pitr_mapping(mapping: dict | None) -> dict | None:
    """Return the optional PITR block and reject a non-object value."""
    if mapping is None or "pitr" not in mapping:
        return None

    pitr_mapping = mapping["pitr"]
    if not isinstance(pitr_mapping, dict):
        raise ValueError("The 'pitr' field in RESTORE_MAPPING must be a JSON object")
    return pitr_mapping


def restore_backup(in_place, config, backup_name=None, mapping=None) -> str:
    pitr_mapping = get_pitr_mapping(mapping)
    node_address = None
    if backup_name is None:
        if pitr_mapping is not None:
            node_address = resolve_node_address(mapping, config)
            node_backups = pitr_mapping.get("node_backups", {})

            if node_address not in node_backups:
                raise ValueError(f"No PITR base backup for node {node_address}")
            backup_name = node_backups[node_address]
        else:
            backup_name = os.environ["BACKUP_NAME"]
    tmp_dir = Path("/tmp") if "MEDUSA_TMP_DIR" not in os.environ else Path(os.environ["MEDUSA_TMP_DIR"])
    print(f"Downloading backup {backup_name} to {tmp_dir}")
    keep_auth = False if in_place else True
    seeds = None
    verify = False
    keyspaces = {}
    tables = {}
    use_sstableloader = False

    with Storage(config=config.storage) as storage:
        cluster_backups = list(medusa.listing.get_backups(storage, config, False))
        logging.info(f"Found {len(cluster_backups)} backups in the cluster")
        # Checking if the backup exists for the node we're restoring.
        # Skipping restore if it doesn't exist.
        for cluster_backup in cluster_backups:
            if cluster_backup.name == backup_name:
                logging.info("Starting restore of backup {}".format(backup_name))
                medusa.restore_node.restore_node(config, tmp_dir, backup_name, in_place, keep_auth,
                                                 seeds, verify, keyspaces, tables, use_sstableloader)
                return f"Finished restore of backup {backup_name}"

    if pitr_mapping is not None:
        raise RuntimeError(f"Missing PITR base backup {backup_name} for node {node_address}")
    return f"Skipped restore of missing backup {backup_name}"


if __name__ == '__main__':
    if len(sys.argv) > 2:
        config_file_path = sys.argv[1]
        restore_key = sys.argv[2]
    else:
        logging.error("Usage: {} <config_file_path> <restore_key>".format(sys.argv[0]))
        sys.exit(1)

    mapping_result = apply_mapping_env()
    # If mapping_result is None, it means that there's no corresponding backup and we can skip the restore phase.
    if mapping_result is not None:
        in_place, mapping = mapping_result
        config = create_config(config_file_path)
        configure_console_logging(config.logging)

        output_message = restore_backup(in_place, config, mapping=mapping)
        logging.info(output_message)
