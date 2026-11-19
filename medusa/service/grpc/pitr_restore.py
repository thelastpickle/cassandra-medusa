# -*- coding: utf-8 -*-
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

"""Stage commitlogs and configure Cassandra archiving in the restore init container."""

import json
import logging
import os
import shutil
import sys
import time
from pathlib import Path

from medusa.pitr_restore import (
    download_commitlog_segments,
    find_commitlog_segments,
    find_next_backup,
    generate_commitlog_archiving_properties,
    get_pitr_target_timestamp_s,
)
from medusa.service.grpc.restore import configure_console_logging, create_config, get_pitr_mapping, resolve_node_address
from medusa.storage import Storage
from medusa.utils import evaluate_boolean


def load_restore_mapping():
    """Read the inline restore mapping shared by the shell and Python entrypoint."""
    mapping = json.loads(os.getenv("RESTORE_MAPPING", "{}"))
    if not isinstance(mapping, dict):
        raise ValueError("RESTORE_MAPPING must be a JSON object")
    return mapping


def pod_participates(mapping, pod_name):
    """Determine whether this pod is included in the restore."""
    return bool(mapping.get("in_place", True) or pod_name in mapping.get("host_map", {}))


def read_last_restore_key(path):
    """Read the key recorded by the shell, if a restore has completed."""
    try:
        return Path(path).read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        return ""


def stage_commitlogs(config, source_address, backup_name):
    """Download the source node's commitlogs within the assigned backup's restore window."""
    with Storage(config=config.storage) as storage:
        base_backup = storage.get_node_backup(fqdn=source_address, name=backup_name)
        after_snapshot_time_s = base_backup.started
        next_backup = find_next_backup(storage, source_address, base_backup)
        if next_backup is None:
            upper_bound_s = time.time()
        else:
            grace_period_s = int(config.pitr.commitlog_restore_grace_period_seconds)
            upper_bound_s = next_backup.snapshot_time + grace_period_s

        segments = find_commitlog_segments(
            storage,
            config.storage.prefix,
            source_address,
            after_snapshot_time_s,
            upper_bound_s=upper_bound_s,
        )
        download_commitlog_segments(
            storage,
            segments,
            config.pitr.commitlog_staging_dir,
        )


def main():
    """Prepare the local commitlogs and properties before Cassandra starts."""
    if len(sys.argv) < 2:
        logging.error("Usage: %s <config_file_path> [restore_key]", sys.argv[0])
        sys.exit(1)

    config_file_path = sys.argv[1]
    restore_key = sys.argv[2] if len(sys.argv) > 2 else os.getenv("RESTORE_KEY", "")

    mapping = load_restore_mapping()
    pitr_mapping = get_pitr_mapping(mapping)
    config = create_config(config_file_path)
    configure_console_logging(config.logging)

    if not evaluate_boolean(config.pitr.enabled):
        if pitr_mapping is not None:
            logging.error("PITR restore requested, but PITR is disabled")
            sys.exit(1)

        logging.info("PITR is disabled. Skipping commitlog configuration")
        return

    staging_dir = Path(config.pitr.commitlog_staging_dir)
    try:
        shutil.rmtree(staging_dir)
    except FileNotFoundError:
        pass
    staging_dir.mkdir(parents=True)

    last_restore_value = read_last_restore_key("/var/lib/cassandra/.last-restore")
    is_pitr_restore = bool(
        pitr_mapping is not None
        and restore_key
        and last_restore_value != restore_key
        and pod_participates(mapping, os.getenv("POD_NAME"))
    )
    archive_command = f"/bin/ln %path {config.pitr.commitlog_spool_dir}/%name"
    if is_pitr_restore:
        source_address = resolve_node_address(mapping, config)
        backup_name = pitr_mapping["node_backups"][source_address]
        target_timestamp_s = get_pitr_target_timestamp_s(pitr_mapping)

        stage_commitlogs(config, source_address, backup_name)

        properties = generate_commitlog_archiving_properties(
            archive_command=archive_command,
            restore_directories=config.pitr.commitlog_staging_dir,
            target_timestamp_s=target_timestamp_s,
        )
    else:
        properties = generate_commitlog_archiving_properties(
            archive_command=archive_command,
        )

    properties_path = Path(config.cassandra.config_file).parent / "commitlog_archiving.properties"
    properties_path.write_text(properties, encoding="utf-8")


if __name__ == '__main__':
    main()
