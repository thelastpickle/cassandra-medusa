#!/bin/bash
#
# While not limited to k8s environments, this script is designed for running
# Medusa in two containers in a Cassandra pod. One container runs the backup
# service alongside Cassandra, i.e., a sidecar container. The other container
# is an initContainer to perform restores. This script allows the same image
# to be used for both containers with the MEDUSA_MODE env var.

set -e

echo "MEDUSA_MODE = $MEDUSA_MODE"

restore() {
    echo "Running Medusa in restore mode"
    local last_restore_file="/var/lib/cassandra/.last-restore"
    local config_file=/etc/medusa/medusa.ini
    local restore_key="${RESTORE_KEY:-}"
    local restore_needed

    # Reject invalid mappings and cutoffs before changing Cassandra data.
    restore_needed=$(poetry run python -c '
import os
import sys

from medusa.pitr_utils import get_pitr_target_timestamp_s
from medusa.service.grpc.pitr_restore import load_restore_mapping, pod_participates, read_last_restore_key
from medusa.service.grpc.restore import get_pitr_mapping

last_restore_file, restore_key = sys.argv[1:]
mapping = load_restore_mapping()
pitr_mapping = get_pitr_mapping(mapping)
restore_requested = (
    restore_key != ""
    and (bool(os.getenv("BACKUP_NAME")) or pitr_mapping is not None)
    and pod_participates(mapping, os.getenv("POD_NAME"))
)
restore_needed = restore_requested and read_last_restore_key(last_restore_file) != restore_key
if restore_needed and pitr_mapping is not None:
    get_pitr_target_timestamp_s(pitr_mapping)
print("true" if restore_needed else "false")
' "$last_restore_file" "$restore_key")

    if [[ "$restore_needed" = true ]]; then
        echo "Restoring SSTable snapshot"
        poetry run python -m medusa.service.grpc.restore "$config_file" "$restore_key"
    else
        echo "Skipping SSTable restore"
    fi

    # Refresh archiving settings even when this pod skips the SSTable restore.
    echo "Configuring commitlog replay and archiving"
    poetry run python -m medusa.service.grpc.pitr_restore "$config_file" "$restore_key" "$last_restore_file"

    # Record a new key only after both restore steps succeed, so failures can retry.
    if [[ "$restore_needed" = true ]]; then
        printf '%s\n' "$restore_key" > "$last_restore_file"
    fi
}

grpc() {
    ORIGINAL_MEDUSA_INI_DIGEST=$(md5sum /etc/medusa/medusa.ini | awk '{print $1}')
    echo "Starting Medusa gRPC service"

    poetry run python -m medusa.service.grpc.server server.py &
    MEDUSA_PID=$!
    # tini -g also sends SIGTERM to Medusa; wait for its clean stop, or tini exits and Medusa is killed mid-stop.
    trap 'wait "$MEDUSA_PID"; exit $?' TERM
    # Medusa starts as a background job with SIGINT ignored, so turn INT into the TERM it stops cleanly on.
    trap 'kill -TERM "$MEDUSA_PID" 2>/dev/null || true; wait "$MEDUSA_PID"; exit $?' INT

    # Loop while Medusa process is running
    while kill -0 "$MEDUSA_PID" 2>/dev/null; do
      CURRENT_DIGEST=$(md5sum /etc/medusa/medusa.ini | awk '{print $1}')
      if [[ "$ORIGINAL_MEDUSA_INI_DIGEST" != "$CURRENT_DIGEST" ]]; then
        echo "Detected change in medusa.ini, checking for running backups"
        if ! [[ -f /tmp/medusa_backup_in_progress ]]; then
          echo "No backups in progress, stopping Medusa"
          kill $MEDUSA_PID
          # A running gRPC call keeps Medusa alive after it stops serving. Allow more than the commitlog
          # archiver's stop (archive interval + 5s), then kill it so the restart is not blocked.
          MEDUSA_STOP_TIMEOUT_S=120
          for ((i = 0; i < MEDUSA_STOP_TIMEOUT_S; i++)); do
            kill -0 "$MEDUSA_PID" 2>/dev/null || break
            sleep 1
          done
          kill -KILL "$MEDUSA_PID" 2>/dev/null || true
          wait "$MEDUSA_PID" || true
          # Always exit with 0 on config change.
          exit 0
        else
          echo "Backup in progress, postponing restart"
        fi
      fi
      sleep 5
    done

    # Exit with the Medusa process exit code.
    wait "$MEDUSA_PID"
}

echo "sleeping for $DEBUG_SLEEP sec"
sleep $DEBUG_SLEEP

if [[ "$MEDUSA_MODE" == "RESTORE" ]]; then
    restore
elif [[ "$MEDUSA_MODE" == "GRPC" ]]; then
    grpc
else
    echo "MEDUSA_MODE env var must be set to either RESTORE or GRPC"
    exit 1
fi
