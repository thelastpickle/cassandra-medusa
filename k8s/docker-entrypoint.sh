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
    # The BACKUP_NAME and RESTORE_KEY env vars have to be set in order for a 
    # restore to be performed. BACKUP_NAME specifies the backup to restore.
    # RESTORE_KEY is written out to a file after the restore completes. We
    # compare the value in the file to RESTORE_KEY. We perform a restore if the
    # the file does not exist or if the values differ.
   
    echo "Running Medusa in restore mode"
    last_restore_file=/var/lib/cassandra/.last-restore

    if [ -z "$BACKUP_NAME" ]; then
        echo "BACKUP_NAME env var not set, skipping restore operation"
        exit 0
    fi

    if [ -z "$RESTORE_KEY" ]; then
        echo "RESTORE_KEY env var not set, skipping restore operation"
        exit 0
    fi

    if [ -a $last_restore_file ]; then
      restore_key=`cat $last_restore_file`
      echo "Last restore is $restore_key"
    else
      restore_key=""
    fi

    if [ "$restore_key" == "$RESTORE_KEY" ]; then
        echo "Skipping restore operation"    
    else
        echo "Restoring backup $BACKUP_NAME"
        exec python3 -m medusa.service.grpc.restore
        echo $RESTORE_KEY > $last_restore_file
    fi
}

grpc() {
    echo "Starting Medusa gRPC service"
    exec python3 -m medusa.service.grpc.server server.py
}

echo "sleeping for $DEBUG_SLEEP sec"
sleep $DEBUG_SLEEP

if [ "$MEDUSA_MODE" == "RESTORE" ]; then
    restore
elif [ "$MEDUSA_MODE" == "GRPC" ]; then
    grpc
else
    echo "MEDUSA_MODE env var must be set to either RESTORE or GRPC"
    exit 1
fi
