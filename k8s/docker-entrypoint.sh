#!/bin/bash

set -e

echo "MEDUSA_MODE = $MEDUSA_MODE"

restore() {
    echo "Running Medusa in restore mode"
    last_restore_file=/var/lib/cassandra/.last-restore
    if [ -z "$BACKUP_NAME" ]; then
        echo "BACKUP_NAME env var not set, skipping restore operation"
        exit 0
    fi

    if [ -a $last_restore_file ]; then
      backup_name=`cat $last_restore_file`
      echo "Last restored backup is $backup_name"
    else
      backup_name=""
    fi

    if [ "$backup_name" == "$BACKUP_NAME" ]; then
        echo "Skipping restore operation"    
    else
        echo "Restoring backup $backup_name"
        python3 -m medusa.service.grpc_svc.restore 
        echo $BACKUP_NAME > $last_restore_file
    fi
}

grpc() {
    echo "Starting Medusa gRPC service"
    python3 -m medusa.service.grpc_svc.server server.py
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
