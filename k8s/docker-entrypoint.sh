#!/bin/bash

set -e

echo "MEDUSA_MODE = $MEDUSA_MODE"

restore() {
    echo "Running Medusa in restore mode"
}

grpc() {
    echo "Starting Medusa gRPC service"
    python3 -m medusa.service.grpc_svc.server server.py
}

if [ "$MEDUSA_MODE" == "RESTORE" ]; then
    restore
elif [ "$MEDUSA_MODE" == "GRPC" ]; then
    grpc
else
    echo "MEDUSA_MODE env var must be set to either RESTORE or GRPC"
    exit 1
fi
