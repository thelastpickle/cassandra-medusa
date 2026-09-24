#!/bin/bash
# Patch cassandra.yaml with the correct listen/broadcast addresses from env
# before starting Cassandra under supervisord.
set -e

YAML=/etc/cassandra/cassandra.yaml

if [ -n "$CASSANDRA_LISTEN_ADDRESS" ]; then
    sed -i "s/^listen_address:.*/listen_address: ${CASSANDRA_LISTEN_ADDRESS}/" "$YAML"
    # rpc_address must also be set to the node IP so CQL is reachable from
    # outside the container; broadcast_rpc_address follows.
    sed -i "s/^rpc_address:.*/rpc_address: ${CASSANDRA_LISTEN_ADDRESS}/" "$YAML"
fi

if [ -n "$CASSANDRA_BROADCAST_ADDRESS" ]; then
    # broadcast_address may be commented out — uncomment and set it
    if grep -q "^#broadcast_address" "$YAML"; then
        sed -i "s/^#broadcast_address:.*/broadcast_address: ${CASSANDRA_BROADCAST_ADDRESS}/" "$YAML"
    elif grep -q "^broadcast_address" "$YAML"; then
        sed -i "s/^broadcast_address:.*/broadcast_address: ${CASSANDRA_BROADCAST_ADDRESS}/" "$YAML"
    else
        echo "broadcast_address: ${CASSANDRA_BROADCAST_ADDRESS}" >> "$YAML"
    fi
    # broadcast_rpc_address is required when rpc_address != listen_address
    if grep -q "^# *broadcast_rpc_address" "$YAML"; then
        sed -i "s/^# *broadcast_rpc_address:.*/broadcast_rpc_address: ${CASSANDRA_BROADCAST_ADDRESS}/" "$YAML"
    elif grep -q "^broadcast_rpc_address" "$YAML"; then
        sed -i "s/^broadcast_rpc_address:.*/broadcast_rpc_address: ${CASSANDRA_BROADCAST_ADDRESS}/" "$YAML"
    else
        echo "broadcast_rpc_address: ${CASSANDRA_BROADCAST_ADDRESS}" >> "$YAML"
    fi
fi

if [ -n "$CASSANDRA_SEEDS" ]; then
    sed -i "s/- seeds: .*/- seeds: \"${CASSANDRA_SEEDS}\"/" "$YAML"
fi

# Fix num_tokens=1 and set the pre-assigned initial token so that nodes never
# collide during concurrent bootstrap. CASSANDRA_INITIAL_TOKEN must be set per
# node in docker-compose.yml.
if [ -n "$CASSANDRA_INITIAL_TOKEN" ]; then
    sed -i "s/^num_tokens:.*/num_tokens: 1/" "$YAML"
    sed -i "s/^# *initial_token:.*/initial_token: ${CASSANDRA_INITIAL_TOKEN}/" "$YAML"
fi

exec cassandra -f
