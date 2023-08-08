import logging
import sys
import traceback

from medusa.monitoring import Monitoring
from medusa.purge import purge_backups
from medusa.storage import Storage
from medusa.cassandra_utils import Cassandra


def main(config):
    monitoring = Monitoring(config=config.monitoring)

    try:
        logging.info('Starting decommissioned purge')
        storage = Storage(config=config.storage)
        cassandra = Cassandra(config=config.cassandra)
        # Get all nodes having backups
        blobs = storage.list_root_blobs()
        all_nodes = get_all_nodes(blobs)

        # Get live nodes
        live_nodes = get_live_nodes(cassandra)

        # Get decommissioned nodes
        decommissioned_nodes = all_nodes - live_nodes

        for node in decommissioned_nodes:
            logging.info('Decommissioned node backups to purge: {}'.format(node))
            backups = storage.list_node_backups(fqdn=node)
            (nb_objects_purged, total_purged_size, total_objects_within_grace) \
                = purge_backups(storage, backups, config.storage.backup_grace_period_in_days, config.storage.fqdn)

        logging.debug('Emitting metrics')
        tags = ['medusa-decommissioned-node-backup', 'purge-error', 'PURGE-ERROR']
        monitoring.send(tags, 0)
        return (nb_objects_purged, total_purged_size, total_objects_within_grace, len(backups))
    except Exception as e:
        traceback.print_exc()
        tags = ['medusa-decommissioned-node-backup', 'purge-error', 'PURGE-ERROR']
        monitoring.send(tags, 1)
        logging.error('This error happened during the purge of decommissioned nodes: {}'.format(str(e)))
        sys.exit(1)


def get_all_nodes(blobs):
    nodes = {blob.rstrip('/') for blob in blobs if not blob.startswith('index')}
    return nodes


def get_live_nodes(cassandra):
    nodes = set()
    for host in cassandra.tokenmap.items():
        nodes.add(host)
    return nodes
