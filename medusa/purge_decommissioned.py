import logging
import sys
import traceback
from medusa.cassandra_utils import CqlSessionProvider

from medusa.monitoring import Monitoring
from medusa.purge import purge_backups
from medusa.storage import Storage


def main(config):
    monitoring = Monitoring(config=config.monitoring)
    try:
        logging.info('Starting decommissioned purge')
        with Storage(config=config.storage) as storage:
            # Get all nodes having backups
            blobs = storage.list_root_blobs()
            all_nodes = get_all_nodes(blobs)

            # Get live nodes
            live_nodes = get_live_nodes(config)

            # Get decommissioned nodes
            decommissioned_nodes = get_decommissioned_nodes(all_nodes, live_nodes)

            for node in decommissioned_nodes:
                logging.info('Decommissioned node backups to purge: {}'.format(node))
                backups = set(storage.list_node_backups(fqdn=node))
                (nb_objects_purged, total_purged_size, total_objects_within_grace) \
                    = purge_backups(storage, backups, config.storage.backup_grace_period_in_days, node)

            logging.debug('Emitting metrics')
            tags = ['medusa-decommissioned-node-backup', 'purge-error', 'PURGE-ERROR']
            monitoring.send(tags, 0)

            object_counts = (nb_objects_purged, total_purged_size, total_objects_within_grace, len(backups))
            return decommissioned_nodes, object_counts

    except Exception as e:
        traceback.print_exc()
        tags = ['medusa-decommissioned-node-backup', 'purge-error', 'PURGE-ERROR']
        monitoring.send(tags, 1)
        logging.error('This error happened during the purge of decommissioned nodes: {}'.format(str(e)))
        sys.exit(1)


def get_all_nodes(blobs):
    nodes = {blob.name.split('/')[1] for blob in blobs if '/index/' not in blob.name}
    return nodes


def get_live_nodes(config):
    session_provider = CqlSessionProvider(ip_addresses=[config.storage.fqdn], config=config)
    with session_provider.new_session(retry=True) as cql_session:
        token_map = cql_session.tokenmap()

    live_nodes = set(token_map.keys())

    return live_nodes


def get_decommissioned_nodes(all_nodes, live_nodes):
    return all_nodes.difference(live_nodes)
