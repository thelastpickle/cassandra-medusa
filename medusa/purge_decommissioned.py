import logging
import sys
import traceback

from medusa.monitoring import Monitoring
from medusa.purge import purge_backups
from medusa.storage import Storage


def main(config):
    monitoring = Monitoring(config=config.monitoring)

    try:
        logging.info('Starting decommissioned purge')
        storage = Storage(config=config.storage)
        # Get all nodes for cluster backups
        all_backups = list(storage.list_cluster_backups())

        # Get all nodes for latest cluster backup
        latest_backup = [storage.latest_cluster_backup()]

        # Get decommissioned nodes
        decommissioned_nodes = get_decommissioned_nodes(all_backups, latest_backup)
        
        for node in decommissioned_nodes:
            logging.info('Decommissioned node backups to purge: {}'.format(node))
            backups = storage.list_node_backups(fqdn=node)
            purge_backups(storage, backups, config.storage.backup_grace_period_in_days, config.storage.fqdn)

    except Exception as e:
        traceback.print_exc()
        tags = ['medusa-node-backup', 'purge-decommissioned-node-error', 'PURGE-DECOMMISSIONED-ERROR']
        monitoring.send(tags, 1)
        logging.error('This error happened during the decommissioned node backup purge: {}'.format(str(e)))
        sys.exit(1)


def get_nodes(backups):
    nodes = set()
    for backup in backups:
        nodes.update(backup.node_backups.keys())
    return nodes


def get_decommissioned_nodes(all_backups, latest_backup):
    all_nodes_backed_up = get_nodes(all_backups)
    latest_nodes_backed_up = get_nodes(latest_backup)
    decommisioned_nodes = all_nodes_backed_up - latest_nodes_backed_up
    return decommisioned_nodes
    
