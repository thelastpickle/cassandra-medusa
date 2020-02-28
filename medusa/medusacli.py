#! /usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright 2019 Spotify AB. All rights reserved.
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

from gevent import monkey
monkey.patch_all()
import datetime
import logging
import logging.handlers
import click
import sys

# Need to get rid of the annoying pssh warning about paramiko
if not sys.warnoptions:
    import warnings
    warnings.simplefilter("ignore")

from collections import defaultdict
from pathlib import Path

import medusa.backup
import medusa.config
import medusa.download
import medusa.index
import medusa.listing
import medusa.purge
import medusa.report_latest
import medusa.restore_cluster
import medusa.restore_node
import medusa.status
import medusa.verify
import medusa.fetch_tokenmap


pass_MedusaConfig = click.make_pass_decorator(medusa.config.MedusaConfig)


def configure_file_logging(config):
    if not medusa.config.evaluate_boolean(config.enabled):
        return

    logging.debug('Logging to file options: %s', config)
    file_handler = logging.handlers.RotatingFileHandler(
        filename=config.file,
        maxBytes=int(config.maxBytes),
        backupCount=int(config.backupCount))

    file_handler.setLevel(getattr(logging, config.level))
    file_handler.setFormatter(logging.Formatter(config.format))
    logging.getLogger('').addHandler(file_handler)


def configure_console_logging(verbosity, without_log_timestamp):
    logger = logging.getLogger('')
    logger.setLevel(logging.DEBUG)  # handlers filter the level

    loglevel = max(2 - verbosity, 0) * 10

    if verbosity == 0:
        loglevel = logging.INFO

    if without_log_timestamp:
        log_format = logging.Formatter('%(levelname)s: %(message)s')
        # log_format = '%(levelname)s: %(message)s'
    else:
        log_format = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s')

    console_handler = logging.StreamHandler()
    console_handler.setLevel(loglevel)
    console_handler.setFormatter(log_format)

    logger.addHandler(console_handler)
    if loglevel >= logging.DEBUG:
        # Disable debugging logging for external libraries
        for loggername in 'urllib3', 'google_cloud_storage.auth.transport.requests', 'paramiko', 'cassandra':
            logging.getLogger(loggername).setLevel(logging.CRITICAL)


@click.group()
@click.option('-v', '--verbosity', help='Verbosity', default=0, count=True)
@click.option('--without-log-timestamp', help='Do not show timestamp in logs', default=False, is_flag=True)
@click.option('--config-file', help='Specify config file')
@click.option('--bucket-name', help='Bucket name')
@click.option('--key-file', help='GCP credentials key file')
@click.option('--prefix', help='Prefix for shared storage')
@click.option('--fqdn', help='Act as another host')
@click.option('--ssh-username')
@click.option('--ssh-key-file')
@click.pass_context
def cli(ctx, verbosity, without_log_timestamp, config_file, **kwargs):
    config_file = Path(config_file) if config_file else None
    args = defaultdict(lambda: None, kwargs)
    configure_console_logging(verbosity, without_log_timestamp)
    ctx.obj = medusa.config.load_config(args, config_file)
    configure_file_logging(ctx.obj.logging)


@cli.command(name='backup')
@click.option('--backup-name', help='Custom name for the backup')
@click.option('--stagger', default=None, type=int, help='Check for staggering initial backups for duration seconds')
@click.option('--mode', default="differential", type=click.Choice(['full', 'differential']))
@pass_MedusaConfig
def backup(medusaconfig, backup_name, stagger, mode):
    """
    Backup Cassandra
    """
    stagger_time = datetime.timedelta(seconds=stagger) if stagger else None
    medusa.backup.main(medusaconfig, backup_name, stagger_time, mode)


@cli.command(name='fetch-tokenmap')
@click.option('--backup-name', help='backup name', required=True)
@pass_MedusaConfig
def fetch_tokenmap(medusaconfig, backup_name):
    """
    Backup Cassandra
    """
    medusa.fetch_tokenmap.main(medusaconfig, backup_name)


@cli.command(name='list-backups')
@click.option('--show-all/--no-show-all', default=False, help="List all backups in the bucket")
@pass_MedusaConfig
def list_backups(medusaconfig, show_all):
    """
    List backups
    """
    medusa.listing.list_backups(medusaconfig, show_all)


@cli.command(name='download')
@click.option('--backup-name', help='Custom name for the backup', required=True)
@click.option('--download-destination', help='Download destination', required=True)
@pass_MedusaConfig
def download(medusaconfig, backup_name, download_destination):
    """
    Download backup
    """
    medusa.download.download_cmd(medusaconfig, backup_name, Path(download_destination))


@cli.command(name='restore-cluster')
@click.option('--backup-name', help='Backup name', required=True)
@click.option('--seed-target', help='seed of the target hosts', required=False)
@click.option('--temp-dir', help='Directory for temporary storage', default="/tmp")
@click.option('--host-list', help='List of nodes to restore with the associated target host', required=False)
@click.option('--keep-auth/--overwrite-auth', help='Keep/overwrite system_auth as found on the nodes', default=True)
@click.option('-y', '--bypass-checks', help='Bypasses the security check for restoring a cluster',
              default=False, is_flag=True)
@click.option('--verify/--no-verify', help='Verify that the cluster is operational after the restore completes,',
              default=False)
@click.option('--keyspace', 'keyspaces', help="Restore tables from this keyspace", multiple=True, default={})
@click.option('--table', 'tables', help="Restore only this table", multiple=True, default={})
@click.option('--use-sstableloader', help='Use the sstableloader to load the backup into the cluster',
              default=False, is_flag=True)
@click.option('--pssh-pool-size', help="Number of concurrent ssh sessions started by pssh", default=10)
@pass_MedusaConfig
def restore_cluster(medusaconfig, backup_name, seed_target, temp_dir, host_list, keep_auth, bypass_checks,
                    verify, keyspaces, tables, use_sstableloader, pssh_pool_size):
    """
    Restore Cassandra cluster
    """
    medusa.restore_cluster.orchestrate(medusaconfig,
                                       backup_name,
                                       seed_target,
                                       Path(temp_dir),
                                       host_list,
                                       keep_auth,
                                       bypass_checks,
                                       verify,
                                       set(keyspaces),
                                       set(tables),
                                       int(pssh_pool_size),
                                       use_sstableloader)


@cli.command(name='restore-node')
@click.option('--temp-dir', help='Directory for temporary storage', default="/tmp")
@click.option('--backup-name', help='Backup name', required=True)
@click.option('--in-place', help='Indicates if the restore happens on the node the backup was done on.',
              default=False, is_flag=True)
@click.option('--keep-auth', help='Keep system_auth keyspace as found on the node',
              default=False, is_flag=True)
@click.option('--seeds', help='Nodes to wait for after downloading backup but before starting C*',
              default=None)
@click.option('--verify/--no-verify', help='Verify that the cluster is operational after the restore completes,',
              default=False)
@click.option('--keyspace', 'keyspaces', help="Restore tables from this keyspace", multiple=True, default={})
@click.option('--table', 'tables', help="Restore only this table", multiple=True, default={})
@click.option('--use-sstableloader', help='Use the sstableloader to load the backup into the cluster',
              default=False, is_flag=True)
@pass_MedusaConfig
def restore_node(medusaconfig, temp_dir, backup_name, in_place, keep_auth, seeds, verify, keyspaces, tables,
                 use_sstableloader):
    """
    Restore single Cassandra node
    """
    medusa.restore_node.restore_node(medusaconfig, Path(temp_dir), backup_name, in_place, keep_auth, seeds,
                                     verify, set(keyspaces), set(tables), use_sstableloader)


@cli.command(name='status')
@click.option('--backup-name', help='Backup name', required=True)
@pass_MedusaConfig
def status(medusaconfig, backup_name):
    """
    Show status of backups.
    """
    medusa.status.status(medusaconfig, backup_name)


@cli.command(name='verify')
@click.option('--backup-name', help='Backup name', required=True)
@pass_MedusaConfig
def verify(medusaconfig, backup_name):
    """
    Verify the integrity of a backup
    """
    medusa.verify.verify(medusaconfig, backup_name)


@cli.command(name='report-last-backup')
@click.option('--push-metrics', default=False, is_flag=True, help='Also push the information via metrics')
@pass_MedusaConfig
def report_last_backup(medusa_config, push_metrics):
    """
    Find time since last backup and print it to stdout
    :return:
    """
    medusa.report_latest.report_latest(medusa_config, push_metrics)


@cli.command(name='get-last-complete-cluster-backup')
@pass_MedusaConfig
def get_last_complete_cluster_backup(medusa_config):
    """
    Print the name of the latest complete cluster backup
    """
    backup = medusa.report_latest.get_latest_complete_cluster_backup(medusa_config)
    if backup is not None:
        print(backup.name)
    else:
        print("Could not find any full backup for the cluster")


@cli.command(name='build-index')
@click.option('--noop', default=False, is_flag=True, help='Compute and print the index only. Do not upload')
@pass_MedusaConfig
def build_index(medusa_config, noop):
    """
    Build indices for all present backups and prints them in logs. Might upload to buckets if asked to
    """
    medusa.index.build_indices(medusa_config, noop)


@cli.command(name='purge')
@pass_MedusaConfig
def purge(medusaconfig):
    """
    Delete obsolete backups
    """
    medusa.purge.main(medusaconfig,
                      max_backup_age=int(medusaconfig.storage.max_backup_age),
                      max_backup_count=int(medusaconfig.storage.max_backup_count))


@cli.command(name='delete-backup')
@click.option('--backup-name', help='Backup name', required=True)
@click.option('-a/-c', '--all-nodes/--current-node',
              help='Delete backups on all nodes (Default is current node only)',
              default=False, is_flag=True)
@pass_MedusaConfig
def delete_backup(medusaconfig, backup_name, all_nodes):
    """
    Delete the given backup on the current node (or on all nodes)
    """
    medusa.purge.delete_backup(medusaconfig, backup_name, all_nodes)
