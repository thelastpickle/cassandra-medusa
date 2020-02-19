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

import collections
import json
import logging
import os
import shlex
import socket
import subprocess
import sys
import time
import uuid

from medusa.cassandra_utils import Cassandra, is_node_up
from medusa.download import download_data
from medusa.storage import Storage
from medusa.verify_restore import verify_restore


A_MINUTE = 60
MAX_ATTEMPTS = 60


def restore_node(config, temp_dir, backup_name, in_place, keep_auth, seeds, verify, keyspaces, tables,
                 use_sstableloader=False):

    if in_place and keep_auth:
        logging.error('Cannot keep system_auth when restoring in-place. It would be overwritten')
        sys.exit(1)

    storage = Storage(config=config.storage)

    if not use_sstableloader:
        restore_node_locally(config, temp_dir, backup_name, in_place, keep_auth, seeds, storage,
                             keyspaces, tables)
    else:
        restore_node_sstableloader(config, temp_dir, backup_name, in_place, keep_auth, seeds, storage,
                                   keyspaces, tables)

    if verify:
        verify_restore([socket.getfqdn()], config)


def restore_node_locally(config, temp_dir, backup_name, in_place, keep_auth, seeds, storage, keyspaces, tables):
    differential_blob = storage.storage_driver.get_blob(
        os.path.join(config.storage.fqdn, backup_name, 'meta', 'differential'))

    node_backup = storage.get_node_backup(
        fqdn=config.storage.fqdn,
        name=backup_name,
        differential_mode=True if differential_blob is not None else False
    )

    if not node_backup.exists():
        logging.error('No such backup')
        sys.exit(1)

    fqtns_to_restore = get_fqtns_to_restore(keyspaces, tables, node_backup.manifest)
    if len(fqtns_to_restore) == 0:
        logging.error('There is nothing to restore')
        sys.exit(0)

    cassandra = Cassandra(config.cassandra)

    # Download the backup
    download_dir = temp_dir / 'medusa-restore-{}'.format(uuid.uuid4())
    logging.info('Downloading data from backup to {}'.format(download_dir))
    download_data(config.storage, node_backup, fqtns_to_restore, destination=download_dir)

    logging.info('Stopping Cassandra')
    cassandra.shutdown()

    # Clean the commitlogs, the saved cache to prevent any kind of conflict
    # especially around system tables.
    clean_path(cassandra.commit_logs_path)
    clean_path(cassandra.saved_caches_path)

    # move backup data to Cassandra data directory according to system table
    logging.info('Moving backup data to Cassandra data directory')
    manifest = json.loads(node_backup.manifest)
    for section in manifest:
        fqtn = "{}.{}".format(section['keyspace'], section['columnfamily'])
        if fqtn not in fqtns_to_restore:
            logging.debug('Skipping restore for {}'.format(fqtn))
            continue
        maybe_restore_section(section, download_dir, cassandra.root, in_place, keep_auth)

    node_fqdn = storage.config.fqdn
    token_map_file = download_dir / 'tokenmap.json'
    with open(str(token_map_file), 'r') as f:
        tokens = get_node_tokens(node_fqdn, f)
        logging.debug("Parsed tokens: {}".format(tokens))

    # possibly wait for seeds
    if seeds is not None:
        wait_for_seeds(config, seeds)
    else:
        logging.info('No --seeds specified so we will not wait for any')

    # Start up Cassandra
    logging.info('Starting Cassandra')
    # restoring in place retains system.local, which has tokens in it. no need to specify extra
    if in_place:
        cassandra.start_with_implicit_token()
    else:
        cassandra.start(tokens)

    return node_backup


def restore_node_sstableloader(config, temp_dir, backup_name, in_place, keep_auth, seeds, storage, keyspaces, tables):
    cassandra = Cassandra(config.cassandra)
    node_backup = None
    fqdns = config.storage.fqdn.split(",")
    for fqdn in fqdns:
        differential_blob = storage.storage_driver.get_blob(
            os.path.join(fqdn, backup_name, 'meta', 'differential'))

        node_backup = storage.get_node_backup(
            fqdn=fqdn,
            name=backup_name,
            differential_mode=True if differential_blob is not None else False
        )

        if not node_backup.exists():
            logging.error('No such backup')
            sys.exit(1)

        fqtns_to_restore = get_fqtns_to_restore(keyspaces, tables, node_backup.manifest)

        if len(fqtns_to_restore) == 0:
            logging.error('There is nothing to restore')
            sys.exit(0)

        # Download the backup
        download_dir = temp_dir / 'medusa-restore-{}'.format(uuid.uuid4())
        logging.info('Downloading data from backup to {}'.format(download_dir))
        download_data(config.storage, node_backup, fqtns_to_restore, destination=download_dir)
        invoke_sstableloader(config, download_dir, keep_auth, fqtns_to_restore, cassandra.storage_port)
        logging.info('Finished loading backup from {}'.format(fqdn))

    return node_backup


def invoke_sstableloader(config, download_dir, keep_auth, fqtns_to_restore, storage_port):
    cassandra_is_ccm = int(shlex.split(config.cassandra.is_ccm)[0])
    keyspaces = os.listdir(str(download_dir))
    for keyspace in keyspaces:
        ks_path = os.path.join(str(download_dir), keyspace)
        if os.path.isdir(ks_path) and keyspace_is_allowed_to_restore(keyspace, keep_auth, fqtns_to_restore):
            logging.info('Restoring keyspace {} with sstableloader...'.format(ks_path))
            for table in os.listdir(str(ks_path)):
                table_path = os.path.join(str(ks_path), table)
                if os.path.isdir(table_path) and table_is_allowed_to_restore(keyspace, table, fqtns_to_restore):
                    logging.debug('Restoring table {} with sstableloader...'.format(table))
                    cql_username = 'foo' if config.cassandra.cql_username is None else config.cassandra.cql_username
                    cql_password = 'foo' if config.cassandra.cql_password is None else config.cassandra.cql_password
                    sstableloader_args = [config.cassandra.sstableloader_bin,
                                          '-d', socket.getfqdn() if cassandra_is_ccm == 0
                                          else '127.0.0.1',
                                          '--username', cql_username,
                                          '--password', cql_password,
                                          '--no-progress',
                                          os.path.join(ks_path, table)]
                    if storage_port != "7000":
                        sstableloader_args.append("--storage-port")
                        sstableloader_args.append(storage_port)
                    output = subprocess.check_output(sstableloader_args)
                    for line in output.decode('utf-8').split('\n'):
                        logging.debug(line)
    clean_path(download_dir)


def keyspace_is_allowed_to_restore(keyspace, keep_auth, fqtns_to_restore):
    if keyspace == 'system':
        return False

    if keyspace == 'system_auth' and keep_auth is True:
        return False

    # a keyspace is allowed to restore if there is at least one fqtn from this keyspace
    # so we get keyspaces from all the fqtns and make it a set to remove duplicates
    keyspaces_to_restore = set(map(lambda fqtn: fqtn.split('.')[0], fqtns_to_restore))
    # then we check if the keyspace we are restoring is present in that set
    if keyspace not in keyspaces_to_restore:
        return False

    return True


def table_is_allowed_to_restore(keyspace, table, fqtns_to_restore):

    # table is allowed to restore if it's present in at least one fqtn allowed to restore
    fqtn = '{}.{}'.format(keyspace, table)
    if fqtn not in fqtns_to_restore:
        return False

    return True


def clean_path(p):
    if p.exists():
        logging.debug('Cleaning ({})'.format(p))
        subprocess.check_output(['sudo', '-u', p.owner(), 'rm', '-rf', str(p)])


def maybe_restore_section(section, download_dir, cassandra_data_dir, in_place, keep_auth):

    # decide whether to restore files for this table or not

    # we restore everything from all keyspaces when restoring in_place

    # when restoring not in_place (i.e. doing a restore test), we skip restoring system.local and system.peers tables
    # but we delete the ones that are present.
    # if --keep-auth is set, we won't touch the existing system_auth (won't delete nor overwrite from the backup)

    restore_section = True

    if not in_place:
        if section['keyspace'] == 'system':
            if section['columnfamily'].startswith('local-') or section['columnfamily'].startswith('peers-'):
                restore_section = False
        if section['keyspace'] == 'system_auth' and keep_auth:
            logging.info('Keeping section {}.{} untouched'.format(section['keyspace'], section['columnfamily']))
            return

    src = download_dir / section['keyspace'] / section['columnfamily']
    # not appending the column family name because mv later on copies the whole folder
    dst = cassandra_data_dir / section['keyspace'] / section['columnfamily']

    # prepare the destination folder
    if dst.exists():
        logging.debug('Cleaning directory {}'.format(dst))
        subprocess.check_output(['sudo', '-u', cassandra_data_dir.owner(),
                                 'rm', '-rf', str(dst)])
    else:
        logging.debug('Creating directory {}'.format(dst))
        subprocess.check_output(['sudo', '-u', cassandra_data_dir.owner(),
                                 'mkdir', '-p', str(cassandra_data_dir / section['keyspace'])])

    if not restore_section:
        logging.debug("Skipping the actual restore of {}".format(section['columnfamily']))
        return

    # restore the table
    logging.debug('Restoring {} -> {}'.format(src, dst))
    subprocess.check_output(['sudo', 'mv', str(src), str(dst)])
    file_ownership = '{}:{}'.format(cassandra_data_dir.owner(), cassandra_data_dir.group())
    subprocess.check_output(['sudo', 'chown', '-R', file_ownership, str(dst)])


def get_node_tokens(node_fqdn, token_map_file):
    token_map = json.load(token_map_file)
    token = token_map[node_fqdn]['tokens']

    # if vnodes, then the tokens come as an iterable
    if isinstance(token, collections.Iterable):
        return list(map(str, token))
    # if there is only a single token, the token might show up as one integer
    else:
        return [str(token)]


def wait_for_seeds(config, seeds):
    seed_list = seeds.split(',')
    attempts = 0
    while not any([is_node_up(config, s) for s in seed_list]):
        logging.info('No seeds are up yet, will wait a minute')
        attempts += 1
        time.sleep(A_MINUTE)
        if attempts > MAX_ATTEMPTS:
            logging.error('Gave up waiting for seeds, aborting the restore')
            sys.exit(1)
    logging.info('At least one seed is now up')


def get_fqtns_to_restore(keep_keyspaces, keep_tables, manifest_str):

    fqtns = set()
    manifest = json.loads(manifest_str)

    for section in manifest:
        ks = section['keyspace']
        # in manifest, the table names have cfids, but from CLI we get it without
        # we need to take care and use both
        t = section['columnfamily'].split('-')[0]

        fqtn = '{}.{}'.format(ks, t)
        fqtn_with_id = '{}.{}'.format(section['keyspace'], section['columnfamily'])

        # if not keyspaces / tables were specified, we keep everything
        if len(keep_keyspaces) == 0 and len(keep_tables) == 0:
            fqtns.add(fqtn_with_id)
            continue

        # if the whole keyspace is a keep
        if ks in keep_keyspaces:
            fqtns.add(fqtn_with_id)
            continue

        # if just the table is a keep
        if fqtn in keep_tables:
            fqtns.add(fqtn_with_id)
            continue

        logging.info('Skipping restore of {}.{}'.format(ks, t))

    return fqtns
