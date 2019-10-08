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

import cassandra
import configparser
import datetime
import json
import logging
import os
import shutil
import subprocess
import time

from aloe import step, world
from pathlib import Path
from subprocess import PIPE
import signal
from cassandra.cluster import Cluster

import medusa.backup
import medusa.index
import medusa.listing
import medusa.purge
import medusa.report_latest
import medusa.restore_node
import medusa.status
import medusa.verify

from medusa.config import MedusaConfig, StorageConfig, CassandraConfig, MonitoringConfig, _namedtuple_from_dict
from medusa.storage import Storage
from medusa.monitoring import LocalMonitoring


def kill_cassandra():
    p = subprocess.Popen(['ps', '-A'], stdout=subprocess.PIPE)
    out, err = p.communicate()
    for line in out.splitlines():
        if b'org.apache.cassandra.service.CassandraDaemon' in line:
            logging.info(line)
            pid = int(line.split(None, 1)[0])
            os.kill(pid, signal.SIGKILL)


def cleanup_monitoring():
    LocalMonitoring(world.config.monitoring).truncate_metric_file()


def cleanup_storage(storage_provider):
    if storage_provider == "local":
        if os.path.isdir(os.path.join("/tmp", "medusa_it_bucket")):
            shutil.rmtree(os.path.join("/tmp", "medusa_it_bucket"))
        os.makedirs(os.path.join("/tmp", "medusa_it_bucket"))
    elif storage_provider == "google_storage" or storage_provider.find("s3") == 0:
        storage = Storage(config=world.config.storage)
        objects = storage.storage_driver.list_objects()
        for obj in objects:
            storage.storage_driver.delete_object(obj)


@step(r'I have a fresh ccm cluster running named "([^"]*)"')
def _i_have_a_fresh_ccm_cluster_running(self, cluster_name):
    world.cassandra_version = "2.2.14"
    world.session = None
    world.cluster_name = cluster_name
    subprocess.run(["ccm", "stop"], stdout=PIPE, stderr=PIPE)
    kill_cassandra()
    res = subprocess.run(["ccm", "switch", world.cluster_name], stdout=PIPE, stderr=PIPE)
    if b"does not appear to be a valid cluster" not in res.stderr:
        subprocess.check_call(["ccm", "remove", world.cluster_name], stdout=PIPE, stderr=PIPE)
    subprocess.check_call(["ccm", "create", world.cluster_name, "-v", "binary:" + world.cassandra_version, "-n", "1"])
    os.popen("LOCAL_JMX=yes ccm start").read()
    world.session = connect_cassandra()


@step(r'I am using "([^"]*)" as storage provider')
def i_am_using_storage_provider(self, storage_provider):
    logging.info("Starting the tests")
    if not hasattr(world, 'cluster_name'):
        world.cluster_name = 'test'
    config = configparser.ConfigParser(interpolation=None)

    if storage_provider == "local":
        if os.path.isdir(os.path.join("/tmp", "medusa_it_bucket")):
            shutil.rmtree(os.path.join("/tmp", "medusa_it_bucket"))
        os.makedirs(os.path.join("/tmp", "medusa_it_bucket"))

        config['storage'] = {
            'host_file_separator': ',',
            'bucket_name': 'medusa_it_bucket',
            'key_file': '',
            'storage_provider': 'local',
            'fqdn': 'localhost',
            'api_key_or_username': '',
            'api_secret_or_password': '',
            'base_path': '/tmp'
        }
    elif storage_provider == "google_storage":
        config['storage'] = {
            'host_file_separator': ',',
            'bucket_name': 'medusa_it_bucket',
            'key_file': '~/medusa_credentials.json',
            'storage_provider': 'google_storage',
            'fqdn': 'localhost',
            'api_key_or_username': '',
            'api_secret_or_password': '',
            'base_path': '/tmp'
        }
    elif storage_provider.startswith("s3"):
        config['storage'] = {
            'host_file_separator': ',',
            'bucket_name': 'tlp-medusa-dev',
            'key_file': '~/.aws/credentials',
            'storage_provider': storage_provider,
            'fqdn': 'localhost',
            'api_key_or_username': '',
            'api_secret_or_password': '',
            'api_profile': 'default',
            'base_path': '/tmp'
        }

    config['cassandra'] = {
        'is_ccm': 1,
        'stop_cmd': 'ccm stop',
        'start_cmd': 'ccm start',
        'cql_username': 'cassandra',
        'cql_password': 'cassandra',
        'config_file': os.path.expanduser(os.path.join('~/.ccm', world.cluster_name, 'node1', 'conf',
                                                       'cassandra.yaml')),
        'sstableloader_bin': os.path.expanduser(os.path.join('~/.ccm', 'repository', world.cassandra_version,
                                                             'bin', 'sstableloader'))
    }

    config['monitoring'] = {
        'monitoring_provider': 'local'
    }

    world.config = MedusaConfig(
        storage=_namedtuple_from_dict(StorageConfig, config['storage']),
        cassandra=_namedtuple_from_dict(CassandraConfig, config['cassandra']),
        monitoring=_namedtuple_from_dict(MonitoringConfig, config['monitoring']),
        ssh=None,
        restore=None
    )
    cleanup_storage(storage_provider)
    cleanup_monitoring()


@step(r'I create the "([^"]*)" table in keyspace "([^"]*)"')
def _i_create_the_whatever_table(self, table_name, keyspace_name):
    keyspace = """CREATE KEYSPACE IF NOT EXISTS {} WITH replication = {{'class':'SimpleStrategy',
    'replication_factor':1}}"""
    world.session.execute(keyspace.format(keyspace_name))

    table = "CREATE TABLE IF NOT EXISTS {}.{} (id timeuuid PRIMARY KEY, value text);"
    world.session.execute(table.format(keyspace_name, table_name))


@step(r'I load "([^"]*)" rows in the "([^"]*)" table')
def _i_load_rows_in_the_whatever_table(self, nb_rows, table_name):
    for i in range(int(nb_rows)):
        world.session.execute("INSERT INTO {} (id, value) VALUES(now(), '{}')".format(table_name, i))


@step(r'run a "([^"]*)" command')
def _i_run_a_whatever_command(self, command):
    os.popen(command).read()


@step(r'I perform a backup in "([^"]*)" mode of the node named "([^"]*)"')
def _i_perform_a_backup_of_the_node_named_backupname(self, backup_mode, backup_name):
    medusa.backup.main(world.config, backup_name, None, None, backup_mode)


@step(r'I can see the backup named "([^"]*)" when I list the backups')
def _i_can_see_the_backup_named_backupname_when_i_list_the_backups(self, backup_name):
    storage = Storage(config=world.config.storage)
    cluster_backups = storage.list_cluster_backups()
    found = False
    for backup in cluster_backups:
        if backup.name == backup_name:
            found = True

    assert found is True


@step(r'I cannot see the backup named "([^"]*)" when I list the backups')
def _i_cannot_see_the_backup_named_backupname_when_i_list_the_backups(self, backup_name):
    storage = Storage(config=world.config.storage)
    cluster_backups = storage.list_cluster_backups()
    found = False
    for backup in cluster_backups:
        if backup.name == backup_name:
            found = True

    assert found is False


@step('I can see the backup status for "([^"]*)" when I run the status command')
def _i_can_see_backup_status_when_i_run_the_status_command(self, backup_name):
    medusa.status.status(config=world.config, backup_name=backup_name)


@step(r'I can see no backups when I list the backups')
def _i_can_see_no_backups(self):
    storage = Storage(config=world.config.storage)
    cluster_backups = storage.list_cluster_backups()

    assert 0 == len(list(cluster_backups))


@step(r'the backup named "([^"]*)" has (\d+) SSTables for the "([^"]*)" table in keyspace "([^"]*)"')
def _the_backup_named_backupname_has_nb_sstables_for_the_whatever_table(self, backup_name, nb_sstables, table_name,
                                                                        keyspace):
    storage = Storage(config=world.config.storage)
    path = os.path.join(world.config.storage.fqdn, backup_name, "data", keyspace, table_name)
    objects = storage.storage_driver.list_objects(path)
    sstables = list(filter(lambda obj: '-Data.db' in obj.name, objects))
    if len(sstables) != int(nb_sstables):
        logging.error("{} SSTables : {}".format(len(sstables), sstables))
        logging.error("Was expecting {} SSTables".format(nb_sstables))
        assert len(sstables) == int(nb_sstables)


@step(r'I can verify the backup named "([^"]*)" successfully')
def _i_can_verify_the_backup_named_successfully(self, backup_name):
    medusa.verify.verify(world.config, backup_name)


@step(r'I restore the backup named "([^"]*)"$')
def _i_restore_the_backup_named(self, backup_name):
    medusa.restore_node.restore_node(world.config, Path("/tmp"), backup_name, in_place=True, keep_auth=False,
                                     seeds=None, verify=None, use_sstableloader=False)


@step(r'I restore the backup named "([^"]*)" with the sstableloader$')
def _i_restore_the_backup_named_with_sstableloader(self, backup_name):
    medusa.restore_node.restore_node(world.config, Path("/tmp"), backup_name, in_place=True, keep_auth=False,
                                     seeds=None, verify=None, use_sstableloader=True)


@step(r'I have "([^"]*)" rows in the "([^"]*)" table')
def _i_have_rows_in_the_table(self, nb_rows, table_name):
    world.session = connect_cassandra()
    rows = world.session.execute("select count(*) as nb from {}".format(table_name))
    assert rows[0].nb == int(nb_rows)


@step(r'I can see the backup index entry for "([^"]*)"')
def _the_backup_named_backupname_is_present_in_the_index(self, backup_name):
    storage = Storage(config=world.config.storage)
    fqdn = world.config.storage.fqdn
    path = os.path.join('index/backup_index', backup_name, 'tokenmap_{}.json'.format(fqdn))
    tokenmap_from_index = storage.storage_driver.get_blob_content_as_string(path)
    path = os.path.join(fqdn, backup_name, 'meta', 'tokenmap.json')
    tokenmap_from_backup = storage.storage_driver.get_blob_content_as_string(path)
    # Check that we have the manifest as well there
    manifest_path = os.path.join('index/backup_index', backup_name, 'manifest_{}.json'.format(fqdn))
    manifest_from_index = storage.storage_driver.get_blob_content_as_string(manifest_path)
    path = os.path.join(fqdn, backup_name, 'meta', 'manifest.json')
    manifest_from_backup = storage.storage_driver.get_blob_content_as_string(path)
    assert tokenmap_from_backup == tokenmap_from_index and manifest_from_backup == manifest_from_index


@step(r'I can see the latest backup for "([^"]*)" being called "([^"]*)"')
def _the_latest_backup_for_fqdn_is_called_backupname(self, expected_fqdn, expected_backup_name):
    storage = Storage(config=world.config.storage)
    latest_backup = storage.latest_node_backup(fqdn=expected_fqdn)
    assert latest_backup.name == expected_backup_name


@step(r'there is no latest backup for node "([^"]*)"')
def _there_is_no_latest_backup_for_node_fqdn(self, fqdn):
    storage = Storage(config=world.config.storage)
    node_backup = storage.latest_node_backup(fqdn=fqdn)
    assert node_backup is None


@step(r'node "([^"]*)" fakes a complete backup named "([^"]*)" on "([^"]*)"')
def _node_fakes_a_complete_backup(self, fqdn, backup_name, backup_datetime):
    path_root = "/tmp/medusa_it_bucket"

    fake_tokenmap = json.dumps({
        "n1": {"tokens": [1], "is_up": True},
        "n2": {"tokens": [2], "is_up": True},
        "n3": {"tokens": [3], "is_up": True}
    })

    dir_path = os.path.join(path_root, 'index', 'backup_index', backup_name)
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)

    # fake token map, manifest and schema in index
    path_tokenmap = '{}/index/backup_index/{}/tokenmap_{}.json'.format(path_root, backup_name, fqdn)
    write_dummy_file(path_tokenmap, backup_datetime, fake_tokenmap)
    path_manifest = '{}/index/backup_index/{}/manifest_{}.json'.format(path_root, backup_name, fqdn)
    write_dummy_file(path_manifest, backup_datetime, fake_tokenmap)
    path_schema = '{}/index/backup_index/{}/schema_{}.cql'.format(path_root, backup_name, fqdn)
    write_dummy_file(path_schema, backup_datetime, fake_tokenmap)

    dir_path = os.path.join(path_root, 'index', 'latest_backup', fqdn)
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)

    # fake token map in latest_backup
    path_latest_backup_tokenmap = '{}/index/latest_backup/{}/tokenmap.json'.format(path_root, fqdn)
    write_dummy_file(path_latest_backup_tokenmap, backup_datetime, fake_tokenmap)

    # fake token name in latest_backup
    path_latest_backup_name = '{}/index/latest_backup/{}/backup_name.txt'.format(path_root, fqdn)
    write_dummy_file(path_latest_backup_name, backup_datetime)

    # fake actual backup folder
    dir_path = os.path.join(path_root, fqdn, backup_name, 'meta')
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)

    # fake schema in actual backup path
    path_schema = '{}/{}/{}/meta/schema.cql'.format(path_root, fqdn, backup_name)
    write_dummy_file(path_schema, backup_datetime)

    # fake manifest in actual backup path
    path_manifest = '{}/{}/{}/meta/manifest.json'.format(path_root, fqdn, backup_name)
    write_dummy_file(path_manifest, backup_datetime)

    # fake token map in actual backup path
    path_tokenmap = '{}/{}/{}/meta/tokenmap.json'.format(path_root, fqdn, backup_name)
    write_dummy_file(path_tokenmap, backup_datetime, fake_tokenmap)


@step(r'the latest cluster backup is "([^"]*)"')
def _the_latest_cluster_backup_is(self, expected_backup_name):
    storage = Storage(config=world.config.storage)
    backup = storage.latest_cluster_backup()
    assert expected_backup_name == backup.name


@step(r'there is no latest complete backup')
def _there_is_no_latest_complete_backup(self):
    storage = Storage(config=world.config.storage)
    actual_backup = storage.latest_complete_cluster_backup()
    assert actual_backup is None


@step(r'I can list and print backups without errors')
def _can_list_print_backups_without_error(self):
    medusa.listing.list_backups(config=world.config, show_all=True)


@step(r'the latest complete cluster backup is "([^"]*)"')
def _the_latest_complete_cluster_backup_is(self, expected_backup_name):
    storage = Storage(config=world.config.storage)
    actual_backup = storage.latest_complete_cluster_backup()
    if actual_backup is not None:
        assert expected_backup_name == actual_backup.name


@step(r'I truncate the backup index')
def _truncate_the_index(self):
    path_root = "/tmp/medusa_it_bucket"
    index_path = '{}/index'.format(path_root)
    shutil.rmtree(index_path)


@step(r'I truncate the backup folder')
def _truncate_the_backup_folder(self):
    path_root = "/tmp/medusa_it_bucket"
    backup_path = '{}/localhost'.format(path_root)
    shutil.rmtree(backup_path)


@step(r'I re-create the backup index')
def _recreate_the_index(self):
    medusa.index.build_indices(world.config, False)


@step(r'I can report latest backups without errors')
def _can_report_backups_without_errors(self):
    medusa.report_latest.report_latest(config=world.config, push_metrics=True)


@step(r'the backup index does not exist')
def _the_backup_index_does_not_exist(self):
    storage = Storage(config=world.config.storage)
    assert False is medusa.index.index_exists(storage)


@step(r'the backup index exists')
def _the_backup_index_exists(self):
    storage = Storage(config=world.config.storage)
    assert True is medusa.index.index_exists(storage)


@step(r'I can see (\d+) SSTables? in the SSTable pool for the "([^"]*)" table in keyspace "([^"]*)"')
def _i_can_see_nb_sstables_in_the_sstable_pool(self, nb_sstables, table_name, keyspace):
    storage = Storage(config=world.config.storage)
    path = os.path.join(world.config.storage.fqdn, "data", keyspace, table_name)
    objects = storage.storage_driver.list_objects(path)
    sstables = list(filter(lambda obj: '-Data.db' in obj.name, objects))
    if len(sstables) != int(nb_sstables):
        logging.error("{} SSTables : {}".format(len(sstables), sstables))
        logging.error("Was expecting {} SSTables".format(nb_sstables))
        assert len(sstables) == int(nb_sstables)


@step(r'backup named "([^"]*)" has (\d+) files? in the manifest for the "([^"]*)" table in keyspace "([^"]*)"')
def _backup_named_something_has_nb_files_in_the_manifest(self, backup_name, nb_files, table_name, keyspace):
    storage = Storage(config=world.config.storage)
    node_backups = storage.list_node_backups()
    # Find the backup we're looking for
    target_backup = list(filter(lambda backup: backup.name == backup_name, node_backups))[0]
    # Parse its manifest
    manifest = json.loads(target_backup.manifest)
    for section in manifest:
        if section['keyspace'] == keyspace and section['columnfamily'][:len(table_name)] == table_name:
            if len(section['objects']) != int(nb_files):
                logging.error("Was expecting {} files, got {}".format(nb_files, len(section['objects'])))
                assert len(section['objects']) == int(nb_files)


@step(r'verify fails on the backup named "([^"]*)"')
def _verify_fails_on_the_backup_named(self, backup_name):
    try:
        medusa.verify.verify(world.config, backup_name)
        raise AssertionError("Backup verification should have failed but didn't.")
    except RuntimeError:
        # This exception is required to be raised to validate the step
        pass


@step(r'I purge the backup history to retain only (\d+) backups')
def _i_purge_the_backup_history_to_retain_only_nb_backups(self, backup_count):
    medusa.purge.main(world.config, max_backup_count=int(backup_count))


@step(r'I see "(\d+)" metrics emitted')
def _i_see_metrics_emitted(self, metrics_count):
    metrics = list(LocalMonitoring(world.config).load_metrics())
    logging.info('There is {} metrics'.format(len(metrics)))
    logging.info('The metrics are: {}'.format(metrics))
    assert int(len(metrics)) == int(metrics_count)


@step(r'I truncate the "([^"]*)" table$')
def _i_truncate_the_table(self, table_name):
    world.session = connect_cassandra()
    world.session.execute("truncate {}".format(table_name))


def connect_cassandra():
    connected = False
    attempt = 0
    session = None
    while not connected and attempt < 10:
        try:
            cluster = Cluster(['127.0.0.1'])
            session = cluster.connect()
            connected = True
        except cassandra.cluster.NoHostAvailable:
            attempt += 1
            time.sleep(10)

    return session


def write_dummy_file(path, mtime_str, contents=None):
    # create the file. if there's some contents, write them too
    with open(path, 'w') as f:
        if contents is not None:
            f.write(contents)
            f.flush()
        f.close()
    # we set the access and modification times for the file we just created
    # this time is set as seconds since epoch
    t = datetime.datetime.strptime(mtime_str, '%Y-%m-%d %H:%M:%S')
    mtime = (t - datetime.datetime(1970, 1, 1)).total_seconds()
    atime = mtime
    os.utime(path, (atime, mtime))
