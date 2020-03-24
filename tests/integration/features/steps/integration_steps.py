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
import uuid

from behave import given, when, then
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

from medusa.config import (
    MedusaConfig,
    StorageConfig,
    CassandraConfig,
    MonitoringConfig,
    ChecksConfig,
)
from medusa.config import _namedtuple_from_dict
from medusa.storage import Storage
from medusa.monitoring import LocalMonitoring

storage_prefix = "{}-{}".format(datetime.datetime.now().isoformat(), str(uuid.uuid4()))


def kill_cassandra():
    p = subprocess.Popen(["ps", "-A"], stdout=subprocess.PIPE)
    out, err = p.communicate()
    for line in out.splitlines():
        if b"org.apache.cassandra.service.CassandraDaemon" in line:
            logging.info(line)
            pid = int(line.split(None, 1)[0])
            os.kill(pid, signal.SIGKILL)


def cleanup_monitoring(context):
    LocalMonitoring(context.medusa_config.monitoring).truncate_metric_file()


def cleanup_storage(context, storage_provider):
    if storage_provider == "local":
        if os.path.isdir(os.path.join("/tmp", "medusa_it_bucket")):
            shutil.rmtree(os.path.join("/tmp", "medusa_it_bucket"))
        os.makedirs(os.path.join("/tmp", "medusa_it_bucket"))
    elif storage_provider == "google_storage" or storage_provider.find("s3") == 0:
        storage = Storage(config=context.medusa_config.storage)
        objects = storage.storage_driver.list_objects(storage._prefix)
        for obj in objects:
            storage.storage_driver.delete_object(obj)


@given(r'I have a fresh ccm cluster running named "{cluster_name}"')
def _i_have_a_fresh_ccm_cluster_running(context, cluster_name):
    context.cassandra_version = "2.2.14"
    context.session = None
    context.cluster_name = cluster_name
    subprocess.run(["ccm", "stop"], stdout=PIPE, stderr=PIPE)
    kill_cassandra()
    res = subprocess.run(
        ["ccm", "switch", context.cluster_name], stdout=PIPE, stderr=PIPE
    )
    if b"does not appear to be a valid cluster" not in res.stderr:
        subprocess.check_call(
            ["ccm", "remove", context.cluster_name], stdout=PIPE, stderr=PIPE
        )
    subprocess.check_call(
        [
            "ccm",
            "create",
            context.cluster_name,
            "-v",
            "binary:" + context.cassandra_version,
            "-n",
            "1",
        ]
    )

    os.popen("ccm node1 updateconf 'storage_port: 7011'").read()
    if os.uname().sysname == "Linux":
        os.popen(
            """sed -i 's/#MAX_HEAP_SIZE="4G"/MAX_HEAP_SIZE="256m"/' ~/.ccm/"""
            + context.cluster_name
            + """/node1/conf/cassandra-env.sh"""
        ).read()
        os.popen(
            """sed -i 's/#HEAP_NEWSIZE="800M"/HEAP_NEWSIZE="200M"/' ~/.ccm/"""
            + context.cluster_name
            + """/node1/conf/cassandra-env.sh"""
        ).read()
    os.popen("LOCAL_JMX=yes ccm start --no-wait").read()
    context.session = connect_cassandra()


@given(r'I am using "{storage_provider}" as storage provider')
def i_am_using_storage_provider(context, storage_provider):
    logging.info("Starting the tests")
    if not hasattr(context, "cluster_name"):
        context.cluster_name = "test"
    config = configparser.ConfigParser(interpolation=None)

    if storage_provider == "local":
        if os.path.isdir(os.path.join("/tmp", "medusa_it_bucket")):
            shutil.rmtree(os.path.join("/tmp", "medusa_it_bucket"))
        os.makedirs(os.path.join("/tmp", "medusa_it_bucket"))

        config["storage"] = {
            "host_file_separator": ",",
            "bucket_name": "medusa_it_bucket",
            "key_file": "",
            "storage_provider": "local",
            "fqdn": "localhost",
            "api_key_or_username": "",
            "api_secret_or_password": "",
            "base_path": "/tmp",
            "prefix": storage_prefix
        }
    elif storage_provider == "google_storage":
        config["storage"] = {
            "host_file_separator": ",",
            "bucket_name": "medusa_it_bucket",
            "key_file": "~/medusa_credentials.json",
            "storage_provider": "google_storage",
            "fqdn": "localhost",
            "api_key_or_username": "",
            "api_secret_or_password": "",
            "base_path": "/tmp",
            "prefix": storage_prefix
        }
    elif storage_provider.startswith("s3"):
        config["storage"] = {
            "host_file_separator": ",",
            "bucket_name": "tlp-medusa-dev",
            "key_file": "~/.aws/credentials",
            "storage_provider": storage_provider,
            "fqdn": "localhost",
            "api_key_or_username": "",
            "api_secret_or_password": "",
            "api_profile": "default",
            "base_path": "/tmp",
            "multi_part_upload_threshold": 1 * 1024,
            "concurrent_transfers": 4,
            "prefix": storage_prefix,
            "aws_cli_path": "aws"
        }

    config["cassandra"] = {
        "is_ccm": 1,
        "stop_cmd": "ccm stop",
        "start_cmd": "ccm start",
        "cql_username": "cassandra",
        "cql_password": "cassandra",
        "config_file": os.path.expanduser(
            os.path.join(
                "~/.ccm", context.cluster_name, "node1", "conf", "cassandra.yaml"
            )
        ),
        "sstableloader_bin": os.path.expanduser(
            os.path.join(
                "~/.ccm",
                "repository",
                context.cassandra_version,
                "bin",
                "sstableloader",
            )
        ),
    }

    config["monitoring"] = {"monitoring_provider": "local"}

    context.medusa_config = MedusaConfig(
        storage=_namedtuple_from_dict(StorageConfig, config["storage"]),
        cassandra=_namedtuple_from_dict(CassandraConfig, config["cassandra"]),
        monitoring=_namedtuple_from_dict(MonitoringConfig, config["monitoring"]),
        ssh=None,
        restore=None,
        logging=None
    )
    cleanup_storage(context, storage_provider)
    cleanup_monitoring(context)


@when(r'I create the "{table_name}" table in keyspace "{keyspace_name}"')
def _i_create_the_whatever_table(context, table_name, keyspace_name):
    keyspace = """CREATE KEYSPACE IF NOT EXISTS {} WITH replication = {{'class':'SimpleStrategy',
    'replication_factor':1}}"""
    context.session.execute(keyspace.format(keyspace_name))

    table = "CREATE TABLE IF NOT EXISTS {}.{} (id timeuuid PRIMARY KEY, value text);"
    context.session.execute(table.format(keyspace_name, table_name))


@when('I create the "{table_name}" table with secondary index in keyspace "{keyspace_name}"')
def _i_create_the_table_with_si(context, table_name, keyspace_name):
    keyspace = """CREATE KEYSPACE IF NOT EXISTS {} WITH replication = {{'class':'SimpleStrategy',
    'replication_factor':1}}"""
    context.session.execute(keyspace.format(keyspace_name))

    table = "CREATE TABLE IF NOT EXISTS {}.{} (id timeuuid PRIMARY KEY, value text);"
    context.session.execute(table.format(keyspace_name, table_name))

    si = "CREATE INDEX IF NOT EXISTS {}_idx ON {}.{} (value);"
    context.session.execute(si.format(table_name, keyspace_name, table_name))


@when(r'I load {nb_rows} rows in the "{table_name}" table')
def _i_load_rows_in_the_whatever_table(context, nb_rows, table_name):
    for i in range(int(nb_rows)):
        context.session.execute(
            "INSERT INTO {} (id, value) VALUES(now(), '{}')".format(table_name, i)
        )


@when(r'I run a "{command}" command')
def _i_run_a_whatever_command(context, command):
    os.popen(command).read()


@when(r'I perform a backup in "{backup_mode}" mode of the node named "{backup_name}"')
def _i_perform_a_backup_of_the_node_named_backupname(context, backup_mode, backup_name):
    (actual_backup_duration, actual_start, end, node_backup, node_backup_cache, num_files, start) \
        = medusa.backup.main(context.medusa_config, backup_name, None, backup_mode)
    context.latest_backup_cache = node_backup_cache


@then(r'I can see the backup named "{backup_name}" when I list the backups')
def _i_can_see_the_backup_named_backupname_when_i_list_the_backups(
    context, backup_name
):
    storage = Storage(config=context.medusa_config.storage)
    cluster_backups = storage.list_cluster_backups()
    found = False
    for backup in cluster_backups:
        if backup.name == backup_name:
            found = True

    assert found is True


@then(r'some files from the previous backup were not reuploaded')
def _some_files_from_the_previous_backup_were_not_reuploaded(context):
    assert context.latest_backup_cache.replaced > 0


@then(r'I cannot see the backup named "{backup_name}" when I list the backups')
def _i_cannot_see_the_backup_named_backupname_when_i_list_the_backups(
    context, backup_name
):
    storage = Storage(config=context.medusa_config.storage)
    cluster_backups = storage.list_cluster_backups()
    found = False
    for backup in cluster_backups:
        if backup.name == backup_name:
            found = True

    assert found is False


@then('I can see the backup status for "{backup_name}" when I run the status command')
def _i_can_see_backup_status_when_i_run_the_status_command(context, backup_name):
    medusa.status.status(config=context.medusa_config, backup_name=backup_name)


@then(r"I can see no backups when I list the backups")
def _i_can_see_no_backups(context):
    storage = Storage(config=context.medusa_config.storage)
    cluster_backups = storage.list_cluster_backups()
    assert 0 == len(list(cluster_backups))


@then(
    r'the backup named "{backup_name}" has {nb_sstables} SSTables '
    + r'for the "{table_name}" table in keyspace "{keyspace}"'
)
def _the_backup_named_backupname_has_nb_sstables_for_the_whatever_table(
    context, backup_name, nb_sstables, table_name, keyspace
):
    storage = Storage(config=context.medusa_config.storage)
    path = os.path.join(
        storage.prefix_path + context.medusa_config.storage.fqdn, backup_name, "data", keyspace, table_name
    )
    objects = storage.storage_driver.list_objects(path)
    sstables = list(filter(lambda obj: "-Data.db" in obj.name, objects))
    if len(sstables) != int(nb_sstables):
        logging.error("{} SSTables : {}".format(len(sstables), sstables))
        logging.error("Was expecting {} SSTables".format(nb_sstables))
        assert len(sstables) == int(nb_sstables)


@then(r'I can verify the backup named "{backup_name}" successfully')
def _i_can_verify_the_backup_named_successfully(context, backup_name):
    medusa.verify.verify(context.medusa_config, backup_name)


@when(r'I restore the backup named "{backup_name}"')
def _i_restore_the_backup_named(context, backup_name):
    medusa.restore_node.restore_node(
        context.medusa_config,
        Path("/tmp"),
        backup_name,
        in_place=True,
        keep_auth=False,
        seeds=None,
        verify=None,
        keyspaces={},
        tables={},
        use_sstableloader=False,
    )


@when(r'I restore the backup named "{backup_name}" with the sstableloader')
def _i_restore_the_backup_named_with_sstableloader(context, backup_name):
    medusa.restore_node.restore_node(
        context.medusa_config,
        Path("/tmp"),
        backup_name,
        in_place=True,
        keep_auth=False,
        seeds=None,
        verify=None,
        keyspaces={},
        tables={},
        use_sstableloader=True,
    )


@when(r'I restore the backup named "{backup_name}" for "{fqtn}" table')
def _i_restore_the_backup_named_for_table(context, backup_name, fqtn):
    medusa.restore_node.restore_node(
        context.medusa_config,
        Path("/tmp"),
        backup_name,
        in_place=True,
        keep_auth=False,
        seeds=None,
        verify=None,
        keyspaces={},
        tables={fqtn},
        use_sstableloader=False,
    )


@then(r'I have {nb_rows} rows in the "{table_name}" table')
def _i_have_rows_in_the_table(context, nb_rows, table_name):
    context.session = connect_cassandra()
    rows = context.session.execute("select count(*) as nb from {}".format(table_name))
    assert rows[0].nb == int(nb_rows)


@then(r'I can see the backup index entry for "{backup_name}"')
def _the_backup_named_backupname_is_present_in_the_index(context, backup_name):
    storage = Storage(config=context.medusa_config.storage)
    fqdn = context.medusa_config.storage.fqdn
    path = os.path.join(
        "{}index/backup_index".format(storage.prefix_path), backup_name, "tokenmap_{}.json".format(fqdn)
    )
    tokenmap_from_index = storage.storage_driver.get_blob_content_as_string(path)
    path = os.path.join(storage.prefix_path + fqdn, backup_name, "meta", "tokenmap.json")
    tokenmap_from_backup = storage.storage_driver.get_blob_content_as_string(path)
    # Check that we have the manifest as well there
    manifest_path = os.path.join(
        "{}index/backup_index".format(storage.prefix_path), backup_name, "manifest_{}.json".format(fqdn)
    )
    manifest_from_index = storage.storage_driver.get_blob_content_as_string(
        manifest_path
    )
    path = os.path.join(storage.prefix_path + fqdn, backup_name, "meta", "manifest.json")
    manifest_from_backup = storage.storage_driver.get_blob_content_as_string(path)
    assert (
        tokenmap_from_backup == tokenmap_from_index
        and manifest_from_backup == manifest_from_index
    )


@then(
    r'I can see the latest backup for "{expected_fqdn}" being called "{expected_backup_name}"'
)
def _the_latest_backup_for_fqdn_is_called_backupname(
    context, expected_fqdn, expected_backup_name
):
    storage = Storage(config=context.medusa_config.storage)
    latest_backup = storage.latest_node_backup(fqdn=expected_fqdn)
    assert latest_backup.name == expected_backup_name


@then(r'there is no latest backup for node "{fqdn}"')
def _there_is_no_latest_backup_for_node_fqdn(context, fqdn):
    storage = Storage(config=context.medusa_config.storage)
    node_backup = storage.latest_node_backup(fqdn=fqdn)
    assert node_backup is None


@when(
    r'node "{fqdn}" fakes a complete backup named "{backup_name}" on "{backup_datetime}"'
)
def _node_fakes_a_complete_backup(context, fqdn, backup_name, backup_datetime):
    storage = Storage(config=context.medusa_config.storage)
    path_root = "/tmp/medusa_it_bucket"

    fake_tokenmap = json.dumps(
        {
            "n1": {"tokens": [1], "is_up": True},
            "n2": {"tokens": [2], "is_up": True},
            "n3": {"tokens": [3], "is_up": True},
        }
    )

    dir_path = os.path.join(path_root, storage.prefix_path + "index", "backup_index", backup_name)
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)

    # fake token map, manifest and schema in index
    path_tokenmap = "{}/{}index/backup_index/{}/tokenmap_{}.json".format(
        path_root, storage.prefix_path, backup_name, fqdn
    )
    write_dummy_file(path_tokenmap, backup_datetime, fake_tokenmap)
    path_manifest = "{}/{}index/backup_index/{}/manifest_{}.json".format(
        path_root, storage.prefix_path, backup_name, fqdn
    )
    write_dummy_file(path_manifest, backup_datetime, fake_tokenmap)
    path_schema = "{}/{}index/backup_index/{}/schema_{}.cql".format(
        path_root, storage.prefix_path, backup_name, fqdn
    )
    write_dummy_file(path_schema, backup_datetime, fake_tokenmap)

    dir_path = os.path.join(path_root, storage.prefix_path + "index", "latest_backup", fqdn)
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)

    # fake token map in latest_backup
    path_latest_backup_tokenmap = "{}/{}index/latest_backup/{}/tokenmap.json".format(
        path_root, storage.prefix_path, fqdn
    )
    write_dummy_file(path_latest_backup_tokenmap, backup_datetime, fake_tokenmap)

    # fake token name in latest_backup
    path_latest_backup_name = "{}/{}index/latest_backup/{}/backup_name.txt".format(
        path_root, storage.prefix_path, fqdn
    )
    write_dummy_file(path_latest_backup_name, backup_datetime)

    # fake actual backup folder
    dir_path = os.path.join(path_root, storage.prefix_path + fqdn, backup_name, "meta")
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)

    # fake schema in actual backup path
    path_schema = "{}/{}{}/{}/meta/schema.cql".format(path_root, storage.prefix_path, fqdn, backup_name)
    write_dummy_file(path_schema, backup_datetime)

    # fake manifest in actual backup path
    path_manifest = "{}/{}{}/{}/meta/manifest.json".format(path_root, storage.prefix_path, fqdn, backup_name)
    write_dummy_file(path_manifest, backup_datetime)

    # fake token map in actual backup path
    path_tokenmap = "{}/{}{}/{}/meta/tokenmap.json".format(path_root, storage.prefix_path, fqdn, backup_name)
    write_dummy_file(path_tokenmap, backup_datetime, fake_tokenmap)


@then(r'the latest cluster backup is "{expected_backup_name}"')
def _the_latest_cluster_backup_is(context, expected_backup_name):
    storage = Storage(config=context.medusa_config.storage)
    backup = storage.latest_cluster_backup()
    assert expected_backup_name == backup.name


@then(r"there is no latest complete backup")
def _there_is_no_latest_complete_backup(context):
    storage = Storage(config=context.medusa_config.storage)
    actual_backup = storage.latest_complete_cluster_backup()
    assert actual_backup is None


@then(r"I can list and print backups without errors")
def _can_list_print_backups_without_error(context):
    medusa.listing.list_backups(config=context.medusa_config, show_all=True)


@then(r'the latest complete cluster backup is "{expected_backup_name}"')
def _the_latest_complete_cluster_backup_is(context, expected_backup_name):
    storage = Storage(config=context.medusa_config.storage)
    actual_backup = storage.latest_complete_cluster_backup()
    if actual_backup is not None:
        assert expected_backup_name == actual_backup.name


@when(r"I truncate the backup index")
def _truncate_the_index(context):
    storage = Storage(config=context.medusa_config.storage)
    path_root = "/tmp/medusa_it_bucket"
    index_path = "{}/{}index".format(path_root, storage.prefix_path)
    shutil.rmtree(index_path)


@when(r"I truncate the backup folder")
def _truncate_the_backup_folder(context):
    storage = Storage(config=context.medusa_config.storage)
    path_root = "/tmp/medusa_it_bucket"
    backup_path = "{}/{}localhost".format(path_root, storage.prefix_path)
    shutil.rmtree(backup_path)


@when(r"I re-create the backup index")
def _recreate_the_index(context):
    medusa.index.build_indices(context.medusa_config, False)


@then(r"I can report latest backups without errors")
def _can_report_backups_without_errors(context):
    medusa.report_latest.report_latest(config=context.medusa_config, push_metrics=True)


@then(r"the backup index does not exist")
def _the_backup_index_does_not_exist(context):
    storage = Storage(config=context.medusa_config.storage)
    assert False is medusa.index.index_exists(storage)


@then(r"the backup index exists")
def _the_backup_index_exists(context):
    storage = Storage(config=context.medusa_config.storage)
    assert True is medusa.index.index_exists(storage)


@then(
    r'I can see {nb_sstables} SSTables in the SSTable pool for the "{table_name}" table in keyspace "{keyspace}"'
)
def _i_can_see_nb_sstables_in_the_sstable_pool(
    context, nb_sstables, table_name, keyspace
):
    storage = Storage(config=context.medusa_config.storage)
    path = os.path.join(
        storage.prefix_path + context.medusa_config.storage.fqdn, "data", keyspace, table_name
    )
    objects = storage.storage_driver.list_objects(path)
    sstables = list(filter(lambda obj: "-Data.db" in obj.name, objects))
    if len(sstables) != int(nb_sstables):
        logging.error("{} SSTables : {}".format(len(sstables), sstables))
        logging.error("Was expecting {} SSTables".format(nb_sstables))
        assert len(sstables) == int(nb_sstables)


@then(
    r'backup named "{backup_name}" has {nb_files} files '
    + r'in the manifest for the "{table_name}" table in keyspace "{keyspace_name}"'
)
def _backup_named_something_has_nb_files_in_the_manifest(
    context, backup_name, nb_files, table_name, keyspace_name
):
    storage = Storage(config=context.medusa_config.storage)
    node_backups = storage.list_node_backups()
    # Find the backup we're looking for
    target_backup = list(
        filter(lambda backup: backup.name == backup_name, node_backups)
    )[0]
    # Parse its manifest
    manifest = json.loads(target_backup.manifest)
    for section in manifest:
        if (
            section["keyspace"] == keyspace_name
            and section["columnfamily"][: len(table_name)] == table_name
        ):
            if len(section["objects"]) != int(nb_files):
                logging.error(
                    "Was expecting {} files, got {}".format(
                        nb_files, len(section["objects"])
                    )
                )
                assert len(section["objects"]) == int(nb_files)


@then(r'I can see secondary index files in the "{backup_name}" files')
def _i_can_see_secondary_index_files_in_backup(context, backup_name):
    storage = Storage(config=context.medusa_config.storage)
    node_backups = storage.list_node_backups()
    target_backup = list(filter(lambda backup: backup.name == backup_name, node_backups))[0]
    manifest = json.loads(target_backup.manifest)
    seen_index_files = 0
    for section in manifest:
        for f in section['objects']:
            if 'idx' in f['path']:
                seen_index_files += 1
    assert seen_index_files > 0


@then(r'verify fails on the backup named "{backup_name}"')
def _verify_fails_on_the_backup_named(context, backup_name):
    try:
        medusa.verify.verify(context.medusa_config, backup_name)
        raise AssertionError("Backup verification should have failed but didn't.")
    except RuntimeError:
        # This exception is required to be raised to validate the step
        pass


@when(r"I purge the backup history to retain only {backup_count} backups")
def _i_purge_the_backup_history_to_retain_only_nb_backups(context, backup_count):
    medusa.purge.main(context.medusa_config, max_backup_count=int(backup_count))


@then(r"I see {metrics_count} metrics emitted")
def _i_see_metrics_emitted(context, metrics_count):
    metrics = list(LocalMonitoring(context.medusa_config).load_metrics())
    logging.info("There is {} metrics".format(len(metrics)))
    logging.info("The metrics are: {}".format(metrics))
    assert int(len(metrics)) == int(metrics_count)


@when(r'I truncate the "{table_name}" table')
def _i_truncate_the_table(context, table_name):
    context.session = connect_cassandra()
    context.session.execute("truncate {}".format(table_name))


@then(r'I can verify the restore verify query "{query}" returned {expected_rows} rows')
def _i_can_verify_the_restore_verify_query_returned_rows(context, query, expected_rows):
    restore_config = {
        "health_check": "cql",
        "query": query,
        "expected_rows": expected_rows,
    }
    custom_config = MedusaConfig(
        storage=context.medusa_config.storage,
        cassandra=context.medusa_config.cassandra,
        monitoring=context.medusa_config.monitoring,
        restore=_namedtuple_from_dict(ChecksConfig, restore_config),
        ssh=None,
        logging=None
    )
    medusa.verify_restore.verify_restore(["localhost"], custom_config)


def connect_cassandra():
    connected = False
    attempt = 0
    session = None
    while not connected and attempt < 10:
        try:
            cluster = Cluster(["127.0.0.1"])
            session = cluster.connect()
            connected = True
        except cassandra.cluster.NoHostAvailable:
            attempt += 1
            time.sleep(10)

    return session


def write_dummy_file(path, mtime_str, contents=None):
    # create the file. if there's some contents, write them too
    with open(path, "w") as f:
        if contents is not None:
            f.write(contents)
            f.flush()
        f.close()
    # we set the access and modification times for the file we just created
    # this time is set as seconds since epoch
    t = datetime.datetime.strptime(mtime_str, "%Y-%m-%d %H:%M:%S")
    mtime = (t - datetime.datetime(1970, 1, 1)).total_seconds()
    atime = mtime
    os.utime(path, (atime, mtime))
