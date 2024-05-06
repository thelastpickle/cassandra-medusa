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

import asyncio
import datetime
import glob
import json
import logging
import os
import random
import shutil
import signal
import subprocess
import sys
import time
import uuid
from pathlib import Path
from ssl import SSLContext, PROTOCOL_TLS, PROTOCOL_TLSv1_2, CERT_REQUIRED
from subprocess import PIPE
from zipfile import ZipFile

import cassandra
import cassandra.cluster
import requests
from behave import given, when, then
from cassandra import ProtocolVersion
from cassandra.cluster import Cluster

import medusa.backup_node
import medusa.config
import medusa.download
import medusa.filtering
import medusa.fetch_tokenmap
import medusa.index
import medusa.listing
import medusa.purge
import medusa.purge_decommissioned
import medusa.report_latest
import medusa.restore_node
import medusa.service.grpc.client
import medusa.status
import medusa.verify
import medusa.verify_restore
import medusa.service.grpc.client
import medusa.service.grpc.server

from medusa import backup_node
from medusa.backup_manager import BackupMan
from medusa.config import (
    MedusaConfig,
    StorageConfig,
    CassandraConfig,
    MonitoringConfig,
    ChecksConfig,
    GrpcConfig,
    KubernetesConfig,
)
from medusa.config import _namedtuple_from_dict
from medusa.monitoring import LocalMonitoring
from medusa.service.grpc import medusa_pb2
from medusa.storage import Storage
from medusa.utils import MedusaTempFile


storage_prefix = "{}-{}".format(datetime.datetime.now().isoformat(), str(uuid.uuid4()))
os.chdir("..")
certfile = "{}/resources/local_with_ssl/rootCa.crt".format(os.getcwd())
usercert = "{}/resources/local_with_ssl/client.pem".format(os.getcwd())
userkey = "{}/resources/local_with_ssl/client.key.pem".format(os.getcwd())
keystore_path = "{}/resources/local_with_ssl/127.0.0.1.jks".format(os.getcwd())
trustore_path = "{}/resources/local_with_ssl/generic-server-truststore.jks".format(os.getcwd())
config_checks = {"health_check": "cql", "enable_md5_checks": "false"}
NOT_A_VALID_CLUSTER_MSG = b"does not appear to be a valid cluster"
STARTING_TESTS_MSG = "Starting the tests"
CCM_STOP = "ccm stop"
CCM_START = "ccm start"
CCM_DIR = "~/.ccm"
BUCKET_ROOT = "/tmp/medusa_it_bucket"
CASSANDRA_YAML = "cassandra.yaml"
AWS_CREDENTIALS = "~/.aws/credentials"
GCS_CREDENTIALS = "~/medusa_credentials.json"

# hide cassandra driver logs, they are overly verbose and we don't really need them for tests
for logger_name in {'cassandra.io', 'cassandra.pool', 'cassandra.cluster', 'cassandra.connection'}:
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.CRITICAL)


def kill_cassandra():
    p = subprocess.Popen(["ps", "-Af"], stdout=subprocess.PIPE)
    out, err = p.communicate()
    for line in out.splitlines():
        if b"org.apache.cassandra.service.CassandraDaemon" in line or b"com.datastax.bdp.DseModul" in line:
            pid = int(line.split(None, 2)[1])
            logging.info(f'Killing Cassandra or DSE with PID {pid}')
            os.kill(pid, signal.SIGKILL)


def cleanup_monitoring(context):
    LocalMonitoring(context.medusa_config.monitoring).truncate_metric_file()


def cleanup_storage(context, storage_provider):
    if storage_provider == "local":
        if os.path.isdir(os.path.join("/tmp", "medusa_it_bucket")):
            shutil.rmtree(os.path.join("/tmp", "medusa_it_bucket"))
        os.makedirs(os.path.join("/tmp", "medusa_it_bucket"))
    else:
        with Storage(config=context.medusa_config.storage) as storage:
            objects = storage.storage_driver.list_objects(storage._prefix)
            storage.delete_objects(objects)


def get_client_encryption_opts(keystore_path, trustore_path):
    # Python 3.10 has a reduced set of ciphers available. Ensure that a compatible cipher is used for Py310, and
    # allow older python versions to test older ciphers/TLS versions.
    if sys.version_info >= (3, 10):
        cipher_suite = "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384"
    else:
        cipher_suite = "TLS_RSA_WITH_AES_256_CBC_SHA"
    return f"""ccm node1 updateconf -y 'client_encryption_options: {{ enabled: true,
        optional: false,keystore: {keystore_path}, keystore_password: testdata1,
        require_client_auth: true,truststore: {trustore_path},  truststore_password: truststorePass1,
        protocol: TLS,algorithm: SunX509,store_type: JKS,cipher_suites: [{cipher_suite}]}}'"""


def tune_ccm_settings(cluster_name):
    if os.uname().sysname == "Linux":
        os.popen(
            """sed -i 's/#MAX_HEAP_SIZE="4G"/MAX_HEAP_SIZE="256m"/' ~/.ccm/"""
            + cluster_name
            + """/node1/conf/cassandra-env.sh"""
        ).read()
        os.popen(
            """sed -i 's/#HEAP_NEWSIZE="800M"/HEAP_NEWSIZE="200M"/' ~/.ccm/"""
            + cluster_name
            + """/node1/conf/cassandra-env.sh"""
        ).read()
    os.popen("LOCAL_JMX=yes ccm start --no-wait").read()


class GRPCServer:
    def __init__(self, config):
        if not os.path.isdir(os.path.join("/tmp", "medusa_grpc")):
            os.makedirs(os.path.join("/tmp", "medusa_grpc"))

        self.config = config
        medusa_conf_file = "/tmp/medusa_grpc/medusa.ini"
        with open(medusa_conf_file, "w") as config_file:
            self.config.write(config_file)

        self.grpc_server = medusa.service.grpc.server.Server(medusa_conf_file, testing=True)
        asyncio.get_event_loop().run_until_complete(self.grpc_server.serve())

    def destroy(self):
        self.grpc_server.shutdown(None, None)
        if os.path.isdir(os.path.join("/tmp", "medusa_grpc")):
            shutil.rmtree(os.path.join("/tmp", "medusa_grpc"))


class MgmtApiServer:
    @staticmethod
    def init(config, cluster_name):
        server = MgmtApiServer(config, cluster_name)
        server.start()
        return server

    @staticmethod
    def destroy():
        p = subprocess.Popen(["ps", "-Af"], stdout=subprocess.PIPE)
        out, err = p.communicate()
        for line in out.splitlines():
            if b"mgmtapi.sock" in line:
                logging.info(line)
                pid = int(line.split(None, 2)[1])
                os.kill(pid, signal.SIGKILL)

    def __init__(self, config, cluster_name):
        self.cluster_name = cluster_name
        shutil.copyfile("resources/grpc/mutual_auth_ca.pem", "/tmp/mutual_auth_ca.pem")
        shutil.copyfile("resources/grpc/mutual_auth_server.crt", "/tmp/mutual_auth_server.crt")
        shutil.copyfile("resources/grpc/mutual_auth_server.key", "/tmp/mutual_auth_server.key")

        env = {**os.environ, "MGMT_API_LOG_DIR": '/tmp'}
        cmd = ["java", "-jar", "/tmp/management-api-server/target/datastax-mgmtapi-server-0.1.0-SNAPSHOT.jar",
               "--tlscacert=/tmp/mutual_auth_ca.pem",
               "--tlscert=/tmp/mutual_auth_server.crt",
               "--tlskey=/tmp/mutual_auth_server.key",
               "--db-socket=/tmp/db.sock",
               "--host=unix:///tmp/mgmtapi.sock",
               "--host=http://localhost:8080",
               "--db-home={}/.ccm/{}/node1".format(str(Path.home()), self.cluster_name),
               "--explicit-start",
               "true",
               "--no-keep-alive",
               "true",
               ]
        subprocess.Popen(cmd, cwd=os.path.abspath("../"), env=env)

    @staticmethod
    def start():
        started = False
        start_count = 0
        while not started and start_count < 20:
            try:
                start_count += 1
                requests.post("https://127.0.0.1:8080/api/v0/lifecycle/start", verify="/tmp/mutual_auth_ca.pem",
                              cert=("/tmp/mutual_auth_client.crt", "/tmp/mutual_auth_client.key"))
                started = True
            except Exception:
                # wait for Cassandra to start
                time.sleep(1)

    @staticmethod
    def stop():
        requests.post("https://127.0.0.1:8080/api/v0/lifecycle/stop", verify="/tmp/mutual_auth_ca.pem",
                      cert=("/tmp/mutual_auth_client.crt", "/tmp/mutual_auth_client.key"))


@given(r'I have a fresh ccm cluster "{client_encryption}" running named "{cluster_name}"')
def _i_have_a_fresh_ccm_cluster_running(context, cluster_name, client_encryption):
    context.session = None
    context.cluster_name = cluster_name
    is_client_encryption_enable = False
    subprocess.run(["ccm", "stop"], stdout=PIPE, stderr=PIPE)
    kill_cassandra()
    res = subprocess.run(
        ["ccm", "switch", context.cluster_name], stdout=PIPE, stderr=PIPE
    )
    if NOT_A_VALID_CLUSTER_MSG not in res.stderr:
        subprocess.check_call(
            ["ccm", "remove", context.cluster_name], stdout=PIPE, stderr=PIPE
        )
    subprocess.check_call(
        [
            "ccm",
            "create",
            context.cluster_name,
            "-v",
            context.cassandra_version,
            "-n",
            "1",
        ]
    )

    if client_encryption == 'with_client_encryption':
        is_client_encryption_enable = True
        update_client_encrytion_opts = get_client_encryption_opts(keystore_path, trustore_path)
        os.popen(update_client_encrytion_opts).read()

    tune_ccm_settings(context.cluster_name)
    context.session = connect_cassandra(is_client_encryption_enable)


@given(
    r'I have a fresh "{num_nodes}" node ccm cluster with jolokia "{client_encryption}" running named "{cluster_name}"'
)
def _i_have_fresh_cluster_with_num_nodes(context, num_nodes, client_encryption, cluster_name):
    _i_have_a_fresh_ccm_cluster_with_jolokia_running(context, cluster_name, client_encryption, num_nodes)


@given(r'I have a fresh ccm cluster with jolokia "{client_encryption}" running named "{cluster_name}"')
def _i_have_a_fresh_ccm_cluster_with_jolokia_running(context, cluster_name, client_encryption, num_nodes=1):
    context.session = None
    context.cluster_name = cluster_name
    is_client_encryption_enable = False
    subprocess.run(["ccm", "stop"], stdout=PIPE, stderr=PIPE)
    kill_cassandra()
    res = subprocess.run(
        ["ccm", "switch", context.cluster_name], stdout=PIPE, stderr=PIPE
    )
    if NOT_A_VALID_CLUSTER_MSG not in res.stderr:
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
            str(num_nodes),
        ]
    )

    if client_encryption == 'with_client_encryption':
        is_client_encryption_enable = True
        update_client_encrytion_opts = get_client_encryption_opts(keystore_path, trustore_path)
        os.popen(update_client_encrytion_opts).read()

    conf_file = os.path.expanduser("~/.ccm/{}/node1/conf/cassandra-env.sh".format(context.cluster_name))
    with open(conf_file, "a") as config_file:
        config_file.write(
            'JVM_OPTS="$JVM_OPTS -javaagent:/tmp/jolokia-jvm-1.6.2-agent.jar=port=8778,host=127.0.0.1"'
        )
    shutil.copyfile("resources/grpc/jolokia-jvm-1.6.2-agent.jar", "/tmp/jolokia-jvm-1.6.2-agent.jar")

    tune_ccm_settings(context.cluster_name)
    context.session = connect_cassandra(is_client_encryption_enable)


@given(r'I have a fresh ccm cluster with mgmt api "{client_encryption}" named "{cluster_name}"')
def _i_have_a_fresh_ccm_cluster_with_mgmt_api_running(context, cluster_name, client_encryption):
    context.session = None
    context.cluster_name = cluster_name
    is_client_encryption_enable = False
    subprocess.run(["ccm", "stop"], stdout=PIPE, stderr=PIPE)
    kill_cassandra()
    res = subprocess.run(
        ["ccm", "switch", context.cluster_name], stdout=PIPE, stderr=PIPE
    )
    if NOT_A_VALID_CLUSTER_MSG not in res.stderr:
        subprocess.check_call(
            ["ccm", "remove", context.cluster_name], stdout=PIPE, stderr=PIPE
        )
    subprocess.check_call(
        [
            "ccm",
            "create",
            context.cluster_name,
            "-v",
            context.cassandra_version,
            "-n",
            "1",
        ]
    )

    if client_encryption == 'with_client_encryption':
        is_client_encryption_enable = True
        update_client_encrytion_opts = get_client_encryption_opts(keystore_path, trustore_path)
        os.popen(update_client_encrytion_opts).read()

    tune_ccm_settings(context.cluster_name)
    context.session = connect_cassandra(is_client_encryption_enable)
    # stop the node via CCM as it needs to be started by the Management API
    os.popen(CCM_STOP).read()

    conf_file = os.path.expanduser("~/.ccm/{}/node1/conf/cassandra-env.sh".format(context.cluster_name))
    with open(conf_file, "a") as config_file:
        config_file.write(
            'JVM_OPTS="$JVM_OPTS -javaagent:/tmp/management-api-agent/target/datastax-mgmtapi-agent-0.1.0-SNAPSHOT.jar"'
        )
    # get the Cassandra Management API jars
    get_mgmt_api_jars(version=context.cassandra_version)


@given(r'I have a fresh DSE cluster version "{dse_version}" with "{client_encryption}" running named "{cluster_name}"')
def _i_have_a_fresh_dse_cluster(context, dse_version, client_encryption, cluster_name):
    context.cluster_name = cluster_name
    context.dse_version = dse_version
    kill_cassandra()
    subprocess.check_call([str(Path(".") / 'resources/dse/configure-dse.sh'), dse_version])
    subprocess.check_call([str(Path(".") / 'resources/dse/start-dse.sh'), dse_version])

    enable_client_server_encryption = client_encryption == 'with_client_encryption'
    context.client_server_encryption_enabled = enable_client_server_encryption
    context.session = connect_cassandra(enable_client_server_encryption)


@then(r'I stop the DSE cluster')
def _i_stop_the_dse_cluster(context):
    subprocess.check_call([str(Path(".") / 'resources/dse/stop-dse.sh'), context.dse_version])


@then(r'I delete the DSE cluster')
def _i_delete_the_dse_cluster(context):
    subprocess.check_call([str(Path(".") / 'resources/dse/delete-dse.sh'), context.dse_version])


@when(r'I run a DSE "{command}" command')
def _i_run_a_dse_command(context, command):
    subprocess.check_call([
        str(Path(".") / 'resources/dse/run-command.sh'),
        context.dse_version,
        *command.split(' ')
    ])


@given(r'I am using "{storage_provider}" as storage provider in ccm cluster "{client_encryption}"')
def i_am_using_storage_provider(context, storage_provider, client_encryption):
    context.storage_provider = storage_provider
    context.client_encryption = client_encryption
    context.medusa_config = get_medusa_config(context, storage_provider, client_encryption, None)
    cleanup_storage(context, storage_provider)
    cleanup_monitoring(context)


@given(r'I am using "{storage_provider}" as storage provider in ccm cluster "{client_encryption}" with gRPC server')
def i_am_using_storage_provider_with_grpc_server(context, storage_provider, client_encryption):
    config = parse_medusa_config(context, storage_provider, client_encryption,
                                 "http://127.0.0.1:8778/jolokia/", grpc='True', use_mgmt_api='False')
    context.storage_provider = storage_provider
    context.client_encryption = client_encryption
    context.grpc_server = GRPCServer(config)
    context.grpc_client = medusa.service.grpc.client.Client(
        "127.0.0.1:50051",
        channel_options=[('grpc.enable_retries', 0)]
    )

    context.medusa_config = MedusaConfig(
        file_path=None,
        storage=_namedtuple_from_dict(StorageConfig, config["storage"]),
        cassandra=_namedtuple_from_dict(CassandraConfig, config["cassandra"]),
        monitoring=_namedtuple_from_dict(MonitoringConfig, config["monitoring"]),
        ssh=None,
        checks=_namedtuple_from_dict(ChecksConfig, config["checks"]),
        logging=None,
        grpc=_namedtuple_from_dict(GrpcConfig, config["grpc"]),
        kubernetes=_namedtuple_from_dict(KubernetesConfig, config['kubernetes']),
    )

    cleanup_storage(context, storage_provider)

    # sleep for a few seconds to give gRPC server a chance to initialize
    time.sleep(3)


@when(r'I forget about all backups')
def i_forget_backups(context):
    # makes the gRPC server look like we've just started it
    BackupMan.remove_all_backups()


@given(r'I am using "{storage_provider}" as storage provider in ccm cluster "{client_encryption}" with mgmt api')
def i_am_using_storage_provider_with_grpc_server_and_mgmt_api(context, storage_provider, client_encryption):
    config = parse_medusa_config(
        context,
        storage_provider,
        client_encryption,
        cassandra_url="https://127.0.0.1:8080/api/v0/ops/node/snapshots",
        use_mgmt_api='True',
        grpc='True',
        ca_cert='/tmp/mutual_auth_ca.pem',
        tls_cert='/tmp/mutual_auth_client.crt',
        tls_key='/tmp/mutual_auth_client.key'
    )
    shutil.copyfile("resources/grpc/mutual_auth_ca.pem", "/tmp/mutual_auth_ca.pem")
    shutil.copyfile("resources/grpc/mutual_auth_client.crt", "/tmp/mutual_auth_client.crt")
    shutil.copyfile("resources/grpc/mutual_auth_client.key", "/tmp/mutual_auth_client.key")

    context.storage_provider = storage_provider
    context.client_encryption = client_encryption
    context.grpc_server = GRPCServer(config)
    context.grpc_client = medusa.service.grpc.client.Client(
        "127.0.0.1:50051",
        channel_options=[('grpc.enable_retries', 0)]
    )

    MgmtApiServer.destroy()
    context.mgmt_api_server = MgmtApiServer.init(config, context.cluster_name)

    context.medusa_config = MedusaConfig(
        file_path=None,
        storage=_namedtuple_from_dict(StorageConfig, config["storage"]),
        cassandra=_namedtuple_from_dict(CassandraConfig, config["cassandra"]),
        monitoring=_namedtuple_from_dict(MonitoringConfig, config["monitoring"]),
        ssh=None,
        checks=_namedtuple_from_dict(ChecksConfig, config["checks"]),
        logging=None,
        grpc=_namedtuple_from_dict(GrpcConfig, config["grpc"]),
        kubernetes=_namedtuple_from_dict(KubernetesConfig, config['kubernetes']),
    )

    cleanup_storage(context, storage_provider)

    is_client_encryption_enable = False
    if client_encryption == 'with_client_encryption':
        is_client_encryption_enable = True

    # sleep for a few seconds to give gRPC server a chance to initialize
    ready_count = 0
    while ready_count < 20:
        ready = 0
        try:
            ready = requests.get("https://127.0.0.1:8080/api/v0/probes/readiness", verify="/tmp/mutual_auth_ca.pem",
                                 cert=("/tmp/mutual_auth_client.crt", "/tmp/mutual_auth_client.key")).status_code
        except Exception:
            # wait for the server to be ready
            time.sleep(1)
        ready_count += 1
        if ready == 200:
            # server is ready, re-establish the session
            context.session = connect_cassandra(is_client_encryption_enable)
            break
        else:
            # wait for Cassandra to be ready
            time.sleep(1)


def get_args(context, storage_provider, client_encryption, cassandra_url, use_mgmt_api='False', grpc='False',
             ca_cert=None, tls_cert=None, tls_key=None):
    logging.info(STARTING_TESTS_MSG)
    if not hasattr(context, "cluster_name"):
        context.cluster_name = "test"

    is_dse = hasattr(context, "dse_version")
    if not is_dse:
        config_file = os.path.expanduser(
            os.path.join(
                CCM_DIR, context.cluster_name, "node1", "conf", CASSANDRA_YAML
            )
        )
        nodetool_executable = None  # will come from CCM
        nodetool_port = 7100
        is_ccm = 1
        start_cmd = CCM_START
        stop_cmd = CCM_STOP
    else:
        cwd = Path(".").absolute()
        relative_yaml = f'resources/dse/dse-{context.dse_version}/resources/cassandra/conf/cassandra.yaml'
        config_file = str(cwd / relative_yaml)
        nodetool_port = 7199
        nodetool_relative_path = f'resources/dse/dse-{context.dse_version}/resources/cassandra/bin/nodetool'
        nodetool_executable = str(cwd / nodetool_relative_path)
        is_ccm = 0
        start_cmd = f'resources/dse/start-dse.sh {context.dse_version}'
        stop_cmd = f'resources/dse/stop-dse.sh {context.dse_version}'

    storage_args = {"prefix": storage_prefix}
    cassandra_args = {
        "is_ccm": str(is_ccm),
        "stop_cmd": stop_cmd,
        "start_cmd": start_cmd,
        "cql_username": "cassandra",
        "cql_password": "cassandra",
        "config_file": config_file,
        "sstableloader_bin": os.path.expanduser(
            os.path.join(
                CCM_DIR,
                "repository",
                context.cassandra_version.replace(
                    "github:", "githubCOLON").replace("/", "SLASH"),
                "bin",
                "sstableloader",
            )
        ),
        "resolve_ip_addresses": "False",
        "use_sudo": "True",
        'nodetool_executable': nodetool_executable,
        "nodetool_port": str(nodetool_port)
    }

    if client_encryption == 'with_client_encryption':
        cassandra_args.update(
            {
                "certfile": certfile,
                "usercert": usercert,
                "userkey": userkey,
                "sstableloader_ts": trustore_path,
                "sstableloader_tspw": "truststorePass1",
                "sstableloader_ks": keystore_path,
                "sstableloader_kspw": "testdata1"
            }
        )

    grpc_args = {
        "grpc_enabled": grpc
    }

    kubernetes_args = {
        "k8s_enabled": use_mgmt_api,
        "cassandra_url": cassandra_url,
        "use_mgmt_api": use_mgmt_api,
        "ca_cert": ca_cert,
        "tls_cert": tls_cert,
        "tls_key": tls_key
    }

    args = {**storage_args, **cassandra_args, **config_checks, **grpc_args, **kubernetes_args}
    return args


def get_medusa_config(context, storage_provider, client_encryption, cassandra_url, use_mgmt_api='False', grpc='False'):
    args = get_args(context, storage_provider, client_encryption, cassandra_url, use_mgmt_api, grpc)

    is_dse = hasattr(context, 'dse_version')
    if is_dse:
        config_file = Path(os.path.join(os.path.abspath("."), f'resources/config/medusa-{storage_provider}-dse.ini'))
    else:
        config_file = Path(os.path.join(os.path.abspath("."), f'resources/config/medusa-{storage_provider}.ini'))

    create_storage_specific_resources(storage_provider)
    config = medusa.config.load_config(args, config_file)
    return config


def parse_medusa_config(
        context, storage_provider, client_encryption, cassandra_url, use_mgmt_api='False', grpc='False',
        ca_cert=None, tls_cert=None, tls_key=None
):
    args = get_args(context, storage_provider, client_encryption, cassandra_url, use_mgmt_api, grpc,
                    ca_cert, tls_cert, tls_key)
    config_file = Path(os.path.join(os.path.abspath("."), f'resources/config/medusa-{storage_provider}.ini'))
    create_storage_specific_resources(storage_provider)
    config = medusa.config.parse_config(args, config_file)
    return config


def create_storage_specific_resources(storage_provider):
    if storage_provider == "local":
        if os.path.isdir(os.path.join("/tmp", "medusa_it_bucket")):
            shutil.rmtree(os.path.join("/tmp", "medusa_it_bucket"))
        os.makedirs(os.path.join("/tmp", "medusa_it_bucket"))


@when(r'I create the "{table_name}" table in keyspace "{keyspace_name}"')
def _i_create_the_whatever_table(context, table_name, keyspace_name):
    keyspace = """CREATE KEYSPACE IF NOT EXISTS {} WITH replication = {{'class':'SimpleStrategy',
    'replication_factor':1}}"""
    context.session.execute(keyspace.format(keyspace_name))

    table = "CREATE TABLE IF NOT EXISTS {}.{} (id timeuuid PRIMARY KEY, value text);"
    context.session.execute(table.format(keyspace_name, table_name))

    # wait for the table to be created on both nodes
    # normally a driver would do this, but for some reason it isn't.
    time.sleep(1)


@when('I create the "{table_name}" table with secondary index in keyspace "{keyspace_name}"')
def _i_create_the_table_with_si(context, table_name, keyspace_name):
    _i_create_the_whatever_table(context, table_name, keyspace_name)

    si = "CREATE INDEX IF NOT EXISTS {}_idx ON {}.{} (value);"
    context.session.execute(si.format(table_name, keyspace_name, table_name))


@when(r'I create a search index on the "{table_name}" table in keyspace "{keyspace_name}"')
def _i_create_a_search_index(context, table_name, keyspace_name):
    subprocess.check_call(
        [str(Path(".") / 'resources/dse/configure-dse-search.sh'), context.dse_version, keyspace_name, table_name]
    )


@when("I wait for the DSE Search indexes to be rebuilt")
def _i_wait_for_indexes_to_be_rebuilt(context):
    session = connect_cassandra(context.client_server_encryption_enabled)
    rows = session.execute('SELECT core_name FROM solr_admin.solr_resources')
    fqtns_with_search = {r.core_name for r in rows}
    assert len(fqtns_with_search) > 0

    for fqtn in fqtns_with_search:
        attempts = 0
        while True:
            cmd = f'dsetool core_indexing_status {fqtn}'
            p = subprocess.Popen([
                str(Path(".") / 'resources/dse/run-command.sh'),
                context.dse_version,
                *cmd.split(' ')
            ], stdout=PIPE)
            stdout, _ = p.communicate()
            output = b''.join(stdout.splitlines())
            if b'FINISHED' in output:
                logging.debug(f'DSE Search rebuild for {fqtn} finished')
                break
            logging.debug(f'DSE Search rebuild for {fqtn} not yet finished, waiting...')
            attempts += 1
            if attempts > 5:
                logging.error(f'DSE Search rebuild of {fqtn} did not finish in time')
                raise RuntimeError(f'DSE Search rebuild of {fqtn} did not finish in time')
            time.sleep(2)


@then(r'I can make a search query against the "{keyspace_name}"."{table_name}" table')
def _i_can_make_a_search_query(context, keyspace_name, table_name):

    # make a new session because this step runs after a restart which might break the session in the context
    session = connect_cassandra(context.client_server_encryption_enabled)

    search_query = f'SELECT * FROM {keyspace_name}.{table_name} WHERE solr_query = \'{{"q":"value:42"}}\';'
    rows = session.execute(search_query)
    results = [r.id for r in rows]
    assert len(results) > 0


@when(r'I load {nb_rows} rows in the "{table_name}" table')
def _i_load_rows_in_the_whatever_table(context, nb_rows, table_name):
    for i in range(int(nb_rows)):
        context.session.execute(
            "INSERT INTO {} (id, value) VALUES(now(), '{}')".format(table_name, i)
        )


@when(r'I run a "{command}" command')
def _i_run_a_whatever_command(context, command):
    os.popen(command).read()


@when(r'I perform a backup in "{backup_mode}" mode of the node named'
      r' "{backup_name}" with md5 checks "{md5_enabled_str}"')
def _i_perform_a_backup_of_the_node_named_backupname(context, backup_mode, backup_name, md5_enabled_str):
    BackupMan.register_backup(backup_name, is_async=False)
    (_, _, _, _, num_files, num_replaced, num_kept, _, _) = backup_node.handle_backup(
        context.medusa_config, backup_name, None, str(md5_enabled_str).lower() == "enabled", backup_mode
    )
    context.latest_num_files = num_files
    context.latest_backup_replaced = num_replaced
    context.latest_num_kept = num_kept


@when(r'I perform a backup over gRPC in "{backup_mode}" mode of the node named "{backup_name}"')
def _i_perform_grpc_backup_of_node_named_backupname(context, backup_mode, backup_name):
    asyncio.get_event_loop().run_until_complete(context.grpc_client.backup(backup_name, backup_mode))


@when(r'I perform an async backup over gRPC in "{backup_mode}" mode of the node named "{backup_name}"')
def _i_perform_grpc_async_backup_of_node_named_backupname(context, backup_mode, backup_name):
    asyncio.get_event_loop().run_until_complete(context.grpc_client.async_backup(backup_name, backup_mode))


@then(r'I wait for the async backup "{backup_name}" to finish')
def _i_wait_for_async_backup_to_finish(context, backup_name):
    while True:
        status = asyncio.get_event_loop().run_until_complete(context.grpc_client.get_backup_status(backup_name))
        if status == medusa_pb2.StatusType.SUCCESS:
            break
        logging.debug(f'Backup {backup_name} is not yet finished, waiting...')
        time.sleep(1)


@when(r'I perform a backup over gRPC in "{backup_mode}" mode of the node named "{backup_name}" and it fails')
def _i_perform_grpc_backup_of_node_named_backupname_fails(context, backup_mode, backup_name):
    try:
        asyncio.get_event_loop().run_until_complete(context.grpc_client.backup(backup_name, backup_mode))
        raise AssertionError("Backup process should have failed but didn't.")
    except Exception:
        # This exception is required to be raised to validate the step
        pass


@then(r'I verify over gRPC that the backup "{backup_name}" has status "{backup_status}"')
def _i_verify_over_grpc_that_backup_has_status(context, backup_name, backup_status):
    status = asyncio.get_event_loop().run_until_complete(context.grpc_client.get_backup_status(name=backup_name))
    assert status == medusa_pb2.StatusType.SUCCESS


@then(r'I verify over gRPC that the backup "{backup_name}" exists and is of type "{backup_type}"')
def _i_verify_over_grpc_backup_exists(context, backup_name, backup_type):
    backup = asyncio.get_event_loop().run_until_complete(context.grpc_client.get_backup(backup_name=backup_name))
    assert backup.backupName == backup_name
    assert backup.backupType == backup_type
    assert backup.totalSize > 0
    assert backup.totalObjects > 0


@then(r'I sleep for {num_secs} seconds')
def _i_sleep_for_seconds(context, num_secs):
    time.sleep(int(num_secs))


@then(r'I verify over gRPC that the backup "{backup_name}" has expected status IN_PROGRESS')
def _i_verify_over_grpc_backup_has_status_in_progress(context, backup_name):
    status = asyncio.get_event_loop().run_until_complete(context.grpc_client.get_backup_status(backup_name))
    assert status == medusa_pb2.StatusType.IN_PROGRESS


@then(r'I verify over gRPC that the backup "{backup_name}" has expected status UNKNOWN')
def _i_verify_over_grpc_backup_has_status_unknown(context, backup_name):
    status = asyncio.get_event_loop().run_until_complete(context.grpc_client.get_backup_status(backup_name))
    assert status == medusa_pb2.StatusType.UNKNOWN


@then(r'I verify over gRPC that the backup "{backup_name}" has expected status SUCCESS')
def _i_verify_over_grpc_backup_has_status_success(context, backup_name):
    status = asyncio.get_event_loop().run_until_complete(context.grpc_client.get_backup_status(backup_name))
    logging.info(f'status={status}')
    assert status == medusa_pb2.StatusType.SUCCESS


@then(r'I verify over gRPC that I can see both backups "{backup_name_1}" and "{backup_name_2}"')
def _i_verify_over_grpc_that_i_can_see_both_backups(context, backup_name_1, backup_name_2):
    backups = asyncio.get_event_loop().run_until_complete(context.grpc_client.get_backups())
    assert len(backups) == 2

    assert backups[0].backupName == backup_name_1
    assert backups[0].nodes[0].host == "127.0.0.1"
    assert backups[0].totalNodes == 1
    assert backups[0].finishedNodes == 1
    assert backups[0].status == 1

    assert backups[1].backupName == backup_name_2


@then(r'I verify over gRPC that the backup "{backup_name}" has the expected placement information')
def _i_verify_over_grpc_backup_has_expected_information(context, backup_name):
    backup = asyncio.get_event_loop().run_until_complete(context.grpc_client.get_backup(backup_name))
    assert backup.nodes[0].host == "127.0.0.1"
    assert backup.nodes[0].datacenter in ["dc1", "datacenter1", "DC1"]
    assert backup.nodes[0].rack in ["rack1", "r1"]
    assert len(backup.nodes[0].tokens) >= 1


@then(r'I delete the backup "{backup_name}" over gRPC')
def _i_delete_backup_grpc(context, backup_name):
    asyncio.get_event_loop().run_until_complete(context.grpc_client.delete_backup(backup_name))


@then(r'I delete the backup "{backup_name}" over gRPC and it fails')
def _i_delete_backup_grpc_fail(context, backup_name):
    try:
        asyncio.get_event_loop().run_until_complete(context.grpc_client.delete_backup(backup_name))
        raise AssertionError("Backup deletion should have failed but didn't.")
    except Exception:
        # This exception is required to be raised to validate the step
        pass


@then(r'I verify over gRPC that the backup "{backup_name}" does not exist')
def _i_verify_over_grpc_backup_does_not_exist(context, backup_name):
    assert not asyncio.get_event_loop().run_until_complete(context.grpc_client.backup_exists(backup_name))


@then(r'I verify that backup manager has removed the backup "{backup_name}"')
def _i_verify_backup_manager_removed_backup(context, backup_name):
    try:
        BackupMan.get_backup_status(backup_name)
    except RuntimeError:
        pass


@then(r'the gRPC server is up')
def _check_grpc_server_is_up(context):
    resp = asyncio.get_event_loop().run_until_complete(context.grpc_client.health_check())
    assert resp.status == 1


@then(r'I shutdown the gRPC server')
def _i_shutdown_the_grpc_server(context):
    context.grpc_server.destroy()


@then(r'I shutdown the mgmt api server')
def _i_shutdown_the_mgmt_api_server(context):
    context.mgmt_api_server.stop()
    context.mgmt_api_server.destroy()


@then(r'I can see the backup named "{backup_name}" when I list the backups')
def _i_can_see_the_backup_named_backupname_when_i_list_the_backups(
        context, backup_name
):
    with Storage(config=context.medusa_config.storage) as storage:
        cluster_backups = storage.list_cluster_backups()
        found = False
        for backup in cluster_backups:
            if backup.name == backup_name:
                found = True

        assert found is True


@then(r'some files from the previous backup were not reuploaded')
def _some_files_from_the_previous_backup_were_not_reuploaded(context):
    assert context.latest_num_kept > 0


@then(r'some files from the previous backup "{outcome}" replaced')
def _some_files_from_the_previous_backup_were_replaced(context, outcome):
    if outcome == "were":
        assert context.latest_backup_replaced > 0
    if outcome == "were not":
        assert context.latest_backup_replaced == 0


@then(r'I cannot see the backup named "{backup_name}" when I list the backups')
def _i_cannot_see_the_backup_named_backupname_when_i_list_the_backups(
        context, backup_name
):
    with Storage(config=context.medusa_config.storage) as storage:
        cluster_backups = storage.list_cluster_backups()
        found = False
        for backup in cluster_backups:
            if backup.name == backup_name:
                found = True

        assert found is False


@then('I can {can_see_purged} see purged backup files for the "{table_name}" table in keyspace "{keyspace}"')
def _i_can_see_purged_backup_files_for_the_tablename_table_in_keyspace_keyspacename(
        context, can_see_purged, table_name, keyspace
):
    with Storage(config=context.medusa_config.storage) as storage:
        path = os.path.join(
            storage.prefix_path + context.medusa_config.storage.fqdn, "data", keyspace, table_name
        )
        sb_files = len(storage.storage_driver.list_objects(path))

        node_backups = storage.list_node_backups()
        # Parse its manifest
        nb_list = list(node_backups)
        nb_files = {}
        for nb in nb_list:
            manifest = json.loads(nb.manifest)
            for section in manifest:
                if (
                        section["keyspace"] == keyspace
                        and section["columnfamily"][: len(table_name)] == table_name
                ):
                    for objects in section["objects"]:
                        nb_files[objects["path"]] = 0
        if can_see_purged == "not":
            assert sb_files == len(nb_files)
        else:
            # GC grace is activated and we expect more files in the storage bucket than in the manifests
            assert sb_files > len(nb_files)


@then('I can see the backup status for "{backup_name}" when I run the status command')
def _i_can_see_backup_status_when_i_run_the_status_command(context, backup_name):
    medusa.status.status(config=context.medusa_config, backup_name=backup_name)


@then(r"I can see no backups when I list the backups")
def _i_can_see_no_backups(context):
    with Storage(config=context.medusa_config.storage) as storage:
        cluster_backups = storage.list_cluster_backups()
        assert 0 == len(list(cluster_backups))


@then(
    r'the backup named "{backup_name}" has {nb_sstables} SSTables '
    + r'for the "{table_name}" table in keyspace "{keyspace}"'
)
def _the_backup_named_backupname_has_nb_sstables_for_the_whatever_table(
        context, backup_name, nb_sstables, table_name, keyspace
):
    with Storage(config=context.medusa_config.storage) as storage:
        path = os.path.join(
            storage.prefix_path + context.medusa_config.storage.fqdn, backup_name, "data", keyspace, table_name
        )
        objects = storage.storage_driver.list_objects(path)
        sstables = list(filter(lambda obj: "-Data.db" in obj.name, objects))
        if len(sstables) != int(nb_sstables):
            logging.error("{} SSTables : {}".format(len(sstables), sstables))
            logging.error("Was expecting {} SSTables".format(nb_sstables))
            assert len(sstables) == int(nb_sstables)


@then(r'I can verify the backup named "{backup_name}" with md5 checks "{md5_enabled_str}" successfully')
def _i_can_verify_the_backup_named_successfully(context, backup_name, md5_enabled_str):
    medusa.verify.verify(context.medusa_config, backup_name, str(md5_enabled_str).lower() == "enabled")


@then(r'I can download the backup named "{backup_name}" for all tables')
def _i_can_download_the_backup_all_tables_successfully(context, backup_name):
    def cleanup(temp_path):
        if os.path.exists(temp_path) and os.path.isdir(temp_path):
            shutil.rmtree(temp_path)

    with Storage(config=context.medusa_config.storage) as storage:
        config = context.medusa_config
        download_path = os.path.join("/tmp", "medusa-download-all-tables/")
        cleanup(download_path)
        os.makedirs(download_path)

        backup = storage.get_node_backup(
            fqdn=config.storage.fqdn,
            name=backup_name,
        )
        keyspaces = set()
        tables = set()
        medusa.download.download_cmd(context.medusa_config, backup_name, Path(download_path), keyspaces, tables, False)

        # check all manifest objects that have been backed up have been downloaded
        keyspaces = {section['keyspace'] for section in json.loads(backup.manifest) if section['objects']}
        for ks in keyspaces:
            ks_path = os.path.join(download_path, ks)
            assert os.path.isdir(ks_path)

        cleanup(download_path)


@then(r'I can download the backup named "{backup_name}" for "{fqtn}"')
def _i_can_download_the_backup_single_table_successfully(context, backup_name, fqtn):
    def cleanup(temp_path):
        if os.path.exists(temp_path) and os.path.isdir(temp_path):
            shutil.rmtree(temp_path)

    with Storage(config=context.medusa_config.storage) as storage:
        config = context.medusa_config
        download_path = os.path.join("/tmp", "medusa-download-one-table/")
        cleanup(download_path)
        os.makedirs(download_path)

        backup = storage.get_node_backup(
            fqdn=config.storage.fqdn,
            name=backup_name,
        )

        # download_data requires fqtn with table id
        fqtns_to_download, _ = medusa.filtering.filter_fqtns([], [fqtn], backup.manifest, True)
        medusa.download.download_data(context.medusa_config.storage, backup, fqtns_to_download, Path(download_path))

        # check the keyspace directory has been created
        ks, table = fqtn.split('.')
        ks_path = os.path.join(download_path, ks)
        assert os.path.isdir(ks_path)

        # check tables have been downloaded
        assert list(Path(ks_path).glob('{}-*/*.db'.format(table)))
        cleanup(download_path)


@then(r'Test TLS version connections if "{client_encryption}" is turned on')
def _i_can_connect_using_all_tls_versions(context, client_encryption):
    if client_encryption == 'with_client_encryption':
        for tls_version in [PROTOCOL_TLS, PROTOCOL_TLSv1_2]:
            connect_cassandra(True, tls_version)


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


@then(r'I have {nb_rows} rows in the "{table_name}" table in ccm cluster "{client_encryption}"')
def _i_have_rows_in_the_table(context, nb_rows, table_name, client_encryption):
    is_client_encryption_enable = False
    if client_encryption == 'with_client_encryption':
        is_client_encryption_enable = True
    context.session = connect_cassandra(is_client_encryption_enable)
    rows = context.session.execute("select count(*) as nb from {}".format(table_name))
    assert rows[0][0] == int(nb_rows)


@then(r'I can see the backup index entry for "{backup_name}"')
def _the_backup_named_backupname_is_present_in_the_index(context, backup_name):
    with Storage(config=context.medusa_config.storage) as storage:
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
        assert (tokenmap_from_backup == tokenmap_from_index and manifest_from_backup == manifest_from_index)


@then(
    r'I can see the latest backup for "{expected_fqdn}" being called "{expected_backup_name}"'
)
def _the_latest_backup_for_fqdn_is_called_backupname(
        context, expected_fqdn, expected_backup_name
):
    with Storage(config=context.medusa_config.storage) as storage:
        latest_backup = storage.latest_node_backup(fqdn=expected_fqdn)
        assert latest_backup.name == expected_backup_name


@then(r'there is no latest backup for node "{fqdn}"')
def _there_is_no_latest_backup_for_node_fqdn(context, fqdn):
    with Storage(config=context.medusa_config.storage) as storage:
        node_backup = storage.latest_node_backup(fqdn=fqdn)
        logging.info("Latest node backup is {}".format(
            node_backup.name if node_backup is not None else "None"))
        assert node_backup is None


@when(
    r'node "{fqdn}" fakes a complete backup named "{backup_name}" on "{backup_datetime}"'
)
def _node_fakes_a_complete_backup(context, fqdn, backup_name, backup_datetime):
    with Storage(config=context.medusa_config.storage) as storage:
        path_root = BUCKET_ROOT

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
    with Storage(config=context.medusa_config.storage) as storage:
        backup = storage.latest_cluster_backup()
        assert expected_backup_name == backup.name


@then(r'listing backups for node "{fqdn}" returns {nb_backups} backups')
def _listing_backups_for_node_returns(context, fqdn, nb_backups):
    with Storage(config=context.medusa_config.storage) as storage:
        backup_index = storage.list_backup_index_blobs()
        backups = list(storage.list_node_backups(fqdn=fqdn, backup_index_blobs=backup_index))
        assert int(nb_backups) == len(backups)


@then(r"there is no latest complete backup")
def _there_is_no_latest_complete_backup(context):
    with Storage(config=context.medusa_config.storage) as storage:
        actual_backup = storage.latest_complete_cluster_backup()
        assert actual_backup is None


@then(r"I can list and print backups without errors")
def _can_list_print_backups_without_error(context):
    medusa.listing.list_backups(config=context.medusa_config, show_all=True)


@then(r'the latest complete cluster backup is "{expected_backup_name}"')
def _the_latest_complete_cluster_backup_is(context, expected_backup_name):
    with Storage(config=context.medusa_config.storage) as storage:
        actual_backup = storage.latest_complete_cluster_backup()
        if actual_backup is not None:
            assert expected_backup_name == actual_backup.name


@when(r"I truncate the backup index")
def _truncate_the_index(context):
    with Storage(config=context.medusa_config.storage) as storage:
        path_root = BUCKET_ROOT
        index_path = "{}/{}index".format(path_root, storage.prefix_path)
        shutil.rmtree(index_path)


@when(r"I truncate the backup folder")
def _truncate_the_backup_folder(context):
    with Storage(config=context.medusa_config.storage) as storage:
        path_root = BUCKET_ROOT
        backup_path = "{}/{}127.0.0.1".format(path_root, storage.prefix_path)
        shutil.rmtree(backup_path)


@when(r"I re-create the backup index")
def _recreate_the_index(context):
    medusa.index.build_indices(context.medusa_config, False)


@then(r"I can report latest backups without errors")
def _can_report_backups_without_errors(context):
    medusa.report_latest.report_latest(config=context.medusa_config, push_metrics=True)


@then(r"the backup index does not exist")
def _the_backup_index_does_not_exist(context):
    with Storage(config=context.medusa_config.storage) as storage:
        assert False is medusa.index.index_exists(storage)


@then(r"the backup index exists")
def _the_backup_index_exists(context):
    with Storage(config=context.medusa_config.storage) as storage:
        assert True is medusa.index.index_exists(storage)


@then(
    r'I can see {nb_sstables} SSTables in the SSTable pool for the "{table_name}" table in keyspace "{keyspace}"'
)
def _i_can_see_nb_sstables_in_the_sstable_pool(
        context, nb_sstables, table_name, keyspace
):
    with Storage(config=context.medusa_config.storage) as storage:
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
    with Storage(config=context.medusa_config.storage) as storage:
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
                    logging.error(
                        "Files in the manifest: {}".format(section["objects"])
                    )
                    assert len(section["objects"]) == int(nb_files)


@then(r'I can see secondary index files in the "{backup_name}" files')
def _i_can_see_secondary_index_files_in_backup(context, backup_name):
    with Storage(config=context.medusa_config.storage) as storage:
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
        medusa.verify.verify(context.medusa_config, backup_name, True)  # enable hash comparison
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


@when(r'I truncate the "{table_name}" table in ccm cluster "{client_encryption}"')
def _i_truncate_the_table(context, table_name, client_encryption):
    is_client_encryption_enable = False

    if client_encryption == 'with_client_encryption':
        is_client_encryption_enable = True
    context.session = connect_cassandra(is_client_encryption_enable)
    context.session.execute("truncate {}".format(table_name))


@then(r'I can verify the restore verify query "{query}" returned {expected_rows} rows')
def _i_can_verify_the_restore_verify_query_returned_rows(context, query, expected_rows):
    restore_config = {
        "health_check": "cql",
        "query": query,
        "expected_rows": expected_rows,
    }
    custom_config = MedusaConfig(
        file_path=None,
        storage=context.medusa_config.storage,
        cassandra=context.medusa_config.cassandra,
        monitoring=context.medusa_config.monitoring,
        checks=_namedtuple_from_dict(ChecksConfig, restore_config),
        ssh=None,
        logging=None,
        grpc=None,
        kubernetes=None,
    )
    medusa.verify_restore.verify_restore(["127.0.0.1"], custom_config)


@then(r'I delete the manifest from the backup named "{backup_name}"')
def _i_delete_the_manifest_from_the_backup_named(context, backup_name):
    with Storage(config=context.medusa_config.storage) as storage:
        path_root = BUCKET_ROOT

        fqdn = "127.0.0.1"
        path_manifest_index_latest = "{}/{}index/backup_index/{}/manifest_{}.json".format(
            path_root, storage.prefix_path, backup_name, fqdn
        )
        path_backup_index = "{}/{}index/backup_index/{}".format(
            path_root, storage.prefix_path, backup_name
        )
        path_manifest_backup = "{}/{}{}/{}/meta/manifest.json".format(
            path_root, storage.prefix_path, fqdn, backup_name, fqdn
        )

        os.remove(path_manifest_backup)
        os.remove(path_manifest_index_latest)

        meta_files = os.listdir(path_backup_index)
        for meta_file in meta_files:
            if meta_file.startswith("finished"):
                os.remove(os.path.join(path_backup_index, meta_file))


@then(r'I delete the manifest from the backup named "{backup_name}" from the storage')
def _i_delete_the_manifest_from_the_backup_named_from_the_storage(context, backup_name):
    with Storage(config=context.medusa_config.storage) as storage:

        fqdn = context.medusa_config.storage.fqdn
        path_manifest_index_latest = "{}index/backup_index/{}/manifest_{}.json".format(
            storage.prefix_path, backup_name, fqdn
        )
        path_backup_index = "{}index/backup_index/{}".format(
            storage.prefix_path, backup_name
        )
        path_manifest_backup = "{}{}/{}/meta/manifest.json".format(
            storage.prefix_path, fqdn, backup_name
        )

        blob = storage.storage_driver.get_blob(path_manifest_backup)
        storage.storage_driver.delete_object(blob)
        blob = storage.storage_driver.get_blob(path_manifest_index_latest)
        storage.storage_driver.delete_object(blob)

        meta_files = storage.storage_driver.list_objects(path_backup_index)
        for meta_file in meta_files:
            if meta_file.name.split("/")[-1].startswith("finished"):
                storage.storage_driver.delete_object(meta_file)


@then(r'the backup named "{backup_name}" is incomplete')
def _the_backup_named_is_incomplete(context, backup_name):
    with Storage(config=context.medusa_config.storage) as storage:
        backups = medusa.listing.list_backups_w_storage(config=context.medusa_config, show_all=True, storage=storage)
        for backup in backups:
            if backup.name == backup_name:
                assert not backup.finished


@then(u'I "{operation}" a random sstable from "{backup_type}" backup "{backup_name}" '
      u'in the "{table}" table in keyspace "{keyspace}"')
def _i_manipulate_a_random_sstable(context, operation, backup_type, backup_name, table, keyspace):

    with Storage(config=context.medusa_config.storage) as storage:
        path_root = BUCKET_ROOT

        fqdn = "127.0.0.1"
        if backup_type == "full":
            path_sstables = "{}/{}{}/{}/data/{}/{}*".format(
                path_root, storage.prefix_path, fqdn, backup_name, keyspace, table
            )
        else:
            path_sstables = "{}/{}{}/data/{}/{}*".format(
                path_root, storage.prefix_path, fqdn, keyspace, table
            )

        table_path = glob.glob(path_sstables)[0]
        sstable_files = os.listdir(table_path)
        # Exclude Statistics.db files from sstables_files as they will be ignored by verify.
        # Exclude the folder with Cassandra's secondary indexes as well.
        sstable_files = [x for x in sstable_files if ('-Statistics.db' not in x) and ('idx') not in x]
        random.shuffle(sstable_files)

        file_path = Path(os.path.join(table_path, sstable_files[0]))
        if operation == "delete":
            os.remove(file_path)
        if operation == "truncate":
            os.remove(file_path)
            file_path.touch()


@then(r'verifying backup "{backup_name}" fails')
def _verifying_backup_fails(context, backup_name):
    try:
        medusa.verify.verify(context.medusa_config, backup_name, True)
        assert "Verify should have failed" == "Well it didn't"
    except RuntimeError:
        # All good, we should get this exception
        pass


@when(r'I delete the backup named "{backup_name}"')
def _i_delete_the_backup_named(context, backup_name, all_nodes=False):
    medusa.purge.delete_backup(context.medusa_config,
                               backup_names=[backup_name], all_nodes=all_nodes)


@then(r'I can fetch the tokenmap of the backup named "{backup_name}"')
def _i_can_fecth_tokenmap_of_backup_named(context, backup_name):
    tokenmap = medusa.fetch_tokenmap.main(backup_name=backup_name, config=context.medusa_config)
    assert "127.0.0.1" in tokenmap


@then(r'the schema of the backup named "{backup_name}" was uploaded with KMS key according to "{storage_provider}"')
def _the_schema_was_uploaded_with_kms_key_according_to_storage(context, backup_name, storage_provider):

    # testing server-side encryption is not irrelevant when running with local storage
    if storage_provider == 'local':
        return

    # we're testing the schema blob, because that one is written from a string
    # which is a different code path than actual SSTables

    # initialise storage with (or without) KMS according to the config
    context.storage_provider = storage_provider
    context.client_encryption = 'without_client_encryption'
    context.medusa_config = get_medusa_config(context, storage_provider, context.client_encryption, None)

    with Storage(config=context.medusa_config.storage) as storage:
        # pick a sample blob, in this case the schema one
        index_bobs = storage.list_backup_index_blobs()
        blobs_by_backup = storage.group_backup_index_by_backup_and_node(index_bobs)
        schema_blob = storage.lookup_blob(blobs_by_backup, backup_name, context.medusa_config.storage.fqdn, 'schema')
        schema_blob_metadata = storage.storage_driver.get_blob_metadata(schema_blob.name)

        # assert situation when the server-side encryption is not enabled
        if context.medusa_config.storage.kms_id is not None:
            # assert the kms is enabled for the blob
            assert True is schema_blob_metadata.sse_enabled
            # assert the KMS key is present and that it equals the one from the config
            assert None is not schema_blob_metadata.sse_key_id
            assert context.medusa_config.storage.kms_id == schema_blob_metadata.sse_key_id
        else:
            # if the KMS is not enabled, we check for SSE being disabled and the key being absent
            assert False is schema_blob_metadata.sse_enabled
            assert None is schema_blob_metadata.sse_key_id


@then(
    r'all files of "{fqtn}" in "{backup_name}" were uploaded with KMS key as configured in "{storage_provider}"'
)
def _all_files_of_table_in_backup_were_uploaded_with_key_configured_in_storage_config(
        context, fqtn, backup_name, storage_provider
):
    # testing server-side encryption is not irrelevant when running with local storage
    if storage_provider == 'local':
        return

    # in this step we're testing the code path that uploads actual files (not just stuff written directly)

    # initialise storage with (or without) KMS according to the config
    context.storage_provider = storage_provider
    context.client_encryption = 'without_client_encryption'
    context.medusa_config = get_medusa_config(context, storage_provider, context.client_encryption, None)

    with Storage(config=context.medusa_config.storage) as storage:

        node_backup = storage.get_node_backup(fqdn=context.medusa_config.storage.fqdn, name=backup_name)
        manifest = json.loads(node_backup.manifest)
        for section in manifest:

            if fqtn != "{}.{}".format(section['keyspace'], section['columnfamily']):
                continue

            srcs = [
                '{}{}'.format(storage.storage_driver.get_path_prefix(node_backup.data_path), obj['path'])
                for obj in section['objects']
            ]
            for blob_metadata in storage.storage_driver.get_blobs_metadata(srcs):
                # assert situation when the server-side encryption is not enabled
                if context.medusa_config.storage.kms_id is not None:
                    # assert the kms is enabled for the blob
                    assert True is blob_metadata.sse_enabled
                    # assert the KMS key is present and that it equals the one from the config
                    assert None is not blob_metadata.sse_key_id
                    assert context.medusa_config.storage.kms_id is blob_metadata.sse_key_id
                else:
                    # if the KMS is not enabled, we check for SSE being disabled and the key being absent
                    assert False is blob_metadata.sse_enabled
                    assert None is blob_metadata.sse_key_id


@when(r'I perform a purge over gRPC')
def _i_perform_a_purge_over_grpc_with_a_max_backup_count(context):
    context.purge_result = asyncio.get_event_loop().run_until_complete((context.grpc_client.purge_backups()))


@then(r'{nb_purged_backups} backup has been purged')
def _backup_has_been_purged(context, nb_purged_backups):
    assert context.purge_result.nbBackupsPurged == int(nb_purged_backups)


@then(r'I wait for {pause_duration} seconds')
def _i_wait_for_seconds(context, pause_duration):
    time.sleep(int(pause_duration))


@then(r'I modify Statistics.db file in the backup in the "{table}" table in keyspace "{keyspace}"')
def _i_modify_a_statistics_db_file(context, table, keyspace):
    with Storage(config=context.medusa_config.storage) as storage:
        path_root = BUCKET_ROOT

        fqdn = "127.0.0.1"
        path_sstables = "{}/{}{}/data/{}/{}*".format(
            path_root, storage.prefix_path, fqdn, keyspace, table
        )
        table_path = glob.glob(path_sstables)[0]
        sstable_files = os.listdir(table_path)
        statistics_db_files = [file for file in sstable_files if '-Statistics.db' in file]
        path_statistics_db_file = os.path.join(table_path, statistics_db_files[0])
        with open(path_statistics_db_file, 'a') as file:
            file.write('Adding some additional characters')


@then(r'checking the list of decommissioned nodes returns "{expected_node}"')
def _checking_list_of_decommissioned_nodes(context, expected_node):
    # Get all nodes having backups
    with Storage(config=context.medusa_config.storage) as storage:
        blobs = storage.list_root_blobs()
        all_nodes = medusa.purge_decommissioned.get_all_nodes(blobs)

        # Get live nodes
        live_nodes = medusa.purge_decommissioned.get_live_nodes(context.medusa_config)

        # Get decommissioned nodes
        decommissioned_nodes = medusa.purge_decommissioned.get_decommissioned_nodes(all_nodes, live_nodes)

        assert expected_node in decommissioned_nodes


@when(r'I run a purge on decommissioned nodes')
def _run_purge_on_decommissioned_nodes(context):
    try:
        medusa.purge_decommissioned.main(context.medusa_config)

    except Exception as e:
        logging.error('This error happened during the purge of decommissioned nodes: {}'.format(str(e)))
        raise e


@when(r'I write "{file_cnt}" files to storage')
def _i_write_count_files_to_storage(context, file_cnt):
    key_content_pairs = [
        (f'{context.medusa_config.storage.prefix}/key{i}', 'some content')
        for i in range(int(file_cnt))
    ]
    with Storage(config=context.medusa_config.storage) as storage:
        # upload the blobs all in parallel, disregarding the parallelism in medusa's config
        # this saves about 2 minutes of the test run time
        storage.storage_driver.upload_blobs_from_strings(key_content_pairs, concurrent_transfers=len(key_content_pairs))


@then(r'I can list all "{file_cnt}" files in the storage')
def _i_can_list_all_count_files_in_storage(context, file_cnt):
    with Storage(config=context.medusa_config.storage) as storage:
        root_blobs = storage.list_root_blobs()
        logging.info(f"Listed {len(root_blobs)} blobs")
        context.blobs_to_delete = root_blobs
        assert int(file_cnt) == len(root_blobs)


@then(u'I clean up the files')
def _i_clean_up_the_files(context):
    with Storage(config=context.medusa_config.storage) as storage:
        # save another minute by deleting stuff all at once
        storage.delete_objects(context.blobs_to_delete, concurrent_transfers=len(context.blobs_to_delete))
        assert 0 == len(storage.list_root_blobs())


@then(u'the backup "{backup_name}" has server_type "{server_type}" in its metadata')
def _backup_has_server_type_and_release_version(context, backup_name, server_type):
    with Storage(config=context.medusa_config.storage) as storage:
        node_backup = storage.get_node_backup(fqdn=context.medusa_config.storage.fqdn, name=backup_name)
        assert server_type == node_backup.server_type
        # not asserting for release_version because it's hard to get the Cassandra's one


@then(u'I can tell a backup "{is_in_progress}" in progress')
def _backup_is_or_is_not_in_progress(context, is_in_progress):
    marker_file_path = MedusaTempFile().get_path()
    if 'is' == is_in_progress:
        assert Path(marker_file_path).exists()
    if 'is not' == is_in_progress:
        assert not Path(marker_file_path).exists()


def connect_cassandra(is_client_encryption_enable, tls_version=PROTOCOL_TLS):
    connected = False
    attempt = 0
    session = None
    _ssl_context = None

    if is_client_encryption_enable:
        ssl_context = SSLContext(tls_version)
        ssl_context.load_verify_locations(certfile)
        ssl_context.verify_mode = CERT_REQUIRED
        ssl_context.load_cert_chain(
            certfile=usercert,
            keyfile=userkey)
        _ssl_context = ssl_context

    while not connected and attempt < 10:
        try:
            cluster = Cluster(contact_points=["127.0.0.1"],
                              ssl_context=_ssl_context,
                              protocol_version=ProtocolVersion.V4)
            session = cluster.connect()
            connected = True
        except cassandra.cluster.NoHostAvailable:
            attempt += 1
            if attempt >= 10:
                raise
            time.sleep(10)

    if tls_version is not PROTOCOL_TLS:  # other TLS versions used for testing, close the session
        session.shutdown()

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


def get_mgmt_api_jars(
        version,
        url="https://api.github.com/repos/datastax/management-api-for-apache-cassandra/releases/latest"):
    # clear out any temp resources that might exist
    remove_temporary_mgmtapi_resources()

    result = requests.get(url=url)

    zip_file = requests.get(json.loads(result.text)["assets"][0]["browser_download_url"], stream=True)

    with open("/tmp/mgmt_api_jars.zip", "wb") as mgmt_api_jars:
        for chunk in zip_file.iter_content(chunk_size=4096):
            if chunk:
                mgmt_api_jars.write(chunk)

    with ZipFile('/tmp/mgmt_api_jars.zip', 'r') as zip_ref:
        list_of_names = zip_ref.namelist()
        for file_name in list_of_names:
            if file_name.endswith('.jar'):
                if 'mgmtapi-agent' in file_name or 'mgmtapi-server' in file_name:
                    zip_ref.extract(file_name, '/tmp')

    symlink_mgmt_api_jar(version)


def symlink_mgmt_api_jar(version):
    management_api_jar_path = '/tmp/management-api-agent/target/datastax-mgmtapi-agent-0.1.0-SNAPSHOT.jar'
    if not Path(management_api_jar_path).is_file():
        # the bundle has split agents (maybe), one for C* 3.x and one for C* 4.x
        # link the C* specific agent to the generic agent file name
        assert False is Path('/tmp/management-api-agent/target/').exists()
        Path('/tmp/management-api-agent/target/').mkdir(parents=True, exist_ok=False)
        if str.startswith(version, '3'):
            # C* is version 3.x, use 3.x agent
            assert Path('/tmp/management-api-agent-3.x/target/datastax-mgmtapi-agent-3.x-0.1.0-SNAPSHOT.jar').is_file()
            Path(management_api_jar_path).symlink_to(
                '/tmp/management-api-agent-3.x/target/datastax-mgmtapi-agent-3.x-0.1.0-SNAPSHOT.jar')
        elif str.startswith(version, '4') or 'github:apache/trunk' == version:
            # C* is version 4.x, use 4.x agent
            assert Path('/tmp/management-api-agent-4.x/target/datastax-mgmtapi-agent-4.x-0.1.0-SNAPSHOT.jar').is_file()
            Path(management_api_jar_path).symlink_to(
                '/tmp/management-api-agent-4.x/target/datastax-mgmtapi-agent-4.x-0.1.0-SNAPSHOT.jar')
        else:
            raise NotImplementedError('Cassandra version not supported: {}'.format(version))


def rm_tree(pth):
    pth = Path(pth)
    for child in pth.glob('*'):
        if child.is_file():
            child.unlink()
        else:
            rm_tree(child)
    pth.rmdir()


def remove_temporary_mgmtapi_resources():
    for tempDir in Path('/tmp').glob('management-api-*'):
        rm_tree(tempDir)
