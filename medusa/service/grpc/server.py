# -*- coding: utf-8 -*-
# Copyright 2020- Datastax, Inc. All rights reserved.
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

import logging
import signal
import sys
from collections import defaultdict
from concurrent import futures
from datetime import datetime
from pathlib import Path

import grpc
import grpc_health.v1.health
from grpc_health.v1 import health_pb2_grpc

import medusa.backup_node
import medusa.config
import medusa.listing
import medusa.purge
from medusa.service.grpc import medusa_pb2
from medusa.service.grpc import medusa_pb2_grpc
from medusa.storage import Storage

TIMESTAMP_FORMAT = '%Y-%m-%d %H:%M:%S'


class MedusaService(medusa_pb2_grpc.MedusaServicer):

    def __init__(self, config):
        logging.info("Init service")
        self.config = config
        self.storage = Storage(config=self.config.storage)

    def Backup(self, request, context):
        logging.info("Performing backup {}".format(request.name))
        resp = medusa_pb2.BackupResponse()
        # TODO pass the staggered and mode args
        try:
            medusa.backup_node.main(self.config, request.name, None, False, "differential")
        except Exception as e:
            context.set_details("failed to create backups: {}".format(e))
            context.set_code(grpc.StatusCode.INTERNAL)
            logging.exception("backup failed")

        return resp

    def BackupStatus(self, request, context):
        response = medusa_pb2.BackupStatusResponse()
        try:
            backup = self.storage.get_cluster_backup(request.backupName)

            # TODO how is the startTime determined?
            response.startTime = datetime.fromtimestamp(backup.started).strftime(TIMESTAMP_FORMAT)
            response.finishedNodes = [node.fqdn for node in backup.complete_nodes()]
            response.unfinishedNodes = [node.fqdn for node in backup.incomplete_nodes()]
            response.missingNodes = [node.fqdn for node in backup.missing_nodes()]

            if backup.finished:
                response.finishTime = datetime.fromtimestamp(backup.finished).strftime(TIMESTAMP_FORMAT)
            else:
                response.finishTime = ""

            return response
        except KeyError:
            context.set_details("backup <{}> does not exist".format(request.backupName))
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return response

    def GetBackups(self, request, context):
        response = medusa_pb2.GetBackupsResponse()
        try:
            backups = medusa.listing.get_backups(self.config, True)
            for backup in backups:
                summary = medusa_pb2.BackupSummary()
                summary.backupName = backup.name
                if backup.started is None:
                    summary.starTime = 0
                else:
                    summary.startTime = backup.started
                if backup.finished is None:
                    summary.finishTime = 0
                else:
                    summary.finishTime = backup.finished
                summary.totalNodes = len(backup.tokenmap)
                summary.finishedNodes = len(backup.complete_nodes())
                response.backups.append(summary)

            return response
        except Exception as e:
            context.set_details("failed to get backups: {}".format(e))
            context.set_code(grpc.StatusCode.INTERNAL)
            return response

    def DeleteBackup(self, request, context):
        logging.info("Deleting backup {}".format(request.name))
        resp = medusa_pb2.DeleteBackupResponse()
        try:
            medusa.purge.delete_backup(self.config, request.name, True)
        except Exception as e:
            context.set_details("deleting backups failed: {}".format(e))
            context.set_code(grpc.StatusCode.INTERNAL)
            logging.exception("Deleting backup {} failed".format(request.name))

        return resp


def create_config(config_file_path):
    config_file = Path(config_file_path)
    args = defaultdict(lambda: None)

    return medusa.config.load_config(args, config_file)


def configure_console_logging(config):
    root_logger = logging.getLogger('')
    root_logger.setLevel(logging.DEBUG)

    log_format = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s')

    console_handler = logging.StreamHandler()
    console_handler.setLevel(getattr(logging, config.level))
    console_handler.setFormatter(log_format)
    root_logger.addHandler(console_handler)

    if console_handler.level > logging.DEBUG:
        # Disable debugging logging for external libraries
        for logger_name in 'urllib3', 'google_cloud_storage.auth.transport.requests', 'paramiko', 'cassandra':
            logging.getLogger(logger_name).setLevel(logging.WARN)


def shutdown(signum, frame):
    logging.info("shutting down")
    server.stop(0)


if len(sys.argv) > 2:
    config_file_path = sys.argv[2]
else:
    config_file_path = "/etc/medusa/medusa.ini"

config = create_config(config_file_path)
configure_console_logging(config.logging)

server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

medusa_pb2_grpc.add_MedusaServicer_to_server(MedusaService(config), server)
health_pb2_grpc.add_HealthServicer_to_server(grpc_health.v1.health.HealthServicer(), server)

logging.info('Starting server. Listening on port 50051.')
server.add_insecure_port('[::]:50051')
server.start()

signal.signal(signal.SIGTERM, shutdown)

server.wait_for_termination()
