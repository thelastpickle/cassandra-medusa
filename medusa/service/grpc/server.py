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

import asyncio
import json
import logging
import os
import sys
from collections import defaultdict
from concurrent import futures
from concurrent.futures.thread import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

import grpc
import grpc_health.v1.health
from grpc import aio
from grpc_health.v1 import health_pb2_grpc

from medusa import backup_node
from medusa import purge
from medusa.backup_manager import BackupMan
from medusa.config import load_config
from medusa.listing import get_backups
from medusa.purge import delete_backup
from medusa.restore_cluster import RestoreJob
from medusa.service.grpc import medusa_pb2
from medusa.service.grpc import medusa_pb2_grpc
from medusa.storage import Storage

TIMESTAMP_FORMAT = '%Y-%m-%d %H:%M:%S'
BACKUP_MODE_DIFFERENTIAL = "differential"
BACKUP_MODE_FULL = "full"
RESTORE_MAPPING_LOCATION = "/var/lib/cassandra/.restore_mapping"
RESTORE_MAPPING_ENV = "RESTORE_MAPPING"


class Server:
    def __init__(self, config_file_path, testing=False):
        self.config_file_path = config_file_path
        self.medusa_config = self.create_config()
        self.testing = testing
        self.grpc_server = aio.server(futures.ThreadPoolExecutor(max_workers=10), options=[
            ('grpc.max_send_message_length', self.medusa_config.grpc.max_send_message_length),
            ('grpc.max_receive_message_length', self.medusa_config.grpc.max_receive_message_length)
        ])
        logging.info("GRPC server initialized")

    def shutdown(self, signum, frame):
        logging.info("Shutting down GRPC server")
        handle_backup_removal_all()
        asyncio.get_event_loop().run_until_complete(self.grpc_server.stop(0))

    async def serve(self):
        config = self.create_config()
        self.configure_console_logging()

        medusa_pb2_grpc.add_MedusaServicer_to_server(MedusaService(config), self.grpc_server)
        health_pb2_grpc.add_HealthServicer_to_server(grpc_health.v1.health.HealthServicer(), self.grpc_server)

        logging.info('Starting server. Listening on port 50051.')
        self.grpc_server.add_insecure_port('[::]:50051')
        await self.grpc_server.start()

        if not self.testing:
            try:
                await self.grpc_server.wait_for_termination()
            except asyncio.exceptions.CancelledError:
                logging.info("Swallowing asyncio.exceptions.CancelledError. This should get fixed at some point")
            handle_backup_removal_all()

    def create_config(self):
        config_file = Path(self.config_file_path)
        args = defaultdict(lambda: None)

        return load_config(args, config_file)

    def configure_console_logging(self):
        root_logger = logging.getLogger('')
        root_logger.setLevel(logging.DEBUG)

        # Clean up handlers on the root_logger, this prevents duplicate log lines
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)

        log_format = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s')

        console_handler = logging.StreamHandler()
        console_handler.setLevel(getattr(logging, self.medusa_config.logging.level))
        console_handler.setFormatter(log_format)
        root_logger.addHandler(console_handler)

        if console_handler.level > logging.DEBUG:
            # Disable debugging logging for external libraries
            for logger_name in 'urllib3', 'google_cloud_storage.auth.transport.requests', 'paramiko', 'cassandra':
                logging.getLogger(logger_name).setLevel(logging.WARN)


class MedusaService(medusa_pb2_grpc.MedusaServicer):

    def __init__(self, config):
        logging.info("Init service")
        self.config = config
        self.storage_config = config.storage

    async def AsyncBackup(self, request, context):
        # TODO pass the staggered arg
        logging.info("Performing ASYNC backup {} (type={})".format(request.name, request.mode))
        response = medusa_pb2.BackupResponse()
        mode = BACKUP_MODE_DIFFERENTIAL
        if medusa_pb2.BackupRequest.Mode.FULL == request.mode:
            mode = BACKUP_MODE_FULL

        try:
            response.backupName = request.name
            response.status = response.status = medusa_pb2.StatusType.IN_PROGRESS
            BackupMan.register_backup(request.name, is_async=True)
            executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix=request.name)
            loop = asyncio.get_running_loop()
            backup_future = loop.run_in_executor(
                executor,
                backup_node.handle_backup,
                self.config, request.name, None, False, mode
            )
            backup_future.add_done_callback(record_backup_info)
            BackupMan.set_backup_future(request.name, backup_future)

        except Exception as e:

            response.status = medusa_pb2.StatusType.FAILED
            if request.name:
                BackupMan.update_backup_status(request.name, BackupMan.STATUS_FAILED)

            context.set_details("Failed to create async backup: {}".format(e))
            context.set_code(grpc.StatusCode.INTERNAL)
            logging.exception("Async backup failed due to error: {}".format(e))

        return response

    def Backup(self, request, context):
        # TODO pass the staggered arg
        logging.info("Performing SYNC backup {} (type={})".format(request.name, request.mode))
        response = medusa_pb2.BackupResponse()
        mode = BACKUP_MODE_DIFFERENTIAL
        if medusa_pb2.BackupRequest.Mode.FULL == request.mode:
            mode = BACKUP_MODE_FULL

        try:
            response.backupName = request.name
            BackupMan.register_backup(request.name, is_async=False)
            backup_node.handle_backup(config=self.config, backup_name_arg=request.name, stagger_time=None,
                                      enable_md5_checks_flag=False, mode=mode)
            record_status_in_response(response, request.name)
            return response
        except Exception as e:
            response.status = medusa_pb2.StatusType.FAILED
            if request.name:
                BackupMan.update_backup_status(request.name, BackupMan.STATUS_FAILED)

            context.set_details("Failed to create sync backups: {}".format(e))
            context.set_code(grpc.StatusCode.INTERNAL)
            logging.exception("Sync backup failed due to error: {}".format(e))

        return response

    def BackupStatus(self, request, context):
        response = medusa_pb2.BackupStatusResponse()
        try:
            with Storage(config=self.storage_config) as storage:
                # find the backup
                backup = storage.get_node_backup(fqdn=storage.config.fqdn, name=request.backupName)
                if backup.started is None:
                    raise KeyError
                # work out the timings
                response.startTime = datetime.fromtimestamp(backup.started).strftime(TIMESTAMP_FORMAT)
                if backup.finished:
                    response.finishTime = datetime.fromtimestamp(backup.finished).strftime(TIMESTAMP_FORMAT)
                else:
                    response.finishTime = ""
                BackupMan.register_backup(request.backupName, is_async=False, overwrite_existing=False)
                status = BackupMan.STATUS_UNKNOWN
                if backup.started:
                    status = BackupMan.STATUS_IN_PROGRESS
                if backup.finished:
                    status = BackupMan.STATUS_SUCCESS
                BackupMan.update_backup_status(request.backupName, status)
                # record the status
                record_status_in_response(response, request.backupName)
        except KeyError:
            context.set_details("backup <{}> does not exist".format(request.backupName))
            context.set_code(grpc.StatusCode.NOT_FOUND)
            response.status = medusa_pb2.StatusType.UNKNOWN

        return response

    def GetBackup(self, request, context):
        response = medusa_pb2.GetBackupResponse()
        try:
            with Storage(config=self.storage_config) as connected_storage:
                backup = connected_storage.get_cluster_backup(request.backupName)
                summary = get_backup_summary(backup)
                response.backup.CopyFrom(summary)
                response.status = summary.status
        except Exception as e:
            context.set_details("Failed to get backup due to error: {}".format(e))
            context.set_code(grpc.StatusCode.INTERNAL)
            response.status = medusa_pb2.StatusType.UNKNOWN
            response.backup.status = medusa_pb2.StatusType.UNKNOWN
        return response

    def GetBackups(self, request, context):
        response = medusa_pb2.GetBackupsResponse()
        try:
            # cluster backups
            with Storage(config=self.storage_config) as connected_storage:
                backups = get_backups(connected_storage, self.config, True)
                for backup in backups:
                    summary = get_backup_summary(backup)
                    response.backups.append(summary)
                set_overall_status(response)

        except Exception as e:
            context.set_details("Failed to get backups due to error: {}".format(e))
            context.set_code(grpc.StatusCode.INTERNAL)
            response.status = medusa_pb2.StatusType.UNKNOWN
        return response

    def DeleteBackup(self, request, context):
        logging.info("Deleting backup {}".format(request.name))
        response = medusa_pb2.DeleteBackupResponse()
        response.name = request.name

        if not BackupMan.is_active():
            logging.warning(f"Backup manager has no backups on record")
            response.status = medusa_pb2.StatusType.FAILED
            return response

        if BackupMan.get_backup_status(request.name) == BackupMan.STATUS_IN_PROGRESS:
            logging.warning(f"Attempted to delete a running backup {request.name}")
            response.status = medusa_pb2.StatusType.FAILED
            return response

        try:
            delete_backup(self.config, [request.name], True)
            handle_backup_removal(request.name)
            response.status = medusa_pb2.StatusType.UNKNOWN
        except Exception as e:
            context.set_details("deleting backups failed: {}".format(e))
            context.set_code(grpc.StatusCode.INTERNAL)
            logging.exception("Deleting backup {} failed".format(request.name))
        return response

    def PurgeBackups(self, request, context):
        logging.info("Purging backups with max age {} and max count {}"
                     .format(self.config.storage.max_backup_age, self.config.storage.max_backup_count))
        response = medusa_pb2.PurgeBackupsResponse()

        try:
            (nb_objects_purged, total_purged_size, total_objects_within_grace, nb_backups_purged) = purge.main(
                self.config,
                max_backup_age=int(self.config.storage.max_backup_age),
                max_backup_count=int(self.config.storage.max_backup_count))
            response.nbObjectsPurged = nb_objects_purged
            response.totalPurgedSize = total_purged_size
            response.totalObjectsWithinGcGrace = total_objects_within_grace
            response.nbBackupsPurged = nb_backups_purged

        except Exception as e:
            context.set_details("purging backups failed: {}".format(e))
            context.set_code(grpc.StatusCode.INTERNAL)
            logging.exception("Purging backups failed")
        return response

    def PrepareRestore(self, request, context):
        logging.info("Preparing restore {} for backup {}".format(request.restoreKey, request.backupName))
        response = medusa_pb2.PrepareRestoreResponse()
        try:
            with Storage(config=self.storage_config) as connected_storage:
                cluster_backup = connected_storage.get_cluster_backup(request.backupName)
                restore_job = RestoreJob(cluster_backup,
                                         self.config, Path("/tmp"),
                                         None,
                                         "127.0.0.1",
                                         True,
                                         False,
                                         1,
                                         bypass_checks=True)
                restore_job.prepare_restore()
                os.makedirs(RESTORE_MAPPING_LOCATION, exist_ok=True)
                with open(f"{RESTORE_MAPPING_LOCATION}/{request.restoreKey}", "w") as f:
                    f.write(json.dumps({'in_place': restore_job.in_place, 'host_map': restore_job.host_map}))
        except Exception as e:
            context.set_details("Failed to prepare restore: {}".format(e))
            context.set_code(grpc.StatusCode.INTERNAL)
            logging.exception("Failed restore prep {} for backup {}".format(request.restoreKey, request.backupName))
        return response


def set_overall_status(get_backups_response):
    get_backups_response.overallStatus = medusa_pb2.StatusType.UNKNOWN
    backups = get_backups_response.backups
    if len(backups) == 0:
        return
    if all(backup.status == medusa_pb2.StatusType.SUCCESS for backup in backups):
        get_backups_response.overallStatus = medusa_pb2.StatusType.SUCCESS
    if any(backup.status == medusa_pb2.StatusType.IN_PROGRESS for backup in backups):
        get_backups_response.overallStatus = medusa_pb2.StatusType.IN_PROGRESS
    if any(backup.status == medusa_pb2.StatusType.FAILED for backup in backups):
        get_backups_response.overallStatus = medusa_pb2.StatusType.FAILED
    if any(backup.status == medusa_pb2.StatusType.UNKNOWN for backup in backups):
        get_backups_response.overallStatus = medusa_pb2.StatusType.UNKNOWN


def get_backup_summary(backup):
    summary = medusa_pb2.BackupSummary()

    summary.backupName = backup.name

    if backup.started is None:
        summary.startTime = 0
    else:
        summary.startTime = backup.started

    if backup.finished is None:
        summary.finishTime = 0
        summary.status = medusa_pb2.StatusType.IN_PROGRESS
    else:
        summary.finishTime = backup.finished
        summary.status = medusa_pb2.StatusType.SUCCESS

    summary.totalNodes = len(backup.tokenmap)
    summary.finishedNodes = len(backup.complete_nodes())

    for node in backup.tokenmap:
        summary.nodes.append(create_token_map_node(backup, node))

    summary.backupType = backup.backup_type

    summary.totalSize = backup.size()
    summary.totalObjects = backup.num_objects()

    return summary


# Callback function for recording unique backup results
def record_backup_info(future):
    try:
        logging.info("Recording async backup information.")
        if future.exception():
            logging.error("Failed to record backup information executed in "
                          "async manner. Error: {}".format(future.exception()))
            return

        result = future.result()
        if not result:
            logging.error("Expected a backup result for recording in callback function.")
            return

        (actual_backup_duration, actual_start, end, node_backup, num_files, num_replaced, num_kept, start,
         backup_name) = result

        logging.info("Setting result in callback for backup Name: {}".format(backup_name))
        BackupMan.set_backup_result(backup_name, result)

    except Exception as e:
        logging.error("Failed to record backup information executed in async manner. Error: {}".format(e))


def create_token_map_node(backup, node):
    token_map_node = medusa_pb2.BackupNode()
    token_map_node.host = node
    token_map_node.datacenter = backup.tokenmap[node]["dc"] if "dc" in backup.tokenmap[node] else ""
    token_map_node.rack = backup.tokenmap[node]["rack"] if "rack" in backup.tokenmap[node] else ""
    if "tokens" in backup.tokenmap[node]:
        for token in backup.tokenmap[node]["tokens"]:
            token_map_node.tokens.append(token)
    return token_map_node


# Transform internal status code to gRPC backup status type
def record_status_in_response(response, backup_name):
    status = BackupMan.get_backup_status(backup_name)
    if status == BackupMan.STATUS_IN_PROGRESS:
        response.status = medusa_pb2.StatusType.IN_PROGRESS
    if status == BackupMan.STATUS_FAILED:
        response.status = medusa_pb2.StatusType.FAILED
    if status == BackupMan.STATUS_SUCCESS:
        response.status = medusa_pb2.StatusType.SUCCESS


def handle_backup_removal(backup_name):
    if not BackupMan.remove_backup(backup_name):
        logging.error("Failed to cleanup single backup Name: {}".format(backup_name))


def handle_backup_removal_all():
    if not BackupMan.remove_all_backups():
        logging.error("Failed to cleanup all backups")


async def main():
    if len(sys.argv) > 2:
        config_file_path = sys.argv[2]
    else:
        config_file_path = "/etc/medusa/medusa.ini"

    server = Server(config_file_path)
    await server.serve()


if __name__ == '__main__':
    asyncio.run(main())
