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

import grpc
import logging
from grpc_health.v1 import health_pb2
from grpc_health.v1 import health_pb2_grpc

from medusa.service.grpc import medusa_pb2
from medusa.service.grpc import medusa_pb2_grpc


class Client:
    def __init__(self, target, channel_options=[]):
        self.channel = grpc.aio.insecure_channel(target, options=channel_options)

    async def health_check(self):
        try:
            health_stub = health_pb2_grpc.HealthStub(self.channel)
            request = health_pb2.HealthCheckRequest()
            return await health_stub.Check(request)
        except grpc.RpcError as e:
            logging.error("Failed health check due to error: {}".format(e))
            return None

    def create_backup_stub(self, mode):
        stub = medusa_pb2_grpc.MedusaStub(self.channel)
        if mode == "differential":
            backup_mode = 0
        elif mode == "full":
            backup_mode = 1
        else:
            raise RuntimeError("{} is not a recognized backup mode".format(mode))
        return backup_mode, stub

    async def async_backup(self, name, mode):
        try:
            backup_mode, stub = self.create_backup_stub(mode=mode)
            request = medusa_pb2.BackupRequest(name=name, mode=backup_mode)
            return await stub.AsyncBackup(request)
        except grpc.RpcError as e:
            logging.error("Failed async backup for name: {} and mode: {} due to error: {}".format(name, mode, e))
            return None

    async def backup(self, name, mode):
        try:
            backup_mode, stub = self.create_backup_stub(mode=mode)
            request = medusa_pb2.BackupRequest(name=name, mode=backup_mode)
            return await stub.Backup(request)
        except grpc.RpcError as e:
            logging.error("Failed sync backup for name: {} and mode: {} due to error: {}".format(name, mode, e))
            return None

    async def delete_backup(self, name):
        try:
            stub = medusa_pb2_grpc.MedusaStub(self.channel)
            request = medusa_pb2.DeleteBackupRequest(name=name)
            await stub.DeleteBackup(request)
        except grpc.RpcError as e:
            logging.error("Failed to delete backup for name: {} due to error: {}".format(name, e))

    async def get_backup(self, backup_name):
        try:
            stub = medusa_pb2_grpc.MedusaStub(self.channel)
            request = medusa_pb2.GetBackupRequest(backupName=backup_name)
            response = await stub.GetBackup(request)
            return response.backup
        except grpc.RpcError as e:
            logging.error("Failed to obtain backup for name: {} due to error: {}".format(backup_name, e))
            return None

    async def get_backups(self):
        try:
            stub = medusa_pb2_grpc.MedusaStub(self.channel)
            request = medusa_pb2.GetBackupsRequest()
            response = await stub.GetBackups(request)
            return response.backups
        except grpc.RpcError as e:
            logging.error("Failed to obtain list of backups due to error: {}".format(e))
            return None

    async def get_backup_status(self, name):
        try:
            stub = medusa_pb2_grpc.MedusaStub(self.channel)
            request = medusa_pb2.BackupStatusRequest(backupName=name)
            resp = await stub.BackupStatus(request)
            return resp.status
        except grpc.RpcError as e:
            logging.error("Failed to determine backup status for name: {} due to error: {}".format(name, e))
            return medusa_pb2.StatusType.UNKNOWN

    async def backup_exists(self, name):
        try:
            backups = await self.get_backups()
            for backup in backups:
                if backup.backupName == name:
                    return True
            return False
        except grpc.RpcError as e:
            logging.error("Failed to determine if backup exists for backup name: {} due to error: {}".format(name, e))
            return False

    async def purge_backups(self):
        try:
            stub = medusa_pb2_grpc.MedusaStub(self.channel)
            request = medusa_pb2.PurgeBackupsRequest()
            resp = await stub.PurgeBackups(request)
            return resp
        except grpc.RpcError as e:
            logging.error("Failed to purge backups due to error: {}".format(e))
            return None
