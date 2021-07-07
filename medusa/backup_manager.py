# -*- coding: utf-8 -*-
# Copyright 2021 DataStax, Inc. All rights reserved.
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


import threading
import asyncio
import logging


class BackupMan:
    IN_PROGRESS = 0
    SUCCESS = 1
    FAILED = 2

    __instance = None
    __backups = {}

    def __init__(self):
        if BackupMan.__instance:
            raise RuntimeError('Unable to re-init BackupMan.')
        self.lock = threading.Lock()
        with self.lock:
            BackupMan.__instance = self
            BackupMan.__backups = {}

    @staticmethod
    def is_active():
        return BackupMan.__instance is not None

    @staticmethod
    def set_backup(backup_id, future):
        if not backup_id or not future:
            raise RuntimeError('No backup identifier and/or future supplied.')

        lock = threading.Lock()
        with lock:
            if not BackupMan.__instance:
                BackupMan()

            if backup_id in BackupMan.__instance.__backups:
                raise RuntimeError('Unable to set as a backup already exists with id: {}'.format(backup_id))

            BackupMan.__instance.__backups[backup_id] = future
            logging.info("Completed registration of backup id {}".format(backup_id))

    # Caller can decide how long to wait for a result using the registered backup future returned.
    @staticmethod
    def get_backup_future(backup_id):
        if not BackupMan.__instance:
            raise RuntimeError('Backups not found, must set before getting.')
        logging.info("Returning backup future for id: {}".format(backup_id))
        return BackupMan.__instance.__backups[backup_id]

    # Returns True when an existing backup_id was found and removed.
    # Returns False when an existing backup_id was not found.
    @staticmethod
    def remove_backup(backup_id):
        if not BackupMan.__instance or backup_id not in BackupMan.__instance.__backups:
            return False

        lock = threading.Lock()
        with lock:
            backup_future = BackupMan.__instance.__backups.pop(backup_id)
            if asyncio.isfuture(backup_future):
                backup_future.cancel("Cancelling backup for id: {} with done state: {}"
                                     .format(backup_id, backup_future.done()))
            logging.debug("Backup removed for id: {}".format(backup_id))
        return True

    # Includes visibility of backup current state while being cleaned.
    @staticmethod
    def cleanup_all_backups():
        if BackupMan.__instance:
            lock = threading.Lock()
            with lock:
                for backup_id in list(BackupMan.__instance.__backups):
                    backup_future = BackupMan.__instance.__backups[backup_id]
                    if asyncio.isfuture(backup_future):
                        logging.debug("Cancelling backup` id: {}".format(backup_id))
                        backup_future.cancel("Cleanup of all backups requested. Cancelling backup for id: {} with "
                                             "done state: {}".format(backup_id, backup_future.done()))
                    del BackupMan.__instance.__backups[backup_id]
                BackupMan.__instance.__backups = None
                BackupMan.__instance = None
                logging.debug("Cleanup of all backups registered completed.")
