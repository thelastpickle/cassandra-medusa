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


import asyncio
import logging
import threading


class BackupMan:
    # Status references
    STATUS_UNKNOWN = -1
    STATUS_IN_PROGRESS = 0
    STATUS_SUCCESS = 1
    STATUS_FAILED = 2

    # Error Messages
    NO_BACKUP_NAME_ERR_MSG = "No backup name supplied"
    NO_FUTURE_ERR_MSG = "No future supplied"

    # [future ref | None]
    __IDX_FUTURE = 0
    # [UNKNOWN (default) | SUCCESS | FAILED | IN_PROGRESS]
    __IDX_STATUS = 1
    # [TRUE | FALSE]
    __IDX_IS_ASYNC = 2

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
    def is_async_mode(backup_name):
        if not backup_name:
            raise RuntimeError(BackupMan.NO_BACKUP_NAME_ERR_MSG)

        backup_state = BackupMan.__instance.__backups[backup_name]
        if backup_state:
            return backup_state[BackupMan.__IDX_IS_ASYNC]

        return False

    @staticmethod
    def is_active():
        return BackupMan.__instance is not None

    @staticmethod
    def get_backup_status(backup_name):
        if not backup_name:
            raise RuntimeError(BackupMan.NO_BACKUP_NAME_ERR_MSG)

        lock = threading.Lock()
        with lock:
            if backup_name not in BackupMan.__instance.__backups:
                raise RuntimeError('Unable to get status for backup id: {}'.format(backup_name))

            backup_state = BackupMan.__instance.__backups[backup_name]
            if backup_state:
                return backup_state[BackupMan.__IDX_STATUS]

            return BackupMan.STATUS_UNKNOWN

    @staticmethod
    def set_backup_future(backup_name, future):

        if not backup_name:
            raise RuntimeError(BackupMan.NO_BACKUP_NAME_ERR_MSG)

        if not future:
            raise RuntimeError(BackupMan.NO_FUTURE_ERR_MSG)

        lock = threading.Lock()
        with lock:
            if not BackupMan.__instance:
                BackupMan()

            # is_async implied True when future is being set.
            BackupMan.__instance.__backups[backup_name] = [future, BackupMan.STATUS_UNKNOWN, True]
            logging.info("Registered backup id {}".format(backup_name))

    # Sets the future for a backup; unknown on overall status at this point unless already existing
    @staticmethod
    def register_backup(backup_name, is_async, overwrite_existing=True):
        if not backup_name:
            raise RuntimeError(BackupMan.NO_BACKUP_NAME_ERR_MSG)

        lock = threading.Lock()
        with lock:
            if not BackupMan.__instance:
                BackupMan()

            if backup_name in BackupMan.__instance.__backups:
                if overwrite_existing:
                    if not BackupMan.__clean(backup_name):
                        logging.error(f"Registered backup name {backup_name} cleanup failed prior to re-register.")

            BackupMan.__instance.__backups[backup_name] = [None, BackupMan.STATUS_UNKNOWN, is_async]
            logging.info("Registered backup id {}".format(backup_name))

    # Caller can decide how long to wait for a result using the registered backup future returned.
    # A future is returned (for async mode), otherwise None (for non-async mode).
    # Exception thrown when expected backup not located.
    @staticmethod
    def get_backup_future(backup_name):
        if not BackupMan.__instance:
            raise RuntimeError('Backups not found, must set before getting.')

        lock = threading.Lock()
        with lock:
            backup_state = BackupMan.__instance.__backups[backup_name]
            if backup_state:
                logging.debug("Returning backup future for id: {}".format(backup_name))
                return backup_state[BackupMan.__IDX_FUTURE]

            raise RuntimeError('Backup not located for id: {}'.format(backup_name))

    @staticmethod
    def remove_backup(backup_name):
        if not BackupMan.__instance or backup_name not in BackupMan.__instance.__backups:
            return True

        lock = threading.Lock()
        with lock:
            return BackupMan.__clean(backup_name)

    @staticmethod
    def remove_all_backups():

        is_all_cleanup_successful = True
        lock = threading.Lock()
        with lock:
            if not BackupMan.__instance or BackupMan.__instance.__backups is None:
                is_all_cleanup_successful = True
            else:
                for backup_name in list(BackupMan.__instance.__backups):
                    if not BackupMan.__clean(backup_name):
                        is_all_cleanup_successful = False
                BackupMan.__instance.__backups = None
                BackupMan.__instance = None

        if is_all_cleanup_successful:
            logging.info("Cleanup of all backups registered was completed.")
        else:
            logging.error("Cleanup of all backups identified failures.")

        return is_all_cleanup_successful

    # Backup must already be registered to perform status update
    # Used for async non-async backup tracking
    @staticmethod
    def update_backup_status(backup_name, status):

        if not backup_name:
            raise RuntimeError(BackupMan.NO_BACKUP_NAME_ERR_MSG)

        if BackupMan.__instance:
            lock = threading.Lock()
            with lock:
                if backup_name in BackupMan.__instance.__backups:
                    old_status = BackupMan.__instance.__backups[backup_name][BackupMan.__IDX_STATUS]
                    logging.debug(
                        "Updated from existing status: {} to new status: {} for backup id: {} "
                        .format(old_status, status, backup_name)
                    )
                    BackupMan.__instance.__backups[backup_name][BackupMan.__IDX_STATUS] = status
                else:
                    raise RuntimeError('Unable to update backup status for backup id: {} '
                                       'as it is not registered.'.format(backup_name))
        else:
            raise RuntimeError('Must register a backup before updating its status. Backup Name: {} '
                               'not registered.'.format(backup_name))

    @staticmethod
    def __clean(backup_name):
        try:
            backup_future = BackupMan.__instance.__backups[backup_name][0]
            logging.debug("Cancelling backup id: {}".format(backup_name))
            if backup_future is not None and asyncio.isfuture(backup_future):
                backup_future.cancel("Removal of backup requested. Cancelling backup Name: {} with "
                                     "done state: {}".format(backup_name, backup_future.done()))
            del BackupMan.__instance.__backups[backup_name]
        except Exception as e:
            logging.error("Failed removal of backup for Name: {} due to "
                          "Error: {}".format(backup_name, str(e)))
            return False
        return True

    @staticmethod
    def set_backup_result(backup_name, result):
        if not backup_name or not result:
            raise RuntimeError("Failed to set backup result as backup name and/or result is missing.")
