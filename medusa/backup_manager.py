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
import os
import time
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

    # Cleanup configuration - PATCH: Added for memory leak prevention
    MAX_COMPLETED_BACKUPS = int(os.environ.get('MEDUSA_MAX_COMPLETED_BACKUPS', 10))  # Maximum number of completed backups to keep in memory
    BACKUP_RETENTION_SECONDS = int(os.environ.get('MEDUSA_BACKUP_RETENTION_SECONDS', 3600))  # 1 hour - time to keep completed backups in memory

    # [future ref | None]
    __IDX_FUTURE = 0
    # [UNKNOWN (default) | SUCCESS | FAILED | IN_PROGRESS]
    __IDX_STATUS = 1
    # [TRUE | FALSE]
    __IDX_IS_ASYNC = 2
    # [timestamp when backup completed] - PATCH: Added for tracking completion time
    __IDX_COMPLETED_AT = 3

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

            # is_async implied True when future is being set. completed_at = None initially
            # PATCH: Added __IDX_COMPLETED_AT field
            BackupMan.__instance.__backups[backup_name] = [future, BackupMan.STATUS_UNKNOWN, True, None]
            logging.info("Registered backup id {}".format(backup_name))
            
            # PATCH: Trigger cleanup of old completed backups
            BackupMan.__cleanup_old_backups()

    # Sets the future for a backup; unknown on overall status at this point unless already existing
    @staticmethod
    def register_backup(backup_name, is_async, overwrite_existing=True):
        if not backup_name:
            raise RuntimeError(BackupMan.NO_BACKUP_NAME_ERR_MSG)

        lock = threading.Lock()
        with lock:
            if not BackupMan.__instance:
                BackupMan()

            if backup_name in BackupMan.__instance.__backups.keys():
                if overwrite_existing:
                    if not BackupMan.__clean(backup_name):
                        logging.error(f"Registered backup name {backup_name} cleanup failed prior to re-register.")
                    # PATCH: Added __IDX_COMPLETED_AT field
                    BackupMan.__instance.__backups[backup_name] = [None, BackupMan.STATUS_UNKNOWN, is_async, None]
            else:
                # PATCH: Added __IDX_COMPLETED_AT field
                BackupMan.__instance.__backups[backup_name] = [None, BackupMan.STATUS_UNKNOWN, is_async, None]
            logging.info("Registered backup id {}".format(backup_name))
            
            # PATCH: Trigger cleanup of old completed backups
            BackupMan.__cleanup_old_backups()

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
                    
                    # PATCH: Set completion timestamp for SUCCESS or FAILED status
                    if status in [BackupMan.STATUS_SUCCESS, BackupMan.STATUS_FAILED]:
                        BackupMan.__instance.__backups[backup_name][BackupMan.__IDX_COMPLETED_AT] = time.time()
                else:
                    raise RuntimeError('Unable to update backup status for backup id: {} '
                                       'as it is not registered.'.format(backup_name))
        else:
            raise RuntimeError('Must register a backup before updating its status. Backup Name: {} '
                               'not registered.'.format(backup_name))

    # PATCH: Added method to clean up old completed backups
    @staticmethod
    def __cleanup_old_backups():
        """
        Clean up old completed backups to prevent memory leaks.
        Removes backups that are:
        1. Completed (SUCCESS or FAILED) AND older than BACKUP_RETENTION_SECONDS
        2. Keeps at most MAX_COMPLETED_BACKUPS completed backups
        
        NOTE: This method should be called while holding the instance lock.
        """
        if not BackupMan.__instance or not BackupMan.__instance.__backups:
            return
        
        try:
            current_time = time.time()
            completed_backups = []
            
            # Collect completed backups with their completion times
            for backup_name, backup_state in list(BackupMan.__instance.__backups.items()):
                status = backup_state[BackupMan.__IDX_STATUS]
                completed_at = backup_state[BackupMan.__IDX_COMPLETED_AT] if len(backup_state) > BackupMan.__IDX_COMPLETED_AT else None
                
                if status in [BackupMan.STATUS_SUCCESS, BackupMan.STATUS_FAILED] and completed_at is not None:
                    completed_backups.append((backup_name, completed_at))
            
            # Sort by completion time (oldest first)
            completed_backups.sort(key=lambda x: x[1])
            
            backups_to_remove = []
            
            # Remove backups older than retention period
            for backup_name, completed_at in completed_backups:
                age_seconds = current_time - completed_at
                if age_seconds > BackupMan.BACKUP_RETENTION_SECONDS:
                    backups_to_remove.append(backup_name)
                    logging.debug(f"Marking backup {backup_name} for cleanup (age: {age_seconds:.0f}s)")
            
            # Also remove excess backups beyond MAX_COMPLETED_BACKUPS
            remaining_completed = len(completed_backups) - len(backups_to_remove)
            if remaining_completed > BackupMan.MAX_COMPLETED_BACKUPS:
                excess_count = remaining_completed - BackupMan.MAX_COMPLETED_BACKUPS
                for backup_name, _ in completed_backups:
                    if backup_name not in backups_to_remove and excess_count > 0:
                        backups_to_remove.append(backup_name)
                        excess_count -= 1
            
            # Perform cleanup
            for backup_name in backups_to_remove:
                # Double-check: Never remove backups that are still in progress
                if backup_name in BackupMan.__instance.__backups:
                    backup_state = BackupMan.__instance.__backups[backup_name]
                    status = backup_state[BackupMan.__IDX_STATUS]
                    if status == BackupMan.STATUS_IN_PROGRESS:
                        logging.warning(f"Skipping cleanup of backup {backup_name} - still in progress")
                        continue
                BackupMan.__clean(backup_name)
                logging.info(f"Cleaned up old backup from memory: {backup_name}")
                
        except Exception as e:
            logging.warning(f"Error during backup cleanup: {e}")

    # PATCH: Added public method to get current backup count (for monitoring)
    @staticmethod
    def get_backup_count():
        """Returns the current number of backups tracked in memory."""
        if not BackupMan.__instance or not BackupMan.__instance.__backups:
            return 0
        return len(BackupMan.__instance.__backups)

    # PATCH: Added public method to force cleanup (can be called externally)
    @staticmethod
    def force_cleanup():
        """
        Force cleanup of old completed backups. Can be called externally.
        
        NOTE: Uses a local lock for consistency with other methods in this class.
        For full thread-safety, all methods should use the instance lock, but that
        would require a broader refactoring of the existing codebase.
        """
        if not BackupMan.__instance:
            return
        # Use local lock for consistency with other methods (register_backup, set_backup_future, etc.)
        lock = threading.Lock()
        with lock:
            BackupMan.__cleanup_old_backups()

    @staticmethod
    def __clean(backup_name):
        try:
            # Safety check: Never clean backups that are in progress
            if backup_name not in BackupMan.__instance.__backups:
                return True
            backup_state = BackupMan.__instance.__backups[backup_name]
            status = backup_state[BackupMan.__IDX_STATUS]
            if status == BackupMan.STATUS_IN_PROGRESS:
                logging.warning(f"Attempted to clean backup {backup_name} that is still IN_PROGRESS - skipping")
                return False
            
            backup_future = backup_state[BackupMan.__IDX_FUTURE]
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
