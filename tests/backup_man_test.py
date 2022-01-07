# -*- coding: utf-8 -*-
# Copyright 2021- Datastax, Inc. All rights reserved.
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
import concurrent.futures
import unittest
from unittest.mock import Mock

from medusa.backup_node import BackupMan


class BackupManTest(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def setUp(self):
        BackupMan.remove_all_backups()

    def test_is_not_active(self):
        self.assertFalse(BackupMan.is_active())

    def test_is_active(self):
        BackupMan.register_backup(Mock(), is_async=False)
        self.assertTrue(BackupMan.is_active())

    def test_get_backup_not_registered(self):
        with self.assertRaises(RuntimeError) as expected_err:
            BackupMan.get_backup_future("test_backup_id")

        self.assertEqual("Backups not found, must set before getting.", str(expected_err.exception))

    def test_register_backup_missing_name(self):
        with self.assertRaises(RuntimeError) as expected_err:
            BackupMan.register_backup(None, False)
        self.assertEqual(BackupMan.NO_BACKUP_NAME_ERR_MSG, str(expected_err.exception))

    def test_set_backup_future_missing_future(self):
        with self.assertRaises(RuntimeError) as expected_err:
            BackupMan.set_backup_future("test_backup_id", None)
        self.assertEqual(BackupMan.NO_FUTURE_ERR_MSG, str(expected_err.exception))

    def test_set_backup_future_missing_name(self):
        with self.assertRaises(RuntimeError) as expected_err:
            BackupMan.set_backup_future(None, Mock())
        self.assertEqual(BackupMan.NO_BACKUP_NAME_ERR_MSG, str(expected_err.exception))

    def test_register_backup_sync_mode(self):
        BackupMan.register_backup("test_backup_id", is_async=False)
        self.assertEqual(BackupMan.STATUS_UNKNOWN, BackupMan.get_backup_status("test_backup_id"))
        self.assertEqual(None, BackupMan.get_backup_future("test_backup_id"))

        BackupMan.update_backup_status("test_backup_id", BackupMan.STATUS_SUCCESS)
        self.assertEqual(BackupMan.STATUS_SUCCESS, BackupMan.get_backup_status("test_backup_id"))

    def test_register_backup_async_mode(self):
        backup_id = "test_backup_id"
        mock_future = Mock(concurrent.futures.Future)
        BackupMan.register_backup(backup_id, is_async=True)
        BackupMan.set_backup_future(backup_id, mock_future)
        stored_future = BackupMan.get_backup_future(backup_id)

        self.assertEqual(BackupMan.STATUS_UNKNOWN, BackupMan.get_backup_status("test_backup_id"))
        self.assertTrue(BackupMan.is_async_mode("test_backup_id"))
        self.assertIs(stored_future, mock_future, "expecting the stored to equal mock registered.")

        backup_id_2 = "test_backup_id_2"
        mock_future_2 = Mock(concurrent.futures.Future)
        BackupMan.register_backup(backup_id_2, is_async=True)
        BackupMan.set_backup_future(backup_id_2, mock_future_2)

        # Update initial status
        BackupMan.update_backup_status(backup_id_2, BackupMan.STATUS_FAILED)
        stored_future_2 = BackupMan.get_backup_future(backup_id_2)

        self.assertTrue(BackupMan.is_async_mode(backup_id_2))
        self.assertEqual(BackupMan.STATUS_FAILED, BackupMan.get_backup_status(backup_id_2))
        self.assertIs(stored_future_2, mock_future_2, "expecting the stored to equal mock registered.")

    def test_register_backup_duplicate(self):
        # Self-healing of detected duplicate, clean and reset w/ new expected
        backup_id_1 = "test_backup_id"
        mock_future_1 = Mock(concurrent.futures.Future)
        mock_future_2 = Mock(concurrent.futures.Future)
        BackupMan.register_backup(backup_id_1, is_async=True)
        BackupMan.set_backup_future(backup_id_1, mock_future_1)
        self.assertEqual(BackupMan.get_backup_future(backup_id_1), mock_future_1)

        # Set using the same named backup.
        BackupMan.register_backup(backup_id_1, is_async=True)
        BackupMan.set_backup_future(backup_id_1, mock_future_2)

        # Expecting different futures
        self.assertNotEqual(BackupMan.get_backup_future(backup_id_1), mock_future_1)
        self.assertEqual(BackupMan.get_backup_future(backup_id_1), mock_future_2)

    def test_backup_man_singleton_check(self):
        backup_id_1 = "test_backup_id"
        mock_future_1 = Mock(concurrent.futures.Future)
        BackupMan.register_backup(backup_id_1, is_async=True)
        BackupMan.set_backup_future(backup_id_1, mock_future_1)

        with self.assertRaises(RuntimeError) as expected_err:
            BackupMan()

        self.assertEqual("Unable to re-init BackupMan.", str(expected_err.exception))

    # Initial registering a backup as sync, but later supplying a future, the mode becomes async
    def test_backup_man_registered_sync_corrected_mode_with_future(self):
        backup_id_1 = "test_backup_id"
        mock_future_1 = Mock(concurrent.futures.Future)
        BackupMan.register_backup(backup_id_1, is_async=False)
        BackupMan.set_backup_future(backup_id_1, mock_future_1)

        self.assertTrue(BackupMan.is_async_mode(backup_id_1))
