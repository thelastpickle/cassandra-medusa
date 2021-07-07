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
        BackupMan.cleanup_all_backups()

    def test_is_not_active(self):
        self.assertFalse(BackupMan.is_active())

    def test_is_active(self):
        BackupMan.set_backup(Mock(), Mock())
        self.assertTrue(BackupMan.is_active())

    def test_get_backup_not_registered(self):
        with self.assertRaises(RuntimeError) as expected_err:
            BackupMan.get_backup_future("test_backup_id")

        self.assertEqual("Backups not found, must set before getting.", str(expected_err.exception))

    def test_register_backup_missing_name(self):
        with self.assertRaises(RuntimeError) as expected_err:
            BackupMan.set_backup(None, Mock(concurrent.futures.Future))

        self.assertEqual("No backup identifier and/or future supplied.", str(expected_err.exception))

    def test_register_backup_missing_future(self):
        with self.assertRaises(RuntimeError) as expected_err:
            BackupMan.set_backup("test_backup_id", None)

        self.assertEqual("No backup identifier and/or future supplied.", str(expected_err.exception))

    def test_register_backup(self):
        backup_id = "test_backup_id"
        mock_future = Mock(concurrent.futures.Future)
        BackupMan.set_backup(backup_id, mock_future)

        backup_id_2 = "test_backup_id_2"
        mock_future_2 = Mock(concurrent.futures.Future)
        BackupMan.set_backup(backup_id_2, mock_future_2)

        stored_future = BackupMan.get_backup_future(backup_id)
        self.assertIs(stored_future, mock_future, "expecting the stored to equal mock registered.")

        stored_future_2 = BackupMan.get_backup_future(backup_id_2)
        self.assertIs(stored_future_2, mock_future_2, "expecting the stored to equal mock registered.")

    def test_register_backup_duplicate(self):
        # "Unable to set as a backup already exists with id: {}
        backup_id_1 = "test_backup_id"
        mock_future_1 = Mock(concurrent.futures.Future)
        BackupMan.set_backup(backup_id_1, mock_future_1)

        with self.assertRaises(RuntimeError) as expected_err:
            BackupMan.set_backup(backup_id_1, mock_future_1)

        self.assertEqual(
            "Unable to set as a backup already exists with id: {}".format(backup_id_1), str(expected_err.exception)
        )

    def test_backup_man_singleton_check(self):
        backup_id_1 = "test_backup_id"
        mock_future_1 = Mock(concurrent.futures.Future)
        BackupMan.set_backup(backup_id_1, mock_future_1)

        with self.assertRaises(RuntimeError) as expected_err:
            BackupMan()

        self.assertEqual("Unable to re-init BackupMan.", str(expected_err.exception))
