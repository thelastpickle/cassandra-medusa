# -*- coding: utf-8 -*-
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
import unittest
import os
from unittest.mock import MagicMock, patch
from pathlib import PosixPath

from medusa.service.grpc.restore import apply_mapping_env, restore_backup


class ServiceRestoreTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def setUp(self):
        os.environ.pop('POD_IP', None)
        os.environ.pop('POD_NAME', None)
        os.environ.pop('RESTORE_MAPPING', None)

    def test_restore_inplace(self):
        os.environ['POD_NAME'] = 'test-dc1-sts-0'
        os.environ['RESTORE_MAPPING'] = '{"in_place": true, "host_map": {' \
            + '"test-dc1-sts-0": {"source": ["test-dc1-sts-0"], "seed": false},' \
            + '"test-dc1-sts-1": {"source": ["test-dc1-sts-1"], "seed": false},' \
            + '"test-dc1-sts-2": {"source": "prod-dc1-sts-2", "seed": false}}}'
        in_place = apply_mapping_env()

        assert in_place is True
        assert "POD_IP" not in os.environ.keys()

    def test_restore_remote(self):
        os.environ.update({'POD_NAME': 'test-dc1-sts-0'})
        os.environ['RESTORE_MAPPING'] = '{"in_place": false, "host_map": {' \
            + '"test-dc1-sts-0": {"source": ["prod-dc1-sts-3"], "seed": false},' \
            + '"test-dc1-sts-1": {"source": ["prod-dc1-sts-1"], "seed": false},' \
            + '"test-dc1-sts-2": {"source": "prod-dc1-sts-2", "seed": false}}}'
        in_place = apply_mapping_env()

        assert in_place is False
        assert "POD_IP" in os.environ.keys()
        assert os.environ['POD_IP'] == 'prod-dc1-sts-3'

    def test_restore_no_match(self):
        os.environ['POD_NAME'] = 'test-dc1-sts-0'
        os.environ['RESTORE_MAPPING'] = '{"in_place": false, "host_map": {' \
            + '"test-dc1-sts-3": {"source": ["prod-dc1-sts-3"], "seed": false},' \
            + '"test-dc1-sts-1": {"source": ["prod-dc1-sts-1"], "seed": false},' \
            + '"test-dc1-sts-2": {"source": "prod-dc1-sts-2", "seed": false}}}'
        in_place = apply_mapping_env()

        assert in_place is None
        assert "POD_IP" not in os.environ.keys()

    def test_success_restore_backup(self):
        # Define test inputs
        in_place = True
        config = {'some': 'config'}

        # Define expected output
        expected_output = 'Finished restore of backup test_backup'

        # Set up mock environment variables
        os.environ["BACKUP_NAME"] = "test_backup"
        os.environ["MEDUSA_TMP_DIR"] = "/tmp"

        # Set up mock for medusa.listing.get_backups()
        with patch('medusa.listing.get_backups') as mock_get_backups:
            mock_cluster_backup = MagicMock()
            mock_cluster_backup.name = "test_backup"
            mock_get_backups.return_value = [mock_cluster_backup]

            # Set up mock for medusa.restore_node.restore_node()
            with patch('medusa.restore_node.restore_node') as mock_restore_node:
                mock_restore_node.return_value = None

                # Call the function
                result = restore_backup(in_place, config)

                # Assertions
                assert result == expected_output
                mock_get_backups.assert_called_once_with(config, True)
                mock_restore_node.assert_called_once_with(config, PosixPath('/tmp'),
                                                          'test_backup', True, False, None, False, {}, {}, False)

    def test_fail_restore_backup(self):
        # Define test inputs
        in_place = True
        config = {'some': 'config'}

        # Define expected output
        expected_output = 'Skipped restore of missing backup test_backup'

        # Set up mock environment variables
        os.environ["BACKUP_NAME"] = "test_backup"
        os.environ["MEDUSA_TMP_DIR"] = "/tmp"

        # Set up mock for medusa.listing.get_backups()
        with patch('medusa.listing.get_backups') as mock_get_backups:
            mock_cluster_backup = MagicMock()
            mock_cluster_backup.name = "test_backup10"
            mock_get_backups.return_value = [mock_cluster_backup]

            # Set up mock for medusa.restore_node.restore_node()
            with patch('medusa.restore_node.restore_node') as mock_restore_node:
                mock_restore_node.return_value = None

                result = restore_backup(in_place, config)

                assert result == expected_output
                mock_get_backups.assert_called_once_with(config, True)
                mock_restore_node.assert_not_called()


if __name__ == '__main__':
    unittest.main()
