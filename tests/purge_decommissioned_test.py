import unittest
from unittest.mock import Mock
from medusa.purge_decommissioned import get_decommissioned_nodes, get_nodes


class TestGetNodes(unittest.TestCase):

    def setUp(self):
        self.mock_backup1 = Mock()
        self.mock_backup1.node_backups.keys.return_value = {'node1', 'node2', 'node3'}

        self.mock_backup2 = Mock()
        self.mock_backup2.node_backups.keys.return_value = {'node1', 'node2'}

        self.all_backups = [self.mock_backup1, self.mock_backup2]
        self.latest_backup = [self.mock_backup2]

    def test_get_nodes(self):
        expected_result = {'node1', 'node2', 'node3'}
        result = get_nodes(self.all_backups)
        self.assertEqual(result, expected_result)

    def test_get_decommissioned_nodes(self):
        expected_decommissioned_nodes = {'node1', 'node2', 'node3'} - {'node1', 'node2'}
        result = get_decommissioned_nodes(self.all_backups, self.latest_backup)
        self.assertEqual(result, expected_decommissioned_nodes)
