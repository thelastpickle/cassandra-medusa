import unittest
from unittest.mock import patch
from medusa.purge_decommissioned import get_all_nodes, get_live_nodes


class TestGetNodes(unittest.TestCase):

    def test_get_all_nodes(self):
        blobs = [
            'node1/',
            'node2/',
            'index/',
            'node3/',
        ]
        nodes = get_all_nodes(blobs)
        self.assertEqual(nodes, {'node1', 'node2', 'node3'})

    @patch("medusa.purge_decommissioned.Cassandra")
    def test_get_live_nodes(self, mock_cassandra):
        mock_cassandra_instance = mock_cassandra.return_value
        mock_cassandra_instance.tokenmap.items.return_value = {"node1", "node3"}
        nodes = get_live_nodes(mock_cassandra_instance)
        self.assertEqual(nodes, {"node1", "node3"})


if __name__ == '__main__':
    unittest.main()
