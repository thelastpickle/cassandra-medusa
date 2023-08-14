import unittest
from unittest.mock import Mock
from medusa.purge_decommissioned import get_all_nodes, get_decommissioned_nodes


class TestGetNodes(unittest.TestCase):

    def test_get_all_nodes(self):
        # Mocking blobs
        blob1 = Mock()
        blob1.name = "node1/"
        blob2 = Mock()
        blob2.name = "node2/"
        blob3 = Mock()
        blob3.name = "index/"
        blobs = [blob1, blob2, blob3]
        nodes = get_all_nodes(blobs)
        self.assertEqual(nodes, {'node1', 'node2'})

    def test_get_decommissioned_nodes(self):
        all_nodes = {'node1', 'node2', 'node3', 'node4'}
        live_nodes = {'node1', 'node3'}
        decommissioned_nodes = get_decommissioned_nodes(all_nodes, live_nodes)
        self.assertEqual(decommissioned_nodes, {'node2', 'node4'})


if __name__ == '__main__':
    unittest.main()
