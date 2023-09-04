import unittest
from unittest.mock import Mock
from medusa.purge_decommissioned import get_all_nodes, get_decommissioned_nodes


class TestGetNodes(unittest.TestCase):

    def test_get_all_nodes(self):
        mock_blob1 = Mock()
        mock_blob1.name = '2023-08-14T17:09:01.728867-ec0b07ce-f0d5-40a0-86a7-61908cb579a8/127.0.0.1/data/file1'

        mock_blob2 = Mock()
        mock_blob2.name = '2023-08-14T17:09:01.728867-ec0b07ce-f0d5-40a0-86a7-61908cb579a8/127.0.0.2/data/file2'

        mock_blob3 = Mock()
        mock_blob3.name = '2023-08-14T17:09:01.728867-ec0b07ce-f0d5-40a0-86a7-61908cb579a8/index/data/file3'

        blobs = [mock_blob1, mock_blob2, mock_blob3]

        nodes = get_all_nodes(blobs)

        self.assertEqual(nodes, {'127.0.0.1', '127.0.0.2'})

    def test_get_decommissioned_nodes(self):
        all_nodes = {'node1', 'node2', 'node3', 'node4'}
        live_nodes = {'node1', 'node3'}
        decommissioned_nodes = get_decommissioned_nodes(all_nodes, live_nodes)
        self.assertEqual(decommissioned_nodes, {'node2', 'node4'})


if __name__ == '__main__':
    unittest.main()
