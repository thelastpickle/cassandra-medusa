import os
import unittest
from unittest import mock
from unittest.mock import MagicMock
from medusa.storage.s3_compat_storage.awscli import AwsCli


class StorageMock:
    def __init__(self):
        self.config = MagicMock()
        self.config.kms_id = None
        self.config.secure = True


class TestAwsCli(unittest.TestCase):

    def create_small_test_file(self):
        with open('small_file.txt', 'w') as small_file:
            small_file.write('a' * 800)
        return 'small_file.txt'

    def create_big_test_file(self):
        with open('big_file.txt', 'w') as big_file:
            big_file.write('a' * 5001)
        return 'big_file.txt'

    def test_get_file_size(self):
        # Arrange
        instance = AwsCli(StorageMock())

        # Act
        small_file_size = instance.get_file_size(self.create_small_test_file())
        big_file_size = instance.get_file_size(self.create_big_test_file())

        # Assert
        self.assertEqual(small_file_size, 800)
        self.assertEqual(big_file_size, 5001)

    @mock.patch.object(AwsCli, '_create_s3_cmd')
    def test_make_cp_cmd_for_small_file(self, mock_create_s3_cmd):
        # Arrange
        instance = AwsCli(StorageMock())
        instance._config.expected_size_threshold = 5000
        mock_create_s3_cmd.return_value = ['awscli']
        src = self.create_small_test_file()
        bucket_name = "test_bucket"
        dest = "test_dest"

        expected_cmd = ['awscli', 's3', 'cp', 'small_file.txt', 's3://test_bucket/test_dest']
        # Act
        actual_cmd = instance._make_cp_cmd(src, bucket_name, dest)
        # Assert
        self.assertEqual(expected_cmd, actual_cmd)

    @mock.patch.object(AwsCli, '_create_s3_cmd')
    def test_make_cp_cmd_for_big_file(self, mock_create_s3_cmd):
        # Arrange
        instance = AwsCli(StorageMock())
        instance._config.expected_size_threshold = 5000
        mock_create_s3_cmd.return_value = ['awscli']
        src = self.create_big_test_file()
        bucket_name = "test_bucket"
        dest = "test_dest"
        actual_size = instance.get_file_size(src)

        expected_cmd = ['awscli', 's3', 'cp', '--expected-size',
                        str(actual_size), 'big_file.txt', 's3://test_bucket/test_dest']
        # Act
        actual_cmd = instance._make_cp_cmd(src, bucket_name, dest)
        # Assert
        self.assertEqual(expected_cmd, actual_cmd)

    def tearDown(self):
        if os.path.exists('small_file.txt'):
            os.remove('small_file.txt')
        if os.path.exists('big_file.txt'):
            os.remove('big_file.txt')


if __name__ == '__main__':
    unittest.main()
