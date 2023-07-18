import os
import unittest
from unittest import mock
from unittest.mock import MagicMock
from medusa.storage.s3_compat_storage.awscli import AwsCli


class StorageMock:
    def __init__(self):
        self.config = MagicMock()
        self.config.kms_id = None
        self.config.secure = True  # Set the secure attribute to return a boolean


class TestAwsCli(unittest.TestCase):

    def test_get_file_size(self):
        # Arrange
        with open('small_file.txt', 'w') as small_file:
            small_file.write('a' * 83886080)
        with open('big_file.txt', 'w') as big_file:
            big_file.write('a' * 8388608080)
        instance = AwsCli(StorageMock())

        # Act
        small_file_size = instance.get_file_size('small_file.txt')
        big_file_size = instance.get_file_size('big_file.txt')

        # Assert
        self.assertEqual(small_file_size, 83886080)
        self.assertEqual(big_file_size, 8388608080)

    @mock.patch.object(AwsCli, '_create_s3_cmd',
                       return_value=['s3', 'cp', 'small_file.txt', 's3://my_bucket/my_dest',
                                     '--expected-size', '83886080'])
    @mock.patch.object(AwsCli, '_env', new_callable=mock.PropertyMock, return_value='whatever you want')
    def test_cp_upload(self, mock_create_s3_cmd):
        # Arrange
        instance = AwsCli(StorageMock())
        with open('small_file.txt', 'w') as small_file:
            small_file.write('a' * 83886080)
        small_file_size = 83886080
        bucket_name = 'my_bucket'
        dest = 'my_dest'
        srcs = ['small_file.txt']
        expected_small_file_cmd = ['s3', 'cp', 'small_file.txt', 's3://my_bucket/my_dest',
                                   '--expected-size', str(small_file_size)]

        # Act
        instance.cp_upload(srcs=srcs, bucket_name=bucket_name, dest=dest, max_retries=5)

        # Assert
        self.assertEqual(mock_create_s3_cmd.return_value, expected_small_file_cmd)

    def tearDown(self):
        if os.path.exists('small_file.txt'):
            os.remove('small_file.txt')
        if os.path.exists('big_file.txt'):
            os.remove('big_file.txt')


if __name__ == '__main__':
    unittest.main()
