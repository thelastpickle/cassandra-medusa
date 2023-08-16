import os
import unittest
from unittest import mock
from unittest.mock import MagicMock

import click
from medusa.medusacli import validate_backup_name
from medusa.storage.s3_compat_storage.awscli import AwsCli


class StorageMock:
    def __init__(self):
        self.config = MagicMock()
        self.config.kms_id = None
        self.config.secure = True


class TestAwsCli(unittest.TestCase):

    def create_test_file(self):
        with open('test_file.txt', 'w') as test_file:
            test_file.write('a' * 5001)
        return 'test_file.txt'

    def test_get_file_size(self):
        # Arrange
        instance = AwsCli(StorageMock())

        # Act
        test_file_size = instance.get_file_size(self.create_test_file())

        # Assert
        self.assertEqual(test_file_size, 5001)

    @mock.patch.object(AwsCli, '_create_s3_cmd')
    def test_make_cp_cmd(self, mock_create_s3_cmd):
        # Arrange
        instance = AwsCli(StorageMock())
        mock_create_s3_cmd.return_value = ['awscli']
        src = self.create_test_file()
        bucket_name = "test_bucket"
        dest = "test_dest"
        actual_size = instance.get_file_size(src)

        expected_cmd = ['awscli', 's3', 'cp', '--expected-size',
                        str(actual_size), 'test_file.txt', 's3://test_bucket/test_dest']
        # Act
        actual_cmd = instance._make_cp_cmd(src, bucket_name, dest)
        # Assert
        self.assertEqual(expected_cmd, actual_cmd)

    def tearDown(self):
        if os.path.exists('test_file.txt'):
            os.remove('test_file.txt')

    def test_validate_backup_name_with_slash(self):
        """
        Test that validate_backup_name raises an error when the value contains a "/"
        """
        ctx = None
        param = None
        value = "invalid/name"

        with self.assertRaises(click.BadParameter) as context:
            validate_backup_name(ctx, param, value)

        self.assertEqual(str(context.exception), 'Backup name cannot contain "/". Please use a valid name.')

    def test_validate_backup_name_without_slash(self):
        """
        Test that validate_backup_name returns the value unchanged when it does not contain a "/"
        """
        ctx = None
        param = None
        value = "validname"

        result = validate_backup_name(ctx, param, value)
        self.assertEqual(result, "validname")


if __name__ == '__main__':
    unittest.main()
