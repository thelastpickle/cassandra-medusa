# -*- coding: utf-8 -*-
# Copyright 2021 DataStax, Inc.
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
import datetime
import json
import os
import unittest
import tempfile

from unittest.mock import patch, MagicMock, mock_open
import botocore.utils

from medusa.storage.s3_base_storage import S3BaseStorage
from tests.storage.abstract_storage_test import AttributeDict


class S3StorageTest(unittest.TestCase):
    original_call = None

    def setUp(self):
        self.original_call = botocore.utils.FileWebIdentityTokenLoader.__call__

    def tearDown(self):
        botocore.utils.FileWebIdentityTokenLoader.__call__ = self.original_call

    def test_legacy_provider_region_replacement(self):
        assert S3BaseStorage._region_from_provider_name("s3_us_west_oregon") == "us-west-2"

    def test_credentials_from_metadata(self):

        with patch('botocore.httpsession.URLLib3Session', return_value=_make_instance_metadata_mock()):
            # make an empty temp file to pass as an unconfigured key_file
            with tempfile.NamedTemporaryFile() as empty_file:

                if os.environ.get('AWS_ACCESS_KEY_ID', None):
                    del(os.environ['AWS_ACCESS_KEY_ID'])
                if os.environ.get('AWS_SECRET_ACCESS_KEY', None):
                    del(os.environ['AWS_SECRET_ACCESS_KEY'])
                if os.environ.get('AWS_PROFILE', None):
                    del(os.environ['AWS_PROFILE'])
                if os.environ.get('AWS_STS_REGIONAL_ENDPOINTS', None):
                    del(os.environ['AWS_STS_REGIONAL_ENDPOINTS'])
                if os.environ.get('AWS_DEFAULT_REGION', None):
                    del(os.environ['AWS_DEFAULT_REGION'])
                if os.environ.get('AWS_REGION', None):
                    del(os.environ['AWS_REGION'])
                if os.environ.get('AWS_ROLE_ARN', None):
                    del(os.environ['AWS_ROLE_ARN'])
                if os.environ.get('AWS_WEB_IDENTITY_TOKEN_FILE', None):
                    del(os.environ['AWS_WEB_IDENTITY_TOKEN_FILE'])

                self.assertIsNone(os.environ.get('AWS_ACCESS_KEY_ID', None))
                self.assertIsNone(os.environ.get('AWS_SECRET_ACCESS_KEY', None))
                self.assertIsNone(os.environ.get('AWS_PROFILE', None))

                config = AttributeDict({
                    'api_profile': None,
                    'region': 'region-from-config',
                    'storage_provider': 's3_us_west_oregon',
                    'key_file': empty_file.name,
                    'concurrent_transfers': '1'
                })

                credentials = S3BaseStorage._consolidate_credentials(config)
                self.assertEqual('key-from-instance-metadata', credentials.access_key_id)
                self.assertEqual('region-from-config', credentials.region)

    def test_credentials_from_env_without_profile(self):
        with tempfile.NamedTemporaryFile() as empty_file:

            os.environ['AWS_ACCESS_KEY_ID'] = 'key-from-env'
            os.environ['AWS_SECRET_ACCESS_KEY'] = 'secret-from-env'

            config = AttributeDict({
                'api_profile': None,
                'region': 'region-from-config',
                'storage_provider': 's3_us_west_oregon',
                'key_file': empty_file.name,
                'concurrent_transfers': '1'
            })

            credentials = S3BaseStorage._consolidate_credentials(config)
            self.assertEqual('key-from-env', credentials.access_key_id)
            self.assertEqual('region-from-config', credentials.region)

            del(os.environ['AWS_ACCESS_KEY_ID'])
            del(os.environ['AWS_SECRET_ACCESS_KEY'])

    def test_credentials_from_file(self):
        credentials_file_content = """
        [default]
        aws_access_key_id = key-from-file
        aws_secret_access_key = secret-from-file
        """
        with tempfile.NamedTemporaryFile() as credentials_file:
            credentials_file.write(credentials_file_content.encode())
            credentials_file.flush()

            # make sure we have clean env
            self.assertIsNone(os.environ.get('AWS_ACCESS_KEY_ID', None))
            self.assertIsNone(os.environ.get('AWS_SECRET_ACCESS_KEY', None))

            config = AttributeDict({
                'api_profile': 'default',
                'region': 'region-from-config',
                'storage_provider': 's3_us_west_oregon',
                'key_file': credentials_file.name,
                'concurrent_transfers': '1'
            })

            credentials = S3BaseStorage._consolidate_credentials(config)
            self.assertEqual('key-from-file', credentials.access_key_id)
            self.assertEqual('region-from-config', credentials.region)

    def test_credentials_from_everything(self):
        credentials_file_content = """
        [test-profile]
        aws_access_key_id = key-from-file
        aws_secret_access_key = secret-from-file
        """
        with patch('botocore.httpsession.URLLib3Session', return_value=_make_instance_metadata_mock()):
            # make an empty temp file to pass as an unconfigured key_file
            with tempfile.NamedTemporaryFile() as credentials_file:
                credentials_file.write(credentials_file_content.encode())
                credentials_file.flush()

                os.environ['AWS_ACCESS_KEY_ID'] = 'key-from-env'
                os.environ['AWS_SECRET_ACCESS_KEY'] = 'secret-from-env'

                config = AttributeDict({
                    'api_profile': 'test-profile',
                    'region': 'region-from-config',
                    'storage_provider': 's3_us_west_oregon',
                    'key_file': credentials_file.name,
                    'concurrent_transfers': '1'
                })

                credentials = S3BaseStorage._consolidate_credentials(config)
                self.assertEqual('key-from-file', credentials.access_key_id)

                del(os.environ['AWS_ACCESS_KEY_ID'])
                del(os.environ['AWS_SECRET_ACCESS_KEY'])

    def test_credentials_with_default_region(self):
        credentials_file_content = """
        [default]
        aws_access_key_id = key-from-file
        aws_secret_access_key = secret-from-file
        """
        with tempfile.NamedTemporaryFile() as credentials_file:
            credentials_file.write(credentials_file_content.encode())
            credentials_file.flush()

            # make sure we have clean env
            self.assertIsNone(os.environ.get('AWS_ACCESS_KEY_ID', None))
            self.assertIsNone(os.environ.get('AWS_SECRET_ACCESS_KEY', None))

            config = AttributeDict({
                'api_profile': 'default',
                'region': 'default',
                'storage_provider': 's3_us_west_oregon',
                'key_file': credentials_file.name,
                'concurrent_transfers': '1'
            })

            credentials = S3BaseStorage._consolidate_credentials(config)
            self.assertEqual('key-from-file', credentials.access_key_id)
            self.assertEqual('us-west-2', credentials.region)

    def test_credentials_with_default_region_and_s3_compatible_storage(self):
        credentials_file_content = """
        [default]
        aws_access_key_id = key-from-file
        aws_secret_access_key = secret-from-file
        """
        with tempfile.NamedTemporaryFile() as credentials_file:
            credentials_file.write(credentials_file_content.encode())
            credentials_file.flush()

            # make sure we have clean env
            self.assertIsNone(os.environ.get('AWS_ACCESS_KEY_ID', None))
            self.assertIsNone(os.environ.get('AWS_SECRET_ACCESS_KEY', None))

            config = AttributeDict({
                'api_profile': 'default',
                'region': 'default',
                'storage_provider': 's3_compatible',
                'key_file': credentials_file.name,
                'concurrent_transfers': '1'
            })

            credentials = S3BaseStorage._consolidate_credentials(config)
            self.assertEqual('key-from-file', credentials.access_key_id)
            # default AWS region
            self.assertEqual('us-east-1', credentials.region)

    def test_make_s3_url(self):
        with patch('botocore.httpsession.URLLib3Session', return_value=_make_instance_metadata_mock()):
            with tempfile.NamedTemporaryFile() as empty_file:
                config = AttributeDict({
                    'storage_provider': 's3_us_west_oregon',
                    'region': 'default',
                    'key_file': empty_file.name,
                    'api_profile': None,
                    'kms_id': None,
                    'transfer_max_bandwidth': None,
                    'bucket_name': 'whatever-bucket',
                    'secure': 'True',
                    'ssl_verify': 'False',
                    'host': None,
                    'port': None,
                    'concurrent_transfers': '1',
                    'multipart_chunksize': '5MB'
                })
                s3_storage = S3BaseStorage(config)
                # there are no extra connection args when connecting to regular S3
                self.assertEqual(
                    dict(),
                    s3_storage.connection_extra_args
                )

    def test_make_s3_url_without_secure(self):
        with patch('botocore.httpsession.URLLib3Session', return_value=_make_instance_metadata_mock()):
            with tempfile.NamedTemporaryFile() as empty_file:
                config = AttributeDict({
                    'storage_provider': 's3_us_west_oregon',
                    'region': 'default',
                    'key_file': empty_file.name,
                    'api_profile': None,
                    'kms_id': None,
                    'transfer_max_bandwidth': None,
                    'bucket_name': 'whatever-bucket',
                    'secure': 'False',
                    'ssl_verify': 'False',
                    'host': None,
                    'port': None,
                    'concurrent_transfers': '1',
                    'multipart_chunksize': '5MB'
                })
                s3_storage = S3BaseStorage(config)
                # again, no extra connection args when connecting to regular S3
                # we can't even disable HTTPS
                self.assertEqual(
                    dict(),
                    s3_storage.connection_extra_args
                )

    def test_make_s3_compatible_url(self):
        with patch('botocore.httpsession.URLLib3Session', return_value=_make_instance_metadata_mock()):
            with tempfile.NamedTemporaryFile() as empty_file:
                config = AttributeDict({
                    'storage_provider': 's3_compatible',
                    'region': 'default',
                    'key_file': empty_file.name,
                    'api_profile': None,
                    'kms_id': None,
                    'transfer_max_bandwidth': None,
                    'bucket_name': 'whatever-bucket',
                    'secure': 'True',
                    'ssl_verify': 'False',
                    'host': 's3.example.com',
                    'port': '443',
                    'concurrent_transfers': '1',
                    'multipart_chunksize': '5MB'
                })
                s3_storage = S3BaseStorage(config)
                self.assertEqual(
                    'https://s3.example.com:443',
                    s3_storage.connection_extra_args['endpoint_url']
                )

    def test_make_s3_compatible_url_without_secure(self):
        with patch('botocore.httpsession.URLLib3Session', return_value=_make_instance_metadata_mock()):
            with tempfile.NamedTemporaryFile() as empty_file:
                config = AttributeDict({
                    'storage_provider': 's3_compatible',
                    'region': 'default',
                    'key_file': empty_file.name,
                    'api_profile': None,
                    'kms_id': None,
                    'transfer_max_bandwidth': None,
                    'bucket_name': 'whatever-bucket',
                    'secure': 'False',
                    'ssl_verify': 'False',
                    'host': 's3.example.com',
                    'port': '8080',
                    'concurrent_transfers': '1',
                    'multipart_chunksize': '5MB'
                })
                s3_storage = S3BaseStorage(config)
                self.assertEqual(
                    'http://s3.example.com:8080',
                    s3_storage.connection_extra_args['endpoint_url']
                )

    def test_make_connection_arguments_without_ssl_verify(self):
        with patch('botocore.httpsession.URLLib3Session', return_value=_make_instance_metadata_mock()):
            config = AttributeDict({
                'storage_provider': 's3_compatible',
                'region': 'default',
                'key_file': '/tmp/whatever',
                'api_profile': None,
                'kms_id': None,
                'transfer_max_bandwidth': None,
                'bucket_name': 'whatever-bucket',
                'secure': 'False',
                'ssl_verify': 'False',
                'host': 's3.example.com',
                'port': '8080',
                'concurrent_transfers': '1',
                'multipart_chunksize': '5MB'
            })
            s3_storage = S3BaseStorage(config)
            connection_args = s3_storage._make_connection_arguments(config)
            self.assertEqual(False, connection_args['verify'])

    def test_make_connection_arguments_with_ssl_verify(self):
        with patch('botocore.httpsession.URLLib3Session', return_value=_make_instance_metadata_mock()):
            config = AttributeDict({
                'storage_provider': 's3_compatible',
                'region': 'default',
                'key_file': '/tmp/whatever',
                'api_profile': None,
                'kms_id': None,
                'transfer_max_bandwidth': None,
                'bucket_name': 'whatever-bucket',
                'secure': 'False',
                'ssl_verify': 'True',
                'host': 's3.example.com',
                'port': '8080',
                'concurrent_transfers': '1',
                'multipart_chunksize': '5MB'
            })
            s3_storage = S3BaseStorage(config)
            connection_args = s3_storage._make_connection_arguments(config)
            self.assertEqual(True, connection_args['verify'])

    def test_assume_role_authentication(self):
        with patch('botocore.httpsession.URLLib3Session', new=_make_assume_role_with_web_identity_mock()):
            if os.environ.get('AWS_ACCESS_KEY_ID', None):
                del(os.environ['AWS_ACCESS_KEY_ID'])
            if os.environ.get('AWS_SECRET_ACCESS_KEY', None):
                del(os.environ['AWS_SECRET_ACCESS_KEY'])
            if os.environ.get('AWS_PROFILE', None):
                del(os.environ['AWS_PROFILE'])

            self.assertIsNone(os.environ.get('AWS_ACCESS_KEY_ID', None))
            self.assertIsNone(os.environ.get('AWS_SECRET_ACCESS_KEY', None))
            self.assertIsNone(os.environ.get('AWS_PROFILE', None))

            os.environ['AWS_STS_REGIONAL_ENDPOINTS'] = 'regional'
            os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'
            os.environ['AWS_REGION'] = 'us-east-1'
            os.environ['AWS_ROLE_ARN'] = 'arn:aws:iam::123456789012:role/testRole'

            os.environ['AWS_WEB_IDENTITY_TOKEN_FILE'] = '/var/run/secrets/token'

            # Create a mock file with the token
            mock_file_content = 'eyJh...'
            mock_call = mock_open(read_data=mock_file_content)
            config = AttributeDict({
                'storage_provider': 's3_us_west_oregon',
                'region': 'default',
                'key_file': '',
                'api_profile': None,
                'kms_id': None,
                'transfer_max_bandwidth': None,
                'bucket_name': 'whatever-bucket',
                'secure': 'True',
                'ssl_verify': 'False',
                'host': None,
                'port': None,
                'concurrent_transfers': '1'
            })

            # Replace the open function with the mock
            with patch('builtins.open', mock_call):
                credentials = S3BaseStorage._consolidate_credentials(config)

            self.assertEqual(None, credentials.access_key_id)
            self.assertEqual(None, credentials.secret_access_key)


def _make_instance_metadata_mock():
    # mock a call to the metadata service
    mock_response = MagicMock()
    mock_response.status_code = 200
    in_one_hour = datetime.datetime.utcnow() + datetime.timedelta(hours=1)
    mock_response.text = json.dumps({
        "AccessKeyId": 'key-from-instance-metadata',
        "SecretAccessKey": 'secret-from-instance-metadata',
        "Token": 'token-from-metadata',
        "Expiration": in_one_hour.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'  # -3 to remove microseconds
    })
    mock_send = MagicMock(return_value=mock_response)
    mock_session = MagicMock()
    mock_session.send = mock_send
    return mock_session


def _make_assume_role_with_web_identity_mock():
    # mock a call to the AssumeRoleWithWebIdentity endpoint
    mock_response = MagicMock()
    mock_response.status_code = 200
    in_one_hour = datetime.datetime.utcnow() + datetime.timedelta(hours=1)
    mock_response.text = json.dumps({
        "AccessKeyId": 'key-from-assume-role',
        "SecretAccessKey": 'secret-from-assume-role',
        "SessionToken": 'token-from-assume-role',
        "Expiration": in_one_hour.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'  # -3 to remove microseconds
    })
    return MagicMock(return_value=mock_response)
