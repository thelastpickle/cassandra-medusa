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

from unittest.mock import patch, MagicMock

from libcloud.storage.providers import get_driver

from medusa.storage.s3_base_storage import S3BaseStorage, DEFAULT_MULTIPART_PARTS_COUNT


class AttributeDict(dict):
    __slots__ = ()
    __getattr__ = dict.__getitem__
    __setattr__ = dict.__setitem__


class S3StorageTest(unittest.TestCase):

    def test_legacy_provider_region_replacement(self):
        assert get_driver("s3_us_west_oregon").region_name == "us-west-2"

    def test_calculate_part_sizes(self):
        size_5mb = 5 * 1024 * 1024
        size_32gb = 32 * 1024 * 1024 * 1024
        part_size, part_count = S3BaseStorage.calculate_part_size(size_32gb)
        self.assertEqual(size_5mb, part_size)
        # 6554 x 5mb
        self.assertEqual(6554, part_count)

    def test_calculate_part_sizes_big_file(self):
        size_512gb = 512 * 1024 * 1024 * 1024
        part_size, part_count = S3BaseStorage.calculate_part_size(size_512gb)
        # part size goes way up if we want to fit to 9500 chunks
        self.assertEqual(57_869_034, part_size)
        self.assertEqual(DEFAULT_MULTIPART_PARTS_COUNT, part_count)

    def test_calculate_part_sizes_file_at_threshold(self):
        size_5mb = 5 * 1024 * 1024
        exact_size = DEFAULT_MULTIPART_PARTS_COUNT * size_5mb

        part_size, part_count = S3BaseStorage.calculate_part_size(exact_size)
        self.assertEqual(size_5mb, part_size)
        self.assertEqual(DEFAULT_MULTIPART_PARTS_COUNT, part_count)

        part_size, part_count = S3BaseStorage.calculate_part_size(exact_size + 1)
        self.assertEqual(5_242_881, part_size)      # 5mb + 1b
        self.assertEqual(DEFAULT_MULTIPART_PARTS_COUNT, part_count)

        # there's 9500 parts, so increasing each by just 1b is enough to accommodate 1023 more bytes
        part_size, part_count = S3BaseStorage.calculate_part_size(exact_size + 1023)
        self.assertEqual(5_242_881, part_size)      # 5mb + 1b
        self.assertEqual(DEFAULT_MULTIPART_PARTS_COUNT, part_count)

        # with 1kb more, we already need bigger pats
        part_size, part_count = S3BaseStorage.calculate_part_size(exact_size + 1024 * 1023)
        self.assertEqual(5_242_991, part_size)
        self.assertEqual(DEFAULT_MULTIPART_PARTS_COUNT, part_count)

    def test_calculate_part_sizes_leading_to_too_big_parts(self):
        size_5mb = 5 * 1024 * 1024
        size_20gb = 20 * 1024 * 1024 * 1024
        size_5gb = 5 * 1024 * 1024 * 1024
        size_2point5gb = 2.5 * 1024 * 1024 * 1024

        # baseline, with a free parts count
        part_size, part_count = S3BaseStorage.calculate_part_size(size_20gb)
        self.assertEqual(size_5mb, part_size)
        self.assertEqual(4096, part_count)

        # baseline, with only 8 parts, we should get 2.5 gb parts
        part_size, part_count = S3BaseStorage.calculate_part_size(size_20gb, max_parts_count=8)
        self.assertEqual(size_2point5gb, part_size)
        self.assertEqual(8, part_count)

        # baseline, with only 4 parts, we should get 5 gb parts, which exactly matches
        part_size, part_count = S3BaseStorage.calculate_part_size(size_20gb, max_parts_count=4)
        self.assertEqual(size_5gb, part_size)
        self.assertEqual(4, part_count)

        # a bit bigger and we're screwed
        self.assertRaises(ValueError, S3BaseStorage.calculate_part_size, size_20gb + 1, max_parts_count=4)

    def test_reproduce_IT_behaviour(self):
        size_40mb = 40 * 1024 * 1024
        part_size, parts_count = S3BaseStorage.calculate_part_size(size_40mb, max_parts_count=5)
        self.assertEqual(8_388_608, part_size)
        self.assertEqual(5, parts_count)

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

                self.assertIsNone(os.environ.get('AWS_ACCESS_KEY_ID', None))
                self.assertIsNone(os.environ.get('AWS_SECRET_ACCESS_KEY', None))
                self.assertIsNone(os.environ.get('AWS_PROFILE', None))

                config = AttributeDict({
                    'api_profile': None,
                    'region': 'region-from-config',
                    'storage_provider': 's3_us_west_oregon',
                    'key_file': empty_file.name,
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
                })

                credentials = S3BaseStorage._consolidate_credentials(config)
                self.assertEqual('key-from-file', credentials.access_key_id)

                del (os.environ['AWS_ACCESS_KEY_ID'])
                del (os.environ['AWS_SECRET_ACCESS_KEY'])

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
            })

            credentials = S3BaseStorage._consolidate_credentials(config)
            self.assertEqual('key-from-file', credentials.access_key_id)
            # default AWS region
            self.assertEqual('us-east-1', credentials.region)


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
