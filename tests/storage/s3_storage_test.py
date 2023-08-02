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
import os
import unittest
import tempfile

from aiohttp_s3_client.credentials import StaticCredentials, ConfigCredentials, EnvironmentCredentials
from aiohttp_s3_client.credentials import merge_credentials
from pathlib import Path

from libcloud.storage.providers import get_driver


class S3StorageTest(unittest.TestCase):

    def test_legacy_provider_region_replacement(self):
        assert get_driver("s3_us_west_oregon").region_name == "us-west-2"

    def test_static_credentials(self):
        credentials = StaticCredentials(
            access_key_id='dummy-key-id',
            secret_access_key='dummy-key-secret',
            session_token='dummy-token',
            region='dummy-region',
        )
        # assert each field is set correctly
        self.assertEqual('dummy-key-id', credentials.access_key_id)
        self.assertEqual('dummy-key-secret', credentials.secret_access_key)
        self.assertEqual('dummy-token', credentials.session_token)
        self.assertEqual('dummy-region', credentials.region)

    def test_config_credentials(self):
        credentials_content = """
        [default]
        aws_access_key_id = foo
        aws_secret_access_key = bar
        """
        credentials = _make_credentials(credentials_content, profile='default')
        # assert each field is set correctly
        self.assertEqual('foo', credentials.access_key_id)
        self.assertEqual('bar', credentials.secret_access_key)
        self.assertEqual(None, credentials.session_token)
        self.assertEqual('', credentials.region)

    def test_config_credentials_with_profile(self):
        credentials_content = """
        [default]
        aws_access_key_id = foo
        aws_secret_access_key = bar
        region = baz

        [test_profile]
        aws_access_key_id = foo2
        aws_secret_access_key = bar2
        region = not_used_region
        """
        credentials = _make_credentials(
            credentials_content,
            region='also_not_used_region',
            profile='test_profile'
        )
        self.assertEqual('foo2', credentials.access_key_id)
        self.assertEqual('bar2', credentials.secret_access_key)
        self.assertEqual(None, credentials.session_token)
        self.assertEqual('', credentials.region)

    def test_config_credentials_with_profile_and_config(self):

        credentials_content = """
        [default]
        aws_access_key_id = foo
        aws_secret_access_key = bar
        region = baz

        [test_profile]
        aws_access_key_id = foo2
        aws_secret_access_key = bar2
        region = not_used_region
        """

        config_content = """
        [default]
        region = default_region

        [test_profile]
        region = test_profile_region
        """

        credentials = _make_credentials_with_config(
            credentials_content,
            config_content,
            region='not_used_region',
            profile='test_profile'
        )
        self.assertEqual('foo2', credentials.access_key_id)
        self.assertEqual('bar2', credentials.secret_access_key)
        self.assertEqual(None, credentials.session_token)
        self.assertEqual('test_profile_region', credentials.region)

    def test_environment_credentials(self):
        # set env variables
        os.environ['AWS_ACCESS_KEY_ID'] = 'env_key_id'
        os.environ['AWS_SECRET_ACCESS_KEY'] = 'env_secret_key'
        os.environ['AWS_SESSION_TOKEN'] = 'env_session_token'
        os.environ['AWS_DEFAULT_REGION'] = 'env_region'

        credentials = EnvironmentCredentials()
        self.assertEqual('env_key_id', credentials.access_key_id)
        self.assertEqual('env_secret_key', credentials.secret_access_key)
        self.assertEqual('env_session_token', credentials.session_token)
        self.assertEqual('env_region', credentials.region)

    def test_merge_credentials(self):
        credentials_content = """
        [default]
        aws_access_key_id = foo
        aws_secret_access_key = bar
        region = baz

        [test_profile]
        aws_access_key_id = test_profile_key
        aws_secret_access_key = test_profile_secret
        region = not_used_region
        """

        config_content = """
        [default]
        region = default_region

        [test_profile]
        region = test_profile_region
        """

        config_credentials = _make_credentials_with_config(
            credentials_content,
            config_content,
            region='not_used_region',
            profile='test_profile'
        )

        os.environ['AWS_ACCESS_KEY_ID'] = 'env_key_id'
        os.environ['AWS_SECRET_ACCESS_KEY'] = 'env_secret_key'
        os.environ['AWS_SESSION_TOKEN'] = 'env_session_token'
        # prevent using region from previous test
        del(os.environ['AWS_DEFAULT_REGION'])
        env_credentials = EnvironmentCredentials()

        merged_credentials = merge_credentials(env_credentials, config_credentials)
        self.assertEqual('env_key_id', merged_credentials.access_key_id)
        self.assertEqual('env_secret_key', merged_credentials.secret_access_key)
        self.assertEqual('env_session_token', merged_credentials.session_token)
        # taken from the profile, because we actually specified the profile
        self.assertEqual('test_profile_region', merged_credentials.region)

    def test_merge_credentials_without_profile(self):
        credentials_content = """
        [default]
        aws_access_key_id = config_file_key
        aws_secret_access_key = config_file_secret
        region = not_used_region
        """
        config_credentials = _make_credentials(
            credentials_content,
            region='also_not_used_region',
            profile='default'
        )
        os.environ['AWS_ACCESS_KEY_ID'] = 'env_key_id'
        os.environ['AWS_SECRET_ACCESS_KEY'] = 'env_secret_key'
        os.environ['AWS_SESSION_TOKEN'] = 'env_session_token'
        os.environ['AWS_DEFAULT_REGION'] = 'env_region'
        env_credentials = EnvironmentCredentials()
        merged_credentials = merge_credentials(config_credentials, env_credentials)
        self.assertEqual('config_file_key', merged_credentials.access_key_id)
        # we intentionally skipped the secret in the env, so it should be taken from the config file
        self.assertEqual('config_file_secret', merged_credentials.secret_access_key)
        # taken from env because we did not set it anywhere before
        self.assertEqual('env_session_token', merged_credentials.session_token)
        # taken from the env, because region from credentials is not taken and there's no config
        self.assertEqual('env_region', merged_credentials.region)

    def test_config_credentials_without_config(self):
        credentials_content = """
        [default]
        aws_access_key_id = config_file_key
        aws_secret_access_key = config_file_secret
        """
        config_credentials = _make_credentials(
            credentials_content,
        )
        # did not populate key in the credentials
        # because it looked for the config file, which did not exist, so it fell back to empty static credentials
        self.assertEqual('', config_credentials.access_key_id)

        config_content = """
        [default]
        foo = bar
        """

        better_credentials = _make_credentials_with_config(
            credentials_content,
            config_content,
            profile='default'
        )
        # now the key got populated because config file exists AND we pick a profile
        self.assertEqual('config_file_key', better_credentials.access_key_id)
        # region is unset because it's not in the config file
        self.assertEqual('', better_credentials.region)


def _make_credentials_with_config(
        credentials_file_content: str,
        config_file_content: str,
        region: str = None,
        profile: str = None,
) -> ConfigCredentials:
    # write the credentials to a temporary file
    with tempfile.NamedTemporaryFile() as conf_f:
        conf_f.write(config_file_content.encode('utf-8'))
        conf_f.flush()
        # create the credentials from the file
        credentials = _make_credentials(
            file_content=credentials_file_content,
            region=region,
            profile=profile,
            config_path=conf_f.name,
        )
        return credentials


def _make_credentials(
        file_content: str,
        region: str = None,
        profile: str = None,
        config_path: str = None,
) -> ConfigCredentials:

    # touch the config file if it does not exist, needed by GHA
    if config_path is None:
        (Path.home() / Path('.aws')).mkdir(exist_ok=True)
        (Path.home() / Path('.aws/config')).touch()

    # write the credentials to a temporary file
    with tempfile.NamedTemporaryFile() as cred_f:
        cred_f.write(file_content.encode('utf-8'))
        cred_f.flush()
        # create the credentials from the file
        credentials = ConfigCredentials(
            credentials_path=cred_f.name,
            region=region,
            profile=profile,
            config_path=config_path,
        )
        return credentials
