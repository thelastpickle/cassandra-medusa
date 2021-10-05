# -*- coding: utf-8 -*-
# Copyright 2019 Spotify AB
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
import sys
import requests
import configparser
import logging
import os
import io

from libcloud.storage.providers import get_driver, Provider

from medusa.storage.s3_base_storage import S3BaseStorage


class S3Storage(S3BaseStorage):
    def __init__(self, config):
        try:
            imds_token = requests.put("http://169.254.169.254/latest/api/token",
                                      headers={"X-aws-ec2-metadata-token-ttl-seconds": "21600"},
                                      timeout=2)
            self.imds_headers = None if imds_token.status_code != 200 else {
                "X-aws-ec2-metadata-token": imds_token.text}
        except Exception as e:
            logging.info(
                'Could not use imdsv2: \
got an exception while reaching http://169.254.169.254/latest/api/token: {}' .format(str(e)))
            self.imds_headers = None
        super().__init__(config)

    def get_aws_instance_profile(self):
        """
        Get IAM Role from EC2
        """
        logging.debug('Getting IAM Role:')
        try:
            aws_instance_profile = requests.get("http://169.254.169.254/latest/meta-data/iam/security-credentials",
                                                timeout=10, headers=self.imds_headers)
        except requests.exceptions.RequestException:
            logging.warn("Can't fetch IAM Role.")
            return None

        if aws_instance_profile.status_code != 200:
            logging.debug("IAM Role not found.")
            return None
        else:
            return aws_instance_profile

    def connect_storage(self):
        """
        Connects to AWS S3 storage using EC2 driver

        :return driver: EC2 driver object
        """
        aws_session_token = ''
        aws_access_key_id = None
        # or authentication via AWS credentials file
        if self.config.key_file and os.path.exists(os.path.expanduser(self.config.key_file)):
            logging.debug("Reading AWS credentials from {}".format(
                self.config.key_file
            ))

            aws_config = configparser.ConfigParser(interpolation=None)
            with io.open(os.path.expanduser(self.config.key_file), 'r', encoding='utf-8') as aws_file:
                aws_config.read_file(aws_file)
                aws_profile = self.config.api_profile
                profile = aws_config[aws_profile]
                aws_access_key_id = profile['aws_access_key_id']
                aws_secret_access_key = profile['aws_secret_access_key']
                if 'aws_session_token' in profile:
                    aws_session_token = profile['aws_session_token']
        # Authentication via environment variables
        elif 'AWS_ACCESS_KEY_ID' in os.environ and \
                'AWS_SECRET_ACCESS_KEY' in os.environ:
            logging.debug("Reading AWS credentials from Environment Variables:")
            aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
            aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']

            # Access token for credentials fetched from STS service:
            # AWS_SECURITY_TOKEN has been renamed AWS_SESSION_TOKEN so we need to support both
            if 'AWS_SESSION_TOKEN' in os.environ:
                aws_session_token = os.environ['AWS_SESSION_TOKEN']
            elif 'AWS_SECURITY_TOKEN' in os.environ:
                aws_session_token = os.environ['AWS_SECURITY_TOKEN']

        # or authentication via IAM Role credentials
        else:
            aws_instance_profile = self.get_aws_instance_profile()
            if aws_instance_profile:
                logging.debug('Reading AWS credentials from IAM Role: %s', aws_instance_profile.text)
                url = "http://169.254.169.254/latest/meta-data/iam/security-credentials/" + aws_instance_profile.text
                try:
                    auth_data = requests.get(url, headers=self.imds_headers).json()
                except requests.exceptions.RequestException:
                    logging.error('Can\'t fetch AWS IAM Role credentials.')
                    sys.exit(1)

                aws_access_key_id = auth_data['AccessKeyId']
                aws_secret_access_key = auth_data['SecretAccessKey']
                aws_session_token = auth_data['Token']

        if aws_access_key_id is None:
            raise NotImplementedError("No valid method of AWS authentication provided.")

        cls = get_driver(Provider.S3)
        region = self.config.region
        if self.config.storage_provider != Provider.S3:
            region = get_driver(self.config.storage_provider).region_name
        driver = cls(
            aws_access_key_id, aws_secret_access_key, token=aws_session_token, region=region
        )

        if self.config.transfer_max_bandwidth is not None:
            self.set_upload_bandwidth()

        return driver
