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
import itertools
import subprocess
from subprocess import PIPE
from dateutil import parser
from pathlib import Path

from libcloud.storage.providers import get_driver

from medusa.storage.abstract_storage import AbstractStorage
import medusa.storage.aws_s3_storage.concurrent
import medusa


class S3Storage(AbstractStorage):
    """
    Available storage providers for S3:
    S3_AP_NORTHEAST = 's3_ap_northeast'
    S3_AP_NORTHEAST1 = 's3_ap_northeast_1'
    S3_AP_NORTHEAST2 = 's3_ap_northeast_2'
    S3_AP_SOUTH = 's3_ap_south'
    S3_AP_SOUTHEAST = 's3_ap_southeast'
    S3_AP_SOUTHEAST2 = 's3_ap_southeast2'
    S3_CA_CENTRAL = 's3_ca_central'
    S3_CN_NORTH = 's3_cn_north'
    S3_CN_NORTHWEST = 's3_cn_northwest'
    S3_EU_WEST = 's3_eu_west'
    S3_EU_WEST2 = 's3_eu_west_2'
    S3_EU_CENTRAL = 's3_eu_central'
    S3_SA_EAST = 's3_sa_east'
    S3_US_EAST2 = 's3_us_east_2'
    S3_US_WEST = 's3_us_west'
    S3_US_WEST_OREGON = 's3_us_west_oregon'
    S3_US_GOV_WEST = 's3_us_gov_west'
    S3_RGW = 's3_rgw'
    S3_RGW_OUTSCALE = 's3_rgw_outscale'
    """
    def get_aws_instance_profile(self):
        """
        Get IAM Role from EC2
        """
        logging.debug('Getting IAM Role:')
        try:
            aws_instance_profile = requests.get('http://169.254.169.254/latest/meta-data/iam/security-credentials')
        except requests.exceptions.RequestException:
            logging.error('Can\'t fetch IAM Role.')
            sys.exit(1)

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
        aws_security_token = ''
        aws_instance_profile = self.get_aws_instance_profile()

        # Authentication via environment variables
        if 'AWS_ACCESS_KEY_ID' in os.environ and \
                'AWS_SECRET_ACCESS_KEY' in os.environ:
            logging.debug("Reading AWS credentials from Environment Variables:")
            aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
            aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']

            # Access token for credentials fetched from STS service:
            if 'AWS_SECURITY_TOKEN' in os.environ:
                aws_security_token = os.environ['AWS_SECURITY_TOKEN']

        # or authentication via IAM Role credentials
        elif aws_instance_profile:
            logging.debug('Reading AWS credentials from IAM Role: %s', aws_instance_profile.text)
            url = "http://169.254.169.254/latest/meta-data/iam/security-credentials/" + aws_instance_profile.text
            try:
                auth_data = requests.get(url).json()
            except requests.exceptions.RequestException:
                logging.error('Can\'t fetch AWS IAM Role credentials.')
                sys.exit(1)

            aws_access_key_id = auth_data['AccessKeyId']
            aws_secret_access_key = auth_data['SecretAccessKey']
            aws_security_token = auth_data['Token']

        # or authentication via AWS credentials file
        elif self.config.key_file and os.path.exists(self.config.key_file):
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
        else:
            raise NotImplementedError("No valid method of AWS authentication provided.")

        cls = get_driver(self.config.storage_provider)
        driver = cls(
            aws_access_key_id, aws_secret_access_key, token=aws_security_token
        )
        return driver

    def _test_awscli_presence(self):
        try:
            subprocess.check_call(["aws", "help"], stdout=PIPE, stderr=PIPE)
        except Exception:
            raise RuntimeError(
                "AWS cli doesn't seem to be installed on this system and is a "
                + "required dependency for the S3 backend. Please install it by running 'pip install awscli' "
                + "or 'sudo apt-get install awscli' and try again."
            )

    def upload_blobs(self, srcs, dest):
        return medusa.storage.aws_s3_storage.concurrent.upload_blobs(
            self,
            srcs,
            dest,
            self.bucket,
            max_workers=self.config.concurrent_transfers,
            multi_part_upload_threshold=int(self.config.multi_part_upload_threshold),
        )

    def download_blobs(self, srcs, dest):
        """
        Downloads a list of files from the remote storage system to the local storage

        :param src: a list of files to download from the remote storage system
        :param dest: the path where to download the objects locally
        :return:
        """
        return medusa.storage.aws_s3_storage.concurrent.download_blobs(
            self,
            srcs,
            dest,
            self.bucket,
            max_workers=self.config.concurrent_transfers,
            multi_part_upload_threshold=int(self.config.multi_part_upload_threshold),
        )

    def get_object_datetime(self, blob):
        logging.debug(
            "Blob {} last modification time is {}".format(
                blob.name, blob.extra["last_modified"]
            )
        )
        return parser.parse(blob.extra["last_modified"])

    def get_cache_path(self, path):
        # Full path for files that will be taken from previous backups
        return path


def _group_by_parent(paths):
    by_parent = itertools.groupby(paths, lambda p: Path(p).parent.name)
    for parent, files in by_parent:
        yield parent, list(files)


def is_aws_s3(storage_name):
    storage_name = storage_name.lower()
    if storage_name.startswith('s3') and storage_name not in ('s3_rgw', 's3_rgw_outscale'):
        return True
    else:
        return False
