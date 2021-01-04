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
import base64
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
from medusa.storage.aws_s3_storage.awscli import AwsCli

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
            aws_instance_profile = requests.get('http://169.254.169.254/latest/meta-data/iam/security-credentials',
                                                timeout=10)
        except requests.exceptions.RequestException:
            logging.warn('Can\'t fetch IAM Role.')
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
        aws_security_token = ''
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
        # Authentication via environment variables
        elif 'AWS_ACCESS_KEY_ID' in os.environ and \
                'AWS_SECRET_ACCESS_KEY' in os.environ:
            logging.debug("Reading AWS credentials from Environment Variables:")
            aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
            aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']

            # Access token for credentials fetched from STS service:
            if 'AWS_SECURITY_TOKEN' in os.environ:
                aws_security_token = os.environ['AWS_SECURITY_TOKEN']

        # or authentication via IAM Role credentials
        else:
            aws_instance_profile = self.get_aws_instance_profile()
            if aws_instance_profile:
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

        if aws_access_key_id is None:
            raise NotImplementedError("No valid method of AWS authentication provided.")

        cls = get_driver(self.config.storage_provider)
        driver = cls(
            aws_access_key_id, aws_secret_access_key, token=aws_security_token
        )

        if self.config.transfer_max_bandwidth is not None:
            self.set_upload_bandwidth()

        return driver

    def check_dependencies(self):
        if self.config.aws_cli_path == 'dynamic':
            aws_cli_path = AwsCli.find_aws_cli()
        else:
            aws_cli_path = self.config.aws_cli_path

        try:
            subprocess.check_call([aws_cli_path, "help"], stdout=PIPE, stderr=PIPE)
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

    @staticmethod
    def blob_matches_manifest(blob, object_in_manifest):
        return S3Storage.compare_with_manifest(
            actual_size=blob.size,
            size_in_manifest=object_in_manifest['size'],
            actual_hash=str(blob.hash),
            hash_in_manifest=object_in_manifest['MD5']
        )

    @staticmethod
    def file_matches_cache(src, cached_item, threshold=None):

        threshold = int(threshold) if threshold else -1

        # single or multi part md5 hash. Used by S3 uploads.
        if src.stat().st_size >= threshold > 0:
            md5_hash = AbstractStorage.md5_multipart(src)
        else:
            md5_hash = AbstractStorage.generate_md5_hash(src)

        return S3Storage.compare_with_manifest(
            actual_size=src.stat().st_size,
            size_in_manifest=cached_item['size'],
            actual_hash=md5_hash,
            hash_in_manifest=cached_item['MD5'],
            threshold=threshold
        )

    @staticmethod
    def compare_with_manifest(actual_size, size_in_manifest, actual_hash=None, hash_in_manifest=None, threshold=None):

        if not threshold:
            threshold = -1
        else:
            threshold = int(threshold)

        if actual_size >= threshold > 0 or "-" in hash_in_manifest:
            multipart = True
        else:
            multipart = False

        sizes_match = actual_size == size_in_manifest

        if multipart:
            hashes_match = (
                actual_hash == hash_in_manifest
            )
        else:
            hashes_match = (
                actual_hash == base64.b64decode(hash_in_manifest).hex()
                or hash_in_manifest == base64.b64decode(actual_hash).hex()
                or actual_hash == hash_in_manifest
            )

        return sizes_match and hashes_match

    def set_upload_bandwidth(self):
        subprocess.check_call(
            [
                "aws",
                "configure",
                "set",
                "default.s3.max_bandwidth",
                self.config.transfer_max_bandwidth,
            ]
        )


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


def prepare_download(self):
    # Unthrottle downloads to speed up restores
    subprocess.check_call(
        [
            "aws",
            "configure",
            "set",
            "default.s3.max_bandwidth",
            "512MB/s",
        ]
    )
