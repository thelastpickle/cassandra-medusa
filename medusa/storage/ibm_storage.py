# -*- coding: utf-8 -*-
# Copyright 2020 DataStax Inc.
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

import configparser
import io
import logging
import os
import subprocess
from subprocess import PIPE

from dateutil import parser
from medusa.libcloud.storage.drivers.ibm import IBMCloudStorageDriver

from medusa.storage.s3_storage import S3Storage
import medusa.storage.ibm_cloud_storage.concurrent
from medusa.storage.ibm_cloud_storage.awscli import AwsCli


class IBMCloudStorage(S3Storage):

    def connect_storage(self):
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
        elif 'AWS_ACCESS_KEY_ID' in os.environ and \
                'AWS_SECRET_ACCESS_KEY' in os.environ:
            logging.debug("Reading IBM Cloud credentials from Environment Variables.")
            aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
            aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']

        driver = IBMCloudStorageDriver(
            host=self.config.host,
            region=self.config.region,
            key=aws_access_key_id,
            secret=aws_secret_access_key
        )

        if self.config.transfer_max_bandwidth is not None:
            self.set_upload_bandwidth()

        return driver

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
        return medusa.storage.ibm_cloud_storage.concurrent.upload_blobs(
            self,
            srcs,
            dest,
            self.bucket,
            max_workers=self.config.concurrent_transfers,
            multi_part_upload_threshold=int(self.config.multi_part_upload_threshold)
        )

    def download_blobs(self, srcs, dest):
        """
        Downloads a list of files from the remote storage system to the local storage

        :param src: a list of files to download from the remote storage system
        :param dest: the path where to download the objects locally
        :return:
        """
        return medusa.storage.ibm_cloud_storage.concurrent.download_blobs(
            self,
            srcs,
            dest,
            self.bucket,
            max_workers=self.config.concurrent_transfers,
            multi_part_upload_threshold=int(self.config.multi_part_upload_threshold)
        )


def is_aws_s3(storage_name):
    return True


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
