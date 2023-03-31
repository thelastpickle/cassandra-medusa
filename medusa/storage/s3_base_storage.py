# -*- coding: utf-8 -*-
# Copyright 2019 Spotify AB
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
import base64
import logging
import itertools
import subprocess
from subprocess import PIPE
from dateutil import parser
from pathlib import Path

import botocore.session
from libcloud.storage.providers import get_driver, Provider

from medusa.libcloud.storage.drivers.s3_base_driver import S3BaseStorageDriver

from medusa.storage.abstract_storage import AbstractStorage
import medusa.storage.s3_compat_storage.concurrent
from medusa.storage.s3_compat_storage.awscli import AwsCli

import medusa


"""
    S3BaseStorage supports all the S3 compatible storages. Certain providers might override this method
    to implement their own specialities (such as environment variables when running in certain clouds)

    This implementation uses awscli instead of libcloud's s3's driver for uploads/downloads. If you wish
    to use the libcloud's internal driver instead of awscli dependency, select s3_rgw.
"""


class S3BaseStorage(AbstractStorage):

    def __init__(self, config):
        self.session = botocore.session.Session()

        if config.api_profile:
            logging.debug("Using AWS profile {}".format(
                config.api_profile,
            ))
            self.session.set_config_variable('profile', config.api_profile)

        if config.region and config.region != "default":
            self.session.set_config_variable('region', config.region)
        elif config.storage_provider not in [Provider.S3, "s3_compatible"] and config.region == "default":
            self.session.set_config_variable('region', get_driver(config.storage_provider).region_name)

        if config.key_file:
            logging.debug("Setting AWS credentials file to {}".format(
                config.key_file,
            ))
            self.session.set_config_variable('credentials_file', config.key_file)

        if config.kms_id:
            logging.debug("Using KMS key {}".format(
                config.kms_id,
            ))

        super().__init__(config)

    def connect_storage(self):
        credentials = self.session.get_credentials()

        secure = False if self.config.secure is None or self.config.secure.lower() in ('0', 'false') else True
        driver = S3BaseStorageDriver(
            host=self.config.host,
            port=self.config.port,
            key=credentials.access_key,
            secret=credentials.secret_key,
            secure=secure,
            region=self.session.get_config_variable('region'),
        )

        return driver

    def check_dependencies(self):
        if self.config.aws_cli_path == 'dynamic':
            aws_cli_cmd = AwsCli.cmd()
        else:
            aws_cli_cmd = [self.config.aws_cli_path]

        try:
            subprocess.check_call(aws_cli_cmd + ['--version'], stdout=PIPE, stderr=PIPE)
        except Exception:
            raise RuntimeError(
                "AWS cli doesn't seem to be installed on this system and is a "
                + "required dependency for the S3 backend. Please install it by running 'pip install awscli' "
                + "or 'sudo apt-get install awscli' and try again."
            )

    def upload_blobs(self, srcs, dest):
        return medusa.storage.s3_compat_storage.concurrent.upload_blobs(
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
        return medusa.storage.s3_compat_storage.concurrent.download_blobs(
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
    def blob_matches_manifest(blob, object_in_manifest, enable_md5_checks=False):
        return S3BaseStorage.compare_with_manifest(
            actual_size=blob.size,
            size_in_manifest=object_in_manifest['size'],
            actual_hash=str(blob.hash) if enable_md5_checks else None,
            hash_in_manifest=object_in_manifest['MD5']
        )

    @staticmethod
    def file_matches_cache(src, cached_item, threshold=None, enable_md5_checks=False):

        threshold = int(threshold) if threshold else -1

        # single or multi part md5 hash. Used by Azure and S3 uploads.
        if not enable_md5_checks:
            md5_hash = None
        elif src.stat().st_size >= threshold > 0:
            md5_hash = AbstractStorage.md5_multipart(src)
        else:
            md5_hash = AbstractStorage.generate_md5_hash(src)

        return S3BaseStorage.compare_with_manifest(
            actual_size=src.stat().st_size,
            size_in_manifest=cached_item['size'],
            actual_hash=md5_hash,
            hash_in_manifest=cached_item['MD5'],
            threshold=threshold
        )

    @staticmethod
    def compare_with_manifest(actual_size, size_in_manifest, actual_hash=None, hash_in_manifest=None, threshold=None):
        sizes_match = actual_size == size_in_manifest
        if not actual_hash:
            return sizes_match

        # md5 hash comparison
        if not threshold:
            threshold = -1
        else:
            threshold = int(threshold)

        if actual_size >= threshold > 0 or "-" in hash_in_manifest:
            multipart = True
        else:
            multipart = False

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

    def prepare_upload(self):
        if self.config.transfer_max_bandwidth is not None:
            subprocess.check_call(
                [
                    "aws",
                    "configure",
                    "set",
                    "default.s3.max_bandwidth",
                    self.config.transfer_max_bandwidth,
                ]
            )

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

    def additional_upload_headers(self):
        headers = {}
        if self.config.kms_id:
            headers.update({
                "x-amz-server-side-encryption": "aws:kms",
                "x-amz-server-side-encryption-aws-kms-key-id": self.config.kms_id
            })

        return headers


def _group_by_parent(paths):
    by_parent = itertools.groupby(paths, lambda p: Path(p).parent.name)
    for parent, files in by_parent:
        yield parent, list(files)
