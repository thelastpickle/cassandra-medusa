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

from libcloud.storage.drivers.minio import MinIOStorageDriver

from medusa.storage.abstract_storage import AbstractStorage
import medusa.storage.s3_compat.concurrent
from medusa.storage.s3_compat.awscli import AwsCli

import medusa


"""
    S3BaseStorage supports all the S3 compatible storages. Certain providers might override this method
    to implement their own specialities (such as environment variables when running in certain clouds)

    This implementation uses awscli instead of libcloud's s3's driver for uploads/downloads. If you wish
    to use the libcloud's internal driver instead of awscli dependency, select s3_rgw.
"""
class S3BaseStorage(AbstractStorage):

    def connect_storage(self):
        with io.open(os.path.expanduser(self.config.key_file), 'r', encoding='utf-8') as json_fi:
            credentials = json.load(json_fi)

        # MinIOStorageDriver is the only clean implementation of BaseS3StorageDriver in libcloud
        driver = MinIOStorageDriver(
            host=self.config.host,
            port=self.config.port,
            key=credentials['access_key_id'],
            secret=credentials['secret_access_key'],
            secure=False if self.config.secure.lower() in ('0', 'false') else True,
        )
        
        if self.config.host is not None:
            self.config.endpoint_url = 'https://{}:{}'.format(self.config.host, self.config.port) if self.config.port is not None else 'https://{}'.format(self.config.host)

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
        return medusa.storage.s3_compact.concurrent.upload_blobs(
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
        return medusa.storage.s3_compat.concurrent.download_blobs(
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


def _group_by_parent(paths):
    by_parent = itertools.groupby(paths, lambda p: Path(p).parent.name)
    for parent, files in by_parent:
        yield parent, list(files)
