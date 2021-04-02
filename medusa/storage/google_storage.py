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
import io
import itertools
import json
import logging
import os
import subprocess
from subprocess import PIPE

from dateutil import parser
from libcloud.storage.drivers.google_storage import GoogleStorageDriver
from pathlib import Path

from medusa.storage.abstract_storage import AbstractStorage
from medusa.storage.google_cloud_storage.gsutil import GSUtil


GSUTIL_MAX_FILES_PER_CHUNK = 64


class GoogleStorage(AbstractStorage):

    def connect_storage(self):
        with io.open(os.path.expanduser(self.config.key_file), 'r', encoding='utf-8') as json_fi:
            credentials = json.load(json_fi)

        driver = GoogleStorageDriver(
            key=credentials['client_email'],
            secret=credentials['private_key'],
            project=credentials['project_id']
        )

        return driver

    def check_dependencies(self):
        try:
            subprocess.check_call(["gsutil", "help"], stdout=PIPE, stderr=PIPE)
        except Exception:
            raise RuntimeError(
                "Google Cloud SDK doesn't seem to be installed on this system and is a "
                + "required dependency for the GCS backend. "
                + "Please check https://cloud.google.com/sdk/docs/quickstarts for installation guidelines."
            )

    def upload_blobs(self, srcs, dest):

        # ensure srcs is always a list
        if isinstance(srcs, str) or isinstance(srcs, Path):
            srcs = [srcs]

        generators = self._upload_blobs(srcs, dest)
        return list(itertools.chain(*generators))

    def _upload_blobs(self, srcs, dest):
        with GSUtil(self.config) as gsutil:
            for parent, src_paths in _group_by_parent(srcs):
                yield self._upload_paths(gsutil, parent, src_paths, dest)

    def _upload_paths(self, gsutil, parent, src_paths, old_dest):
        # if the parent doesn't start with a '.', it's probably business as usual
        # if it doesn't we need to modify the dest by squeezing the new parent in
        # so the final upload destination is `dest / parent / object`
        # this is needed to handle things like secondary indices that live in hidden folders within table folders
        new_dest = '{}/{}'.format(old_dest, parent) if parent.startswith('.') else old_dest
        return gsutil.cp(
            srcs=src_paths,
            dst="gs://{}/{}".format(self.bucket.name, new_dest),
            parallel_process_count=self.config.concurrent_transfers
        )

    def download_blobs(self, srcs, dest):

        # src is a list of strings, each string is a path inside the backup bucket pointing to a file, or a GCS URI
        # dst is a table full path to a temporary folder, ends with table name

        # ensure srcs is always a list
        if isinstance(srcs, str) or isinstance(srcs, Path):
            srcs = [srcs]

        generators = self._download_blobs(srcs, dest)
        return list(itertools.chain(*generators))

    def _download_blobs(self, srcs, dest):
        with GSUtil(self.config) as gsutil:
            for parent, src_paths in _group_by_parent(srcs):
                yield self._download_paths(gsutil, parent, src_paths, dest)

    def _download_paths(self, gsutil, parent, src_paths, old_dest):
        new_dest = '{}/{}'.format(old_dest, parent) if parent.startswith('.') else old_dest
        # we made src_paths a list of Path objects, but we need strings for copying
        # plus, we must not forget to point them to the bucket
        srcs = ['gs://{}/{}'.format(self.bucket.name, str(p)) for p in src_paths]
        return gsutil.cp(srcs=srcs, dst=new_dest)

    def get_object_datetime(self, blob):
        logging.debug("Blob {} last modification time is {}".format(blob.name, blob.extra["last_modified"]))
        return parser.parse(blob.extra["last_modified"])

    def get_path_prefix(self, path):
        return ""

    def get_download_path(self, path):
        if "gs://" in path:
            return path
        else:
            return "gs://{}/{}".format(self.bucket.name, path)

    def get_cache_path(self, path):
        # Full path for files that will be taken from previous backups
        return self.get_download_path(path)

    @staticmethod
    def blob_matches_manifest(blob, object_in_manifest, enable_md5_checks=False):
        return GoogleStorage.compare_with_manifest(
            actual_size=blob.size,
            size_in_manifest=object_in_manifest['size'],
            actual_hash=str(blob.hash) if enable_md5_checks else None,
            hash_in_manifest=object_in_manifest['MD5']
        )

    @staticmethod
    def file_matches_cache(src, cached_item, threshold=None, enable_md5_checks=False):
        return GoogleStorage.compare_with_manifest(
            actual_size=src.stat().st_size,
            size_in_manifest=cached_item['size'],
            actual_hash=AbstractStorage.generate_md5_hash(src) if enable_md5_checks else None,
            hash_in_manifest=cached_item['MD5']
        )

    @staticmethod
    def compare_with_manifest(actual_size, size_in_manifest, actual_hash=None, hash_in_manifest=None, threshold=None):
        sizes_match = actual_size == size_in_manifest
        if not actual_hash:
            return sizes_match

        hashes_match = (
            # this case comes from comparing blob hashes to manifest entries (in context of GCS)
            actual_hash == base64.b64decode(hash_in_manifest).hex()
            # this comes from comparing files to a cache
            or hash_in_manifest == base64.b64decode(actual_hash).hex()
            # and perhaps we need the to check for match even without base64 encoding
            or actual_hash == hash_in_manifest
        )

        return sizes_match and hashes_match


def _is_in_folder(file_path, folder_path):
    return file_path.parent.name == Path(folder_path).name


def _group_by_parent(paths):
    by_parent = itertools.groupby(paths, lambda p: Path(p).parent.name)
    for parent, files in by_parent:
        yield parent, list(files)
