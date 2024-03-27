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
import pathlib

from medusa.storage.abstract_storage import ManifestObject
from medusa.storage.s3_base_storage import S3BaseStorage
from medusa.storage.s3_storage import S3Storage


class S3RGWStorage(S3BaseStorage):

    def get_cache_path(self, path):
        # Full path for files that will be taken from previous backups
        return path

    @staticmethod
    def blob_matches_manifest(blob, object_in_manifest, enable_md5_checks=False):
        return S3Storage.blob_matches_manifest(blob, object_in_manifest, enable_md5_checks)

    @staticmethod
    def file_matches_storage(src: pathlib.Path, cached_item: ManifestObject, threshold=None, enable_md5_checks=False):
        # for S3RGW, we never set threshold so the S3's multipart never happens
        return S3Storage.file_matches_storage(src, cached_item, None, enable_md5_checks)

    @staticmethod
    def compare_with_manifest(actual_size, size_in_manifest, actual_hash=None, hash_in_manifest=None, threshold=None):
        return S3Storage.compare_with_manifest(actual_size, size_in_manifest, actual_hash, hash_in_manifest, None)
