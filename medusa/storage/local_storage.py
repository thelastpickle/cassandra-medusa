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

import datetime
import pathlib

from libcloud.storage.drivers.local import LocalStorageDriver

from medusa.storage.abstract_storage import AbstractStorage


class LocalStorage(AbstractStorage):

    def connect_storage(self):
        driver = LocalStorageDriver(key=self.config.base_path)
        containers = list(map(lambda container: container.name, driver.list_containers()))
        if self.config.bucket_name not in containers:
            driver.create_container(self.config.bucket_name)

        return driver

    def list_objects(self, path=None):
        # List objects in the bucket/container that have the corresponding prefix (emtpy means all objects)
        objects = self.driver.list_container_objects(self.bucket)

        if isinstance(path, pathlib.Path):
            path = str(path)

        if path is not None:
            objects = list(filter(lambda blob: blob.name.startswith(path), objects))

        return objects

    def get_object_datetime(self, blob):
        return datetime.datetime.fromtimestamp(int(blob.extra["modify_time"]))

    def get_cache_path(self, path):
        # Full path for files that will be taken from previous backups
        return "{}/{}/{}".format(self.config.base_path, self.config.bucket_name, path)

    @staticmethod
    def blob_matches_manifest(blob, object_in_manifest, enable_md5_checks=False):
        return LocalStorage.compare_with_manifest(
            actual_size=blob.size,
            size_in_manifest=object_in_manifest['size']
        )

    @staticmethod
    def file_matches_cache(src, cached_item, threshold=None, enable_md5_checks=False):
        return LocalStorage.compare_with_manifest(
            actual_size=src.stat().st_size,
            size_in_manifest=cached_item['size']
        )

    @staticmethod
    def compare_with_manifest(actual_size, size_in_manifest, actual_hash=None, hash_in_manifest=None, threshold=None):
        return actual_size == size_in_manifest
