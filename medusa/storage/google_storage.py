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

import io
import json
import logging
import os

from dateutil import parser
from libcloud.storage.drivers.google_storage import GoogleStorageDriver

from medusa.storage.abstract_storage import AbstractStorage
from medusa.storage.google_cloud_storage.gsutil import GSUtil


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

    def upload_blobs(self, src, dest):
        with GSUtil(self.config) as gsutil:
            return gsutil.cp(srcs=src, dst="gs://{}/{}".format(self.bucket.name, dest))

    def download_blobs(self, src, dest):
        with GSUtil(self.config) as gsutil:
            src = list(map(lambda name: "gs://{}/{}".format(self.bucket.name, name), src))
            return gsutil.cp(srcs=src, dst=dest)

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
