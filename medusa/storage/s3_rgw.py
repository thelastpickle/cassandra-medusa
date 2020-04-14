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
from libcloud.storage.drivers.rgw import S3RGWStorageDriver

from medusa.storage.abstract_storage import AbstractStorage


class S3RGWStorage(AbstractStorage):

    def connect_storage(self):
        with io.open(os.path.expanduser(self.config.key_file), 'r', encoding='utf-8') as json_fi:
            credentials = json.load(json_fi)

        driver = S3RGWStorageDriver(
            host=self.config.host,
            port=self.config.port,
            region=self.config.region,
            signature_version="4",
            key=credentials['access_key_id'],
            secret=credentials['secret_access_key'],
            secure=False if self.config.secure.lower() in ('0', 'false') else True,
        )

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
