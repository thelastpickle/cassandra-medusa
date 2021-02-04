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

from medusa.libcloud.storage.drivers.ibm import IBMCloudStorageDriver
from medusa.libcloud.storage.drivers.ibm import IBM_CLOUD_HOSTS_BY_REGION
from medusa.storage.s3_base_storage import S3BaseStorage


class IBMCloudStorage(S3BaseStorage):

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

        if self.config.host is None and self.config.region is not None:
            self.config.host = IBM_CLOUD_HOSTS_BY_REGION[self.config.region]

        return driver
