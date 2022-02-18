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
from libcloud.storage.providers import get_driver, Provider

from medusa.storage.s3_base_storage import S3BaseStorage


class S3Storage(S3BaseStorage):

    def connect_storage(self):
        """
        Connects to AWS S3 storage

        :return driver: S3 driver object
        """

        credentials = self.session.get_credentials()

        cls = get_driver(Provider.S3)
        driver = cls(
            credentials.access_key,
            credentials.secret_key,
            token=credentials.token,
            region=self.session.get_config_variable('region'),
        )

        if self.config.transfer_max_bandwidth is not None:
            self.set_upload_bandwidth()

        return driver
