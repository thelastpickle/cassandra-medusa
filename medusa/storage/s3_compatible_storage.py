# -*- coding: utf-8 -*-
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

import logging

import libcloud.security

from medusa import utils
from medusa.storage.storage_provider import StorageProvider
from medusa.storage.s3_base_storage import S3BaseStorage


class S3CompatibleStorage(S3BaseStorage):
    def __init__(self, config):
        if utils.evaluate_boolean(config.secure) and config.storage_provider == StorageProvider.S3_COMPATIBLE:
            if config.cert_file:
                libcloud.security.CA_CERTS_PATH = config.cert_file
            else:
                logging.warning('No certificate path was supplied. '
                                'Disabling certificate verification for all connections to S3 storage host.')
                libcloud.security.VERIFY_SSL_CERT = False

        super(S3CompatibleStorage, self).__init__(config)
