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
import configparser
import logging
import os
import io
from dateutil import parser

from libcloud.storage.providers import get_driver

from medusa.storage.abstract_storage import AbstractStorage


class S3Storage(AbstractStorage):
    """
    Available storage providers for S3:
    S3_AP_NORTHEAST = 's3_ap_northeast'
    S3_AP_NORTHEAST1 = 's3_ap_northeast_1'
    S3_AP_NORTHEAST2 = 's3_ap_northeast_2'
    S3_AP_SOUTH = 's3_ap_south'
    S3_AP_SOUTHEAST = 's3_ap_southeast'
    S3_AP_SOUTHEAST2 = 's3_ap_southeast2'
    S3_CA_CENTRAL = 's3_ca_central'
    S3_CN_NORTH = 's3_cn_north'
    S3_CN_NORTHWEST = 's3_cn_northwest'
    S3_EU_WEST = 's3_eu_west'
    S3_EU_WEST2 = 's3_eu_west_2'
    S3_EU_CENTRAL = 's3_eu_central'
    S3_SA_EAST = 's3_sa_east'
    S3_US_EAST2 = 's3_us_east_2'
    S3_US_WEST = 's3_us_west'
    S3_US_WEST_OREGON = 's3_us_west_oregon'
    S3_US_GOV_WEST = 's3_us_gov_west'
    S3_RGW = 's3_rgw'
    S3_RGW_OUTSCALE = 's3_rgw_outscale'
    """
    def connect_storage(self):
        aws_config = configparser.ConfigParser(interpolation=None)
        with io.open(os.path.expanduser(self.config.key_file), 'r', encoding='utf-8') as aws_file:
            aws_config.read_file(aws_file)
            aws_profile = self.config.api_profile
            profile = aws_config[aws_profile]
            cls = get_driver(self.config.storage_provider)
            driver = cls(profile['aws_access_key_id'], profile['aws_secret_access_key'])
            return driver

    def download_blobs(self, src, dest):
        """
        Downloads a list of files from the remote storage system to the local storage

        :param src: a list of files to download from the remote storage system
        :param dest: the path where to download the objects locally
        :return:
        """
        for src_obj in list(src):
            blob = self.get_blob(src_obj)
            index = src_obj.rfind('/')
            if index > 0:
                file_name = src_obj[src_obj.rfind('/') + 1:]
            else:
                file_name = src_obj
            blob.download(os.path.join(dest, file_name), overwrite_existing=True)

    def get_object_datetime(self, blob):
        logging.debug("Blob {} last modification time is {}".format(blob.name, blob.extra["last_modified"]))
        return parser.parse(blob.extra["last_modified"])

    def get_cache_path(self, path):
        # Full path for files that will be taken from previous backups
        return path
