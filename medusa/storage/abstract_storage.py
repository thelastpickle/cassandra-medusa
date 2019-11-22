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

import abc
import base64
import io
import logging

from libcloud.storage.types import ObjectDoesNotExistError
from retrying import retry

import medusa.storage
import medusa.storage.concurrent


class AbstractStorage(abc.ABC):

    def __init__(self, config):
        self.config = config
        self.driver = self.connect_storage()
        self.bucket = self.driver.get_container(container_name=config.bucket_name)

    @abc.abstractmethod
    def connect_storage(self):
        # Override for each child class
        pass

    @retry(stop_max_attempt_number=7, wait_exponential_multiplier=10000, wait_exponential_max=120000)
    def list_objects(self, path=None):
        # List objects in the bucket/container that have the corresponding prefix (emtpy means all objects)
        logging.debug("[Storage] Listing objects in {}".format(path if path is not None else 'everywhere'))

        if path is None:
            objects = self.driver.list_container_objects(self.bucket)
        else:
            objects = self.driver.list_container_objects(self.bucket, ex_prefix=str(path))

        return objects

    @retry(stop_max_attempt_number=7, wait_exponential_multiplier=10000, wait_exponential_max=120000)
    def upload_blob_from_string(self, path, content, encoding="utf-8"):
        # Upload a string content to the provided path in the bucket
        obj = self.driver.upload_object_via_stream(
            io.BytesIO(bytes(content, encoding)),
            container=self.bucket,
            object_name=str(path)
        )
        return medusa.storage.ManifestObject(obj.name, obj.size, obj.hash)

    def download_blobs(self, srcs, dest):
        """
        Downloads a list of files from the remote storage system to the local storage

        :param src: a list of files to download from the remote storage system
        :param dest: the path where to download the objects locally
        :return:
        """
        medusa.storage.concurrent.download_blobs(self, srcs, dest, self.bucket.name,
                                                 max_workers=self.config.concurrent_transfers)

    def upload_blobs(self, src, dest):
        """
        Uploads a list of files from the local storage into the remote storage system
        :param src: a list of files to upload
        :param dest: the location where to upload the files in the target bucket (doesn't contain the filename)
        :return: a list of ManifestObject describing all the uploaded files
        """
        return medusa.storage.concurrent.upload_blobs(self, src, dest, self.bucket,
                                                      max_workers=self.config.concurrent_transfers)

    def get_blob(self, path):
        try:
            logging.debug("[Storage] Getting object {}".format(path))
            return self.driver.get_object(self.bucket.name, str(path))
        except ObjectDoesNotExistError:
            return None

    @retry(stop_max_attempt_number=7, wait_exponential_multiplier=10000, wait_exponential_max=120000)
    def get_blob_content_as_string(self, path):
        blob = self.get_blob(str(path))
        if blob is None:
            return None
        return self.read_blob_as_string(blob)

    def get_blob_content_as_bytes(self, path):
        blob = self.get_blob(str(path))
        return self.read_blob_as_bytes(blob)

    def read_blob_as_string(self, blob, encoding="utf-8"):
        logging.debug("[Storage] Reading blob {}...".format(blob.name))
        return self.read_blob_as_bytes(blob).decode(encoding)

    @abc.abstractmethod
    def get_object_datetime(self, blob):
        pass

    @staticmethod
    def read_blob_as_bytes(blob):
        logging.debug("[Storage] Reading blob {}...".format(blob.name))
        buffer = io.BytesIO()
        stream = blob.as_stream()

        for chunk in stream:
            buffer.write(chunk)

        return buffer.getvalue()

    @staticmethod
    def hashes_match(manifest_hash, object_hash):
        return base64.b64decode(manifest_hash).hex() == str(object_hash) or manifest_hash == str(object_hash)

    def get_path_prefix(self, path):
        return ""

    @abc.abstractmethod
    def get_cache_path(self, path):
        # Full path for files that will be taken from previous backups
        pass

    def get_download_path(self, path):
        return path

    @retry(stop_max_attempt_number=7, wait_exponential_multiplier=10000, wait_exponential_max=120000)
    def delete_object(self, object):
        self.driver.delete_object(object)
