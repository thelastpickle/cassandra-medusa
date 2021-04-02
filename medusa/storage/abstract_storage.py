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
import hashlib
import io
import logging

from libcloud.storage.types import ObjectDoesNotExistError
from retrying import retry

import medusa.storage
import medusa.storage.concurrent


BLOCK_SIZE_BYTES = 65536
MULTIPART_PART_SIZE_IN_MB = 8
MULTIPART_BLOCK_SIZE_BYTES = 65536
MULTIPART_BLOCKS_PER_MB = 16


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

    @retry(stop_max_attempt_number=7, wait_exponential_multiplier=10000, wait_exponential_max=120000)
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

    def check_dependencies(self):
        """
        Check that required dependencies are available.
        Each child class should implement this function if it relies on an optional dependency.
        """
        pass

    @staticmethod
    def generate_md5_hash(src, block_size=BLOCK_SIZE_BYTES):

        checksum = hashlib.md5()
        with open(str(src), 'rb') as f:
            # Incrementally read data and update the digest
            while True:
                read_data = f.read(block_size)
                if not read_data:
                    break
                checksum.update(read_data)

        # Once we have all the data, compute checksum
        checksum = checksum.digest()
        # Convert into a bytes type that can be base64 encoded
        base64_md5 = base64.encodebytes(checksum).decode('UTF-8').strip()
        # Print the Base64 encoded CRC32C
        return base64_md5

    @staticmethod
    def md5_multipart(src):
        eof = False
        hash_list = []
        with open(str(src), 'rb') as f:
            while eof is False:
                (md5_hash, eof) = AbstractStorage.md5_part(f)
                hash_list.append(md5_hash)

        multipart_hash = hashlib.md5(b''.join(hash_list)).hexdigest()

        return '%s-%d' % (multipart_hash, len(hash_list))

    @staticmethod
    def md5_part(f):
        hash_md5 = hashlib.md5()
        eof = False
        for i in range(MULTIPART_PART_SIZE_IN_MB * MULTIPART_BLOCKS_PER_MB):
            chunk = f.read(MULTIPART_BLOCK_SIZE_BYTES)
            if chunk == b'':
                eof = True
                break
            hash_md5.update(chunk)
        return hash_md5.digest(), eof

    @staticmethod
    @abc.abstractmethod
    def blob_matches_manifest(blob, object_in_manifest, enable_md5_checks=False):
        """
        Compares a blob with its records in the manifest. This happens during backup verification.

        Implementations of this are expected to work out the size of the original blob and its equivalent in
        the manifest. If the storage implementation supports hashes (all but local do), then this function should
        also work out the blob hash, its equivalent in the manifest.

        Ultimately, this should just work out the values, then call _compare_blob_with_manifest() to compare them.

        :param blob: blob from (any) storage
        :param object_in_manifest: a record (dict) from the Medusa manifest
        :param enable_md5_checks: if enabled, calculates the MD5 hash of the file to check file integrity
        :return: boolean informing if the blob matches or not
        """
        pass

    @staticmethod
    @abc.abstractmethod
    def file_matches_cache(src, cached_item, threshold=None, enable_md5_checks=False):
        """
        Compares a local file with its entry in the cache of backed up items. This happens when doing an actual backup.

        This method is expected to take care of actually computing the local hash, but leave the actual comparing to
        _compare_blob_with_manifest().

        :param src: typically, local file that comes as a string/path
        :param cached_item: usually a reference to a item in the storage, mostly a dict. Likely a manifest object
        :param threshold: files bigger than this are digested by chunks
        :param enable_md5_checks: boolean flag to enable md5 file generation and comparison to the md5
                found in the manifest (only applicable to some cloud storage implementations that compare md5 hashes)
        :return: boolean informing if the files match or not
        """
        pass

    @staticmethod
    @abc.abstractmethod
    def compare_with_manifest(actual_size, size_in_manifest, actual_hash=None, hash_in_manifest=None, threshold=None):
        """
        Actually compares the sizes and hashes of a blob. Different implementations do this differently, for example:
        - local storage just compares sizes
        - S3 does multipart hashes if files are too big
        - GCS doesn't do multipart at ll
        :param actual_size: the size of local blob/file
        :param size_in_manifest: the size of the blob/file in the manifest
        :param actual_hash: hash of the local blob/file
        :param hash_in_manifest: hash of the blob/file in the manifest
        :return: boolean informing if the blob matches or not
        """
        pass

    def prepare_download(self):
        # Override for each child class
        pass
