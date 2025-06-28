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
import asyncio
import base64
import collections
import datetime
import hashlib
import io
import logging
import pathlib
import typing as t

from pathlib import Path
from retrying import retry


BLOCK_SIZE_BYTES = 65536
MULTIPART_PART_SIZE_IN_MB = 8
MULTIPART_BLOCK_SIZE_BYTES = 65536
MULTIPART_BLOCKS_PER_MB = 16
MAX_UP_DOWN_LOAD_RETRIES = 5


AbstractBlob = collections.namedtuple('AbstractBlob', ['name', 'size', 'hash', 'last_modified', 'storage_class'])

AbstractBlobMetadata = collections.namedtuple('AbstractBlobMetadata',
                                              ['name', 'sse_enabled', 'sse_key_id', 'sse_customer_key_md5'])

ManifestObject = collections.namedtuple('ManifestObject', ['path', 'size', 'MD5'])


class ObjectDoesNotExistError(Exception):
    pass


class AbstractStorage(abc.ABC):

    # still not certain what precisely this is used for
    # sometimes we store Cassandra version in this it seems
    api_version = None

    def __init__(self, config):
        self.config = config
        self.bucket_name = config.bucket_name

    @abc.abstractmethod
    def connect(self):
        raise NotImplementedError

    @abc.abstractmethod
    def disconnect(self):
        raise NotImplementedError

    @retry(stop_max_attempt_number=7, wait_exponential_multiplier=10000, wait_exponential_max=120000)
    def list_objects(self, path=None):
        # List objects in the bucket/container that have the corresponding prefix (emtpy means all objects)
        logging.debug("[Storage] Listing objects in {}".format(path if path is not None else 'everywhere'))

        objects = self.list_blobs(prefix=path)

        objects = list(filter(lambda blob: blob.size > 0, objects))

        return objects

    @retry(stop_max_attempt_number=7, wait_exponential_multiplier=10000, wait_exponential_max=120000)
    def list_blobs(self, prefix=None):
        loop = self.get_or_create_event_loop()
        objects = loop.run_until_complete(self._list_blobs(prefix))
        return objects

    @abc.abstractmethod
    async def _list_blobs(self, prefix=None):
        raise NotImplementedError()

    def upload_blobs_from_strings(
            self,
            key_content_pairs: t.List[t.Tuple[str, str]],
            encoding='utf-8',
            concurrent_transfers=None
    ):
        loop = self.get_or_create_event_loop()
        loop.run_until_complete(self._upload_blobs_from_strings(key_content_pairs, encoding, concurrent_transfers))

    async def _upload_blobs_from_strings(
            self,
            key_content_pairs: t.List[t.Tuple[str, str]],
            encoding,
            concurrent_transfers
    ):
        # if the concurrent_transfers is not explicitly set, then use the value from the medusa's config
        chunk_size = concurrent_transfers if concurrent_transfers else int(self.config.concurrent_transfers)

        # split key_content_pairs into chunk-sized chunks
        chunks = [key_content_pairs[i:i + chunk_size] for i in range(0, len(key_content_pairs), chunk_size)]
        for chunk in chunks:
            coros = [
                self._upload_object(
                    data=io.BytesIO(bytes(str(pair[1]), encoding)),
                    object_key=pair[0],
                    headers={}
                )
                for pair in chunk
            ]
            await asyncio.gather(*coros)

    @retry(stop_max_attempt_number=7, wait_exponential_multiplier=10000, wait_exponential_max=120000)
    def upload_blob_from_string(self, path, content, encoding="utf-8"):
        headers = self.additional_upload_headers()

        # Upload a string content to the provided path in the bucket
        obj = self.upload_object_via_stream(
            data=io.BytesIO(bytes(content, encoding)),
            object_name=str(path),
            headers=headers,
        )

        return ManifestObject(obj.name, obj.size, obj.hash)

    def upload_object_via_stream(self, data: io.BytesIO, object_name: str, headers: t.Dict[str, str]) -> AbstractBlob:
        loop = self.get_or_create_event_loop()
        o = loop.run_until_complete(self._upload_object(data, object_name, headers))
        return o

    @abc.abstractmethod
    async def _upload_object(self, data: io.BytesIO, object_key: str, headers: t.Dict[str, str]) -> AbstractBlob:
        raise NotImplementedError()

    def download_blobs(self, srcs, dest):
        """
        Downloads a list of files from the remote storage system to the local storage

        :param src: a list of files to download from the remote storage system
        :param dest: the path where to download the objects locally
        :return:
        """
        loop = self.get_or_create_event_loop()
        loop.run_until_complete(self._download_blobs(srcs, dest))

    async def _download_blobs(self, srcs: t.List[t.Union[Path, str]], dest: t.Union[Path, str]):
        chunk_size = int(self.config.concurrent_transfers)
        # split srcs into chunk-sized chunks
        chunks = [srcs[i:i + chunk_size] for i in range(0, len(srcs), chunk_size)]
        for chunk in chunks:
            coros = [self._download_blob(src, dest) for src in map(str, chunk)]
            await asyncio.gather(*coros)

    @abc.abstractmethod
    async def _download_blob(self, src: str, dest: str):
        raise NotImplementedError()

    def upload_blobs(self, srcs: t.List[t.Union[Path, str]], dest: str) -> t.List[ManifestObject]:
        """
        Uploads a list of files from the local storage into the remote storage system
        :param srcs: a list of files to upload
        :param dest: the location where to upload the files in the target bucket (doesn't contain the filename)
        :return: a list of ManifestObject describing all the uploaded files
        """
        loop = self.get_or_create_event_loop()
        manifest_objects = loop.run_until_complete(self._upload_blobs(srcs, dest))
        return manifest_objects

    async def _upload_blobs(self, srcs: t.List[t.Union[Path, str]], dest: str) -> t.List[ManifestObject]:
        coros = [self._upload_blob(src, dest) for src in map(str, srcs)]
        manifest_objects = []
        n = int(self.config.concurrent_transfers)
        for chunk in [coros[i:i + n] for i in range(0, len(coros), n)]:
            manifest_objects += await asyncio.gather(*chunk)
        return manifest_objects

    @abc.abstractmethod
    async def _upload_blob(self, src: str, dest: str) -> ManifestObject:
        raise NotImplementedError()

    @retry(stop_max_attempt_number=7, wait_exponential_multiplier=10000, wait_exponential_max=120000)
    def get_blob(self, path: t.Union[Path, str]):
        try:
            logging.debug("[Storage] Getting object {}".format(path))
            return self.get_object(str(path))
        except ObjectDoesNotExistError:
            return None

    def get_object(self, object_key: t.Union[Path, str]):
        # Doesn't actually read the contents, just lists the thing
        try:
            loop = self.get_or_create_event_loop()
            o = loop.run_until_complete(self._get_object(object_key))
            return o
        except ObjectDoesNotExistError:
            return None

    @abc.abstractmethod
    async def _get_object(self, object_key: t.Union[Path, str]) -> AbstractBlob:
        raise NotImplementedError()

    @retry(stop_max_attempt_number=7, wait_exponential_multiplier=10000, wait_exponential_max=120000)
    def get_blob_content_as_string(self, path: t.Union[Path, str]):
        blob = self.get_blob(str(path))
        if blob is None:
            return None
        return self.read_blob_as_string(blob)

    @retry(stop_max_attempt_number=7, wait_exponential_multiplier=10000, wait_exponential_max=120000)
    def get_blob_content_as_bytes(self, path: t.Union[Path, str]):
        blob = self.get_blob(str(path))
        return self.read_blob_as_bytes(blob)

    def read_blob_as_string(self, blob: AbstractBlob, encoding="utf-8") -> str:
        return self.read_blob_as_bytes(blob).decode(encoding)

    def read_blob_as_bytes(self, blob: AbstractBlob) -> bytes:
        logging.debug("[Storage] Reading blob {}...".format(blob.name))
        loop = self.get_or_create_event_loop()
        b = loop.run_until_complete(self._read_blob_as_bytes(blob))
        return b

    @abc.abstractmethod
    async def _read_blob_as_bytes(self, blob: AbstractBlob) -> bytes:
        raise NotImplementedError()

    def get_object_datetime(self, blob: AbstractBlob) -> datetime.datetime:
        logging.debug(
            "Blob {} last modification time is {}".format(
                blob.name, blob.last_modified
            )
        )
        return blob.last_modified

    @staticmethod
    def hashes_match(manifest_hash, object_hash):
        return base64.b64decode(manifest_hash).hex() == str(object_hash) or manifest_hash == str(object_hash)

    @staticmethod
    def path_maybe_with_parent(dest: str, src_path: Path) -> str:
        """
        Works out which path to download or upload a file into.
        @param dest : path to a directory where we'll be placing the file into
        @param src_path :  full path of a file (or object) we are read
        @returns : full path of the file or object we write

        Medusa generally expects SSTables which reside in .../keyspace/table/ (this is where dest points to)
        But in some cases, we have exceptions:
            - secondary indexes are stored in whatever/data_folder/keyspace/table/.index_name/,
              so we need to include the index name in the destination path
            - DSE metadata in resides in whatever/metadata where there is a `nodes` folder only (DSE 6.8)
              in principle, this is just like a 2i file structure, so we reuse all the other logic
        """
        if src_path.parent.name.startswith(".") or src_path.parent.name.endswith('nodes'):
            # secondary index file or a DSE metadata file
            return "{}/{}/{}".format(dest, src_path.parent.name, src_path.name)
        else:
            # regular SSTable
            return "{}/{}".format(dest, src_path.name)

    def get_path_prefix(self, path=None) -> str:
        return ""

    def get_cache_path(self, path) -> str:
        # Full path for files that will be taken from previous backups
        return path

    def get_download_path(self, path) -> str:
        return path

    def delete_object(self, object: AbstractBlob):
        loop = self.get_or_create_event_loop()
        loop.run_until_complete(self._delete_object(object))

    def delete_objects(self, objects: t.List[AbstractBlob], concurrent_transfers: int = None):
        loop = self.get_or_create_event_loop()
        loop.run_until_complete(self._delete_objects(objects, concurrent_transfers))

    async def _delete_objects(self, objects: t.List[AbstractBlob], concurrent_transfers: int = None):

        # if we get the concurrent_transfers provided, use those instead of the ones from the config
        chunk_size = concurrent_transfers if concurrent_transfers else int(self.config.concurrent_transfers)

        # split objects into chunk-sized chunks
        chunks = [objects[i:i + chunk_size] for i in range(0, len(objects), chunk_size)]
        for chunk in chunks:
            coros = [self._delete_object(obj) for obj in chunk]
            await asyncio.gather(*coros)

    @abc.abstractmethod
    async def _delete_object(self, obj: AbstractBlob):
        raise NotImplementedError()

    @retry(stop_max_attempt_number=5, wait_fixed=5000)
    def get_blobs_metadata(self, blob_keys):
        loop = self.get_or_create_event_loop()
        return loop.run_until_complete(self._get_blobs_metadata(blob_keys))

    @retry(stop_max_attempt_number=5, wait_fixed=5000)
    def get_blob_metadata(self, blob_key: str) -> AbstractBlobMetadata:
        loop = self.get_or_create_event_loop()
        return loop.run_until_complete(self._get_blob_metadata(blob_key))

    async def _get_blobs_metadata(self, blob_keys: t.List[str]) -> t.List[AbstractBlobMetadata]:
        chunk_size = self.config.concurrent_transfers
        # split blob_keys into chunk-sized chunks
        chunks = [blob_keys[i:i + chunk_size] for i in range(0, len(blob_keys), chunk_size)]
        r = []
        for chunk in chunks:
            coros = [self._get_blob_metadata(blob_key) for blob_key in chunk]
            r.append(*await asyncio.gather(*coros))
        return r

    async def _get_blob_metadata(self, blob_key: str) -> AbstractBlobMetadata:
        # Only S3 really implements this because of the KMS support
        # Other storage providers don't do that for now
        return AbstractBlobMetadata(blob_key, False, None, None)

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
        for _ in range(MULTIPART_PART_SIZE_IN_MB * MULTIPART_BLOCKS_PER_MB):
            chunk = f.read(MULTIPART_BLOCK_SIZE_BYTES)
            if chunk == b'':
                eof = True
                break
            hash_md5.update(chunk)
        return hash_md5.digest(), eof

    @staticmethod
    def get_or_create_event_loop() -> asyncio.AbstractEventLoop:
        try:
            loop = asyncio.get_event_loop()
        except Exception:
            loop = None
        if loop is None or loop.is_closed():
            logging.warning("Having to make a new event loop unexpectedly")
            new_loop = asyncio.new_event_loop()
            if new_loop.is_closed():
                logging.error("Even the new event loop was not running, bailing out")
                raise RuntimeError("Could not create a new event loop")
            asyncio.set_event_loop(new_loop)
            return new_loop
        return loop

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
    def file_matches_storage(src: pathlib.Path, cached_item: ManifestObject, threshold=None, enable_md5_checks=False):
        """
        Compares a local file with its version in the storage backend. This happens when doing an actual backup.

        This method is expected to take care of actually computing the local hash, but leave the actual comparing to
        _compare_blob_with_manifest().

        :param src: typically, local file that comes as a Path
        :param cached_item: a reference to the storage, should be via a manifest object
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

    def additional_upload_headers(self):
        """
        Additional HTTP headers to be passed during upload operations. To be overriden by
        child classes.
        """
        return {}

    def get_storage_class(self):
        if self.config.storage_class is not None:
            return self.config.storage_class.upper()
        else:
            return None

    @staticmethod
    def human_readable_size(size, decimal_places=3):
        for unit in ["B", "KiB", "MiB", "GiB", "TiB"]:
            if size < 1024.0:
                break
            size /= 1024.0
        return '{:.{}f}{}'.format(size, decimal_places, unit)

    @staticmethod
    def _human_size_to_bytes(size_str: str) -> int:
        multipliers = [
            ('PB', 1024 ** 5),
            ('TB', 1024 ** 4),
            ('GB', 1024 ** 3),
            ('MB', 1024 ** 2),
            ('KB', 1024),
            ('B', 1),
        ]

        cleaned_size_str = size_str.replace(' ', '').replace('/s', '')

        for unit, multiplier in multipliers:
            if cleaned_size_str.endswith(unit):
                size = float(cleaned_size_str.rstrip(unit))
                return int(size * multiplier)

        raise ValueError(f"Invalid human-friendly size format: {size_str}")
