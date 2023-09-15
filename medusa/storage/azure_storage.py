# -*- coding: utf-8 -*-
# Copyright 2019 Spotify AB
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

import asyncio
import base64
import collections
import datetime
import io
import json
import logging
import os
import typing as t

from azure.core.credentials import AzureNamedKeyCredential
from azure.storage.blob.aio import BlobServiceClient
from azure.storage.blob import BlobProperties
from libcloud.storage.types import ObjectDoesNotExistError
from medusa.storage.abstract_storage import AbstractStorage, AbstractBlob, AbstractBlobMetadata
from pathlib import Path
from tenacity import retry, stop_after_attempt, wait_fixed


ManifestObject = collections.namedtuple('ManifestObject', ['path', 'size', 'MD5'])

MAX_UP_DOWN_LOAD_RETRIES = 5


class AzureStorage(AbstractStorage):

    api_version = '2006-03-01'

    def __init__(self, config):

        credentials_file = Path(config.key_file).expanduser()
        with open(credentials_file, "r") as f:
            credentials_dict = json.loads(f.read())
            self.credentials = AzureNamedKeyCredential(
                name=credentials_dict["storage_account"],
                key=credentials_dict["key"]
            )
        self.account_name = self.credentials.named_key.name
        self.bucket_name = config.bucket_name

        self.azure_blob_service = BlobServiceClient(
            account_url=f"https://{self.account_name}.blob.core.windows.net/",
            credential=self.credentials
        )
        self.azure_container_client = self.azure_blob_service.get_container_client(self.bucket_name)

        # disable chatty loggers
        logging.getLogger('azure.core.pipeline.policies.http_logging_policy').setLevel(logging.WARNING)
        logging.getLogger('chardet.universaldetector').setLevel(logging.WARNING)

        super().__init__(config)

    def connect_storage(self):
        # TODO
        # Until we get rid of libcloud from the other providers, we need to pretend to be a driver
        return self

    def get_container(self, container_name):
        # TODO
        # Another libcloud compatibility method
        return self

    def name(self):
        # TODO
        # needed for libcloud compatibility, will trash later
        return 'azure_storage'

    def list_container_objects(self, container_name, ex_prefix: t.Optional[str] = None) -> t.List[AbstractBlob]:
        blobs = self.list_blobs(prefix=ex_prefix)
        return blobs

    def check_dependencies(self):
        pass

    def read_blob_as_string(self, blob: AbstractBlob, encoding: t.Optional[str] = 'utf-8') -> str:
        return self.read_blob_as_bytes(blob).decode(encoding)

    def read_blob_as_bytes(self, blob: AbstractBlob) -> bytes:
        logging.debug("[Azure Storage] Reading blob {}...".format(blob.name))
        loop = self._get_or_create_event_loop()
        b = loop.run_until_complete(self._read_blob_as_bytes(blob))
        return b

    async def _read_blob_as_bytes(self, blob: AbstractBlob) -> bytes:
        downloader = await self.azure_container_client.download_blob(
            blob=blob.name,
            max_concurrency=1,
        )
        return await downloader.readall()

    def list_blobs(self, prefix: t.Optional[str] = None) -> t.List[AbstractBlob]:
        loop = self._get_or_create_event_loop()
        objects = loop.run_until_complete(self._list_blobs(prefix))
        return objects

    async def _list_blobs(self, prefix: t.Optional[str] = None) -> t.List[AbstractBlob]:
        blobs = []
        async for b_props in self.azure_container_client.list_blobs(name_starts_with=prefix):
            blobs.append(AbstractBlob(
                b_props.name,
                b_props.size,
                self._get_blob_hash(b_props),
                b_props.last_modified)
            )
        return blobs

    def _get_blob_hash(self, bp: BlobProperties) -> str:
        md5_hash = bp.get('content_settings', {}).get('content_md5', bp.etag)
        if md5_hash == bp.etag:
            md5_hash_str = md5_hash.replace('"', '')
        elif md5_hash is None:
            md5_hash_str = 'no-md5-hash'
        else:
            md5_hash_str = base64.encodebytes(md5_hash).decode('UTF-8').strip()
        return md5_hash_str

    def get_object(self, bucket_name, object_key: str) -> t.Optional[AbstractBlob]:
        # Doesn't actually read the contents, just lists the thing
        try:
            loop = self._get_or_create_event_loop()
            o = loop.run_until_complete(self._get_object(object_key))
            return o
        except ObjectDoesNotExistError:
            return None

    async def _get_object(self, object_key: str) -> AbstractBlob:
        blob = await self._stat_blob(object_key)
        return blob

    def upload_blobs(self, srcs: t.List[t.Union[Path, str]], dest: str) -> t.List[ManifestObject]:
        loop = self._get_or_create_event_loop()
        manifest_objects = loop.run_until_complete(self._upload_blobs(srcs, dest))
        return manifest_objects

    async def _upload_blobs(self, srcs: t.List[t.Union[Path, str]], dest: str) -> t.List[ManifestObject]:
        coros = [self._upload_blob(src, dest) for src in map(str, srcs)]
        manifest_objects = []
        n = int(self.config.concurrent_transfers)
        for chunk in [coros[i:i + n] for i in range(0, len(coros), n)]:
            manifest_objects += await asyncio.gather(*chunk)
        return manifest_objects

    @retry(stop=stop_after_attempt(MAX_UP_DOWN_LOAD_RETRIES), wait=wait_fixed(5000))
    async def _upload_blob(self, src: str, dest: str) -> ManifestObject:
        src_chunks = src.split('/')
        parent_name, file_name = src_chunks[-2], src_chunks[-1]

        # check if objects resides in a sub-folder (e.g. secondary index). if it does, use the sub-folder in object path
        object_key = (
            "{}/{}/{}".format(dest, parent_name, file_name)
            if parent_name.startswith(".")
            else "{}/{}".format(dest, file_name)
        )

        file_size = os.stat(src).st_size
        logging.debug(
            '[Azure Storage] Uploading {} ({}) -> azure://{}/{}'.format(
                src, _human_readable_size(file_size), self.config.bucket_name, object_key
            )
        )

        with open(src, "rb") as data:
            blob_client = await self.azure_container_client.upload_blob(
                name=object_key,
                data=data,
                overwrite=True,
                max_concurrency=4,
            )
        blob_properties = await blob_client.get_blob_properties()
        mo = ManifestObject(
            blob_properties.name,
            blob_properties.size,
            self._get_blob_hash(blob_properties),
        )
        return mo

    def upload_object_via_stream(self, data: io.BytesIO, container, object_name: str, headers) -> AbstractBlob:
        loop = self._get_or_create_event_loop()
        o = loop.run_until_complete(self._upload_object(data, object_name, headers))
        return o

    async def _upload_object(self, data: io.BytesIO, object_key: str, headers: t.Dict[str, str]) -> AbstractBlob:
        logging.debug(
            '[Azure Storage] Uploading object from stream -> azure://{}/{}'.format(
                self.config.bucket_name, object_key
            )
        )
        blob_client = await self.azure_container_client.upload_blob(
            name=object_key,
            data=data,
            overwrite=True,
        )
        blob_properties = await blob_client.get_blob_properties()
        return AbstractBlob(
            blob_properties.name,
            blob_properties.size,
            self._get_blob_hash(blob_properties),
            blob_properties.last_modified,
        )

    async def _stat_blob(self, object_key: str) -> AbstractBlob:

        blob_client = self.azure_container_client.get_blob_client(object_key)

        if not await blob_client.exists():
            raise ObjectDoesNotExistError(
                value='Object {} does not exist'.format(object_key),
                driver=self,
                object_name=object_key
            )

        blob_properties = await blob_client.get_blob_properties()
        return AbstractBlob(
            blob_properties.name,
            blob_properties.size,
            self._get_blob_hash(blob_properties),
            blob_properties.last_modified,
        )

    def delete_object(self, obj: AbstractBlob):
        loop = self._get_or_create_event_loop()
        loop.run_until_complete(self._delete_object(obj))

    def delete_objects(self, objects: t.List[AbstractBlob]):
        loop = self._get_or_create_event_loop()
        loop.run_until_complete(self._delete_objects(objects))

    async def _delete_objects(self, objects: t.List[AbstractBlob]):
        coros = [self._delete_object(obj) for obj in objects]
        await asyncio.gather(*coros)

    async def _delete_object(self, obj: AbstractBlob):
        await self.azure_container_client.delete_blob(obj.name, delete_snapshots='include')

    @retry(stop=stop_after_attempt(MAX_UP_DOWN_LOAD_RETRIES), wait=wait_fixed(5000))
    def get_blob_metadata(self, blob_key: str) -> AbstractBlobMetadata:
        loop = self._get_or_create_event_loop()
        return loop.run_until_complete(self._get_blob_metadata(blob_key))

    async def _get_blobs_metadata(self, blob_keys: t.List[str]) -> t.List[AbstractBlobMetadata]:
        coros = [self._get_blob_metadata(blob_key) for blob_key in blob_keys]
        return await asyncio.gather(*coros)

    async def _get_blob_metadata(self, blob_key: str) -> AbstractBlobMetadata:
        # blob_client = self.azure_container_client.get_blob_client(blob_key)
        # blob_properties = await blob_client.get_blob_properties()
        # if we ever start to support KMS in Azure, here we'd have to check for the encryption key id
        sse_enabled = False
        sse_key_id = None
        return AbstractBlobMetadata(blob_key, sse_enabled, sse_key_id)

    def download_blobs(self, srcs: t.List[t.Union[Path, str]], dest: t.Union[Path, str]):
        loop = self._get_or_create_event_loop()
        loop.run_until_complete(self._download_blobs(srcs, dest))

    async def _download_blobs(self, srcs: t.List[t.Union[Path, str]], dest: t.Union[Path, str]):
        coros = [self._download_blob(src, dest) for src in map(str, srcs)]
        await asyncio.gather(*coros)

    @retry(stop=stop_after_attempt(MAX_UP_DOWN_LOAD_RETRIES), wait=wait_fixed(5000))
    async def _download_blob(self, src: str, dest: str):

        # _stat_blob throws if the blob does not exist
        blob = await self._stat_blob(src)
        object_key = blob.name

        # we must make sure the blob gets stored under sub-folder (if there is any)
        # the dest variable only points to the table folder, so we need to add the sub-folder
        src_path = Path(src)
        file_path = (
            "{}/{}/{}".format(dest, src_path.parent.name, src_path.name)
            if src_path.parent.name.startswith(".")
            else "{}/{}".format(dest, src_path.name)
        )

        if blob.size < int(self.config.multi_part_upload_threshold):
            workers = 1
        else:
            workers = int(self.config.concurrent_transfers)

        logging.debug(
            '[Azure Storage] Downloading with {} workers: {}/{} -> {}'.format(
                workers, self.config.bucket_name, object_key, file_path
            )
        )

        downloader = await self.azure_container_client.download_blob(
            blob=object_key,
            max_concurrency=workers,
        )
        await downloader.readinto(open(file_path, "wb"))

    def get_object_datetime(self, blob: AbstractBlob) -> datetime.datetime:
        logging.debug(
            "Blob {} last modification time is {}".format(
                blob.name, blob.last_modified
            )
        )
        return blob.last_modified

    def get_cache_path(self, path: str) -> str:
        logging.debug(
            "My cache path is {}".format(path)
        )
        return path

    @staticmethod
    def blob_matches_manifest(blob, object_in_manifest, enable_md5_checks=False):
        # Azure use hashed timespan in eTag header. It changes everytime
        # when the file is overwrote. "content-md5" is the right hash to
        # validate the file.
        return AzureStorage.compare_with_manifest(
            actual_size=blob.size,
            size_in_manifest=object_in_manifest['size'],
            actual_hash=blob.hash if enable_md5_checks else None,
            hash_in_manifest=object_in_manifest['MD5']
        )

    @staticmethod
    def file_matches_cache(src, cached_item, threshold=None, enable_md5_checks=False):
        return AzureStorage.compare_with_manifest(
            actual_size=src.stat().st_size,
            size_in_manifest=cached_item['size'],
            actual_hash=AbstractStorage.generate_md5_hash(src) if enable_md5_checks else None,
            hash_in_manifest=cached_item['MD5'],
        )

    @staticmethod
    def compare_with_manifest(actual_size, size_in_manifest, actual_hash=None, hash_in_manifest=None, threshold=None):
        sizes_match = actual_size == size_in_manifest
        if not actual_hash:
            return sizes_match

        hashes_match = (
            # and perhaps we need the to check for match even without base64 encoding
            actual_hash == hash_in_manifest
            # this case comes from comparing blob hashes to manifest entries (in context of GCS)
            or actual_hash == base64.b64decode(hash_in_manifest).hex()
            # this comes from comparing files to a cache
            or hash_in_manifest == base64.b64decode(actual_hash).hex()
        )

        return sizes_match and hashes_match


def _human_readable_size(size, decimal_places=3):
    for unit in ["B", "KiB", "MiB", "GiB", "TiB"]:
        if size < 1024.0:
            break
        size /= 1024.0
    return '{:.{}f}{}'.format(size, decimal_places, unit)
