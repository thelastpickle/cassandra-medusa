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

import aiohttp
import aiohttp_s3_client
import asyncio
import base64
import botocore.session
import collections
import datetime
import logging
import io
import itertools
import os
import subprocess
import typing as t

from http import HTTPStatus
from libcloud.storage.types import ObjectDoesNotExistError
from pathlib import Path
from retrying import retry

from aiohttp_s3_client import S3Client
from aiohttp_s3_client.credentials import StaticCredentials

from medusa.storage.abstract_storage import AbstractStorage, AbstractBlob, AbstractBlobMetadata


ManifestObject = collections.namedtuple('ManifestObject', ['path', 'size', 'MD5'])

MAX_UP_DOWN_LOAD_RETRIES = 5

"""
    S3BaseStorage supports all the S3 compatible storages. Certain providers might override this method
    to implement their own specialities (such as environment variables when running in certain clouds)

    This implementation uses awscli instead of libcloud's s3's driver for uploads/downloads. If you wish
    to use the libcloud's internal driver instead of awscli dependency, select s3_rgw.
"""


class CensoredCredentials(StaticCredentials):

    def __repr__(self):
        if len(self.access_key_id) > 0:
            key = f"{self.access_key_id[0]}..{self.access_key_id[-1]}"
        else:
            key = "None"
        secret = "*****"
        return f"CensoredCredentials(access_key_id={key}, secret_access_key={secret}, region={self.region})"


LIBCLOUD_REGION_NAME_MAP = {
    'S3_US_EAST': 'us-east-1',
    'S3_US_EAST1': 'us-east-1',
    'S3_US_EAST2': 'us-east-2',
    'S3_US_WEST': 'us-west-1',
    'S3_US_WEST2': 'us-west-2',
    'S3_US_WEST_OREGON': 'us-west-2',
    'S3_US_GOV_EAST': 'us-gov-east-1',
    'S3_US_GOV_WEST': 'us-gov-west-1',
    'S3_EU_WEST': 'eu-west-1',
    'S3_EU_WEST2': 'eu-west-2',
    'S3_EU_CENTRAL': 'eu-central-1',
    'S3_EU_NORTH1': 'eu-north-1',
    'S3_AP_SOUTH': 'ap-south-1',
    'S3_AP_SOUTHEAST': 'ap-southeast-1',
    'S3_AP_SOUTHEAST2': 'ap-southeast-2',
    'S3_AP_NORTHEAST': 'ap-northeast-1',
    'S3_AP_NORTHEAST1': 'ap-northeast-1',
    'S3_AP_NORTHEAST2': 'ap-northeast-2',
    'S3_SA_EAST': 'sa-east-1',
    'S3_SA_EAST2': 'sa-east-2',
    'S3_CA_CENTRAL': 'ca-central-1',
    'S3_CN_NORTH': 'cn-north-1',
    'S3_CN_NORTHWEST': 'cn-northwest-1',
    'S3_AF_SOUTH': 'af-south-1',
    'S3_ME_SOUTH': 'me-south-1',
}


class S3BaseStorage(AbstractStorage):

    api_version = '2006-03-01'

    def __init__(self, config):

        if config.kms_id:
            logging.debug("Using KMS key {}".format(config.kms_id))

        self.credentials = self._consolidate_credentials(config)

        logging.info('Using credentials {}'.format(self.credentials))

        self.bucket_name: str = config.bucket_name

        if config.storage_provider != 's3_compatible':
            # assuming we're dealing with regular aws
            s3_url = "https://{}.s3.amazonaws.com".format(self.bucket_name)
        else:
            # we're dealing with a custom s3 compatible storage, so we need to craft the URL
            protocol = 'https' if config.secure.lower() == 'true' else 'http'
            port = '' if config.port is None else str(config.port)
            s3_url = '{}://{}:{}/{}'.format(protocol, config.host, port, self.bucket_name)

        logging.info('Using S3 URL {}'.format(s3_url))

        self.http_session: aiohttp.ClientSession = aiohttp.ClientSession(
            loop=self._get_or_create_event_loop(),
        )
        self.http_client: aiohttp_s3_client.S3Client = S3Client(
            url=s3_url,
            session=self.http_session,
            credentials=self.credentials,
            region=self.credentials.region,
        )

        # disable aiohttp_s3_client's debug logging, it's just too noisy
        logging.getLogger('aiohttp_s3_client').setLevel(logging.WARNING)
        logging.getLogger('charset_normalizer').setLevel(logging.WARNING)

        super().__init__(config)

    @staticmethod
    def _consolidate_credentials(config) -> CensoredCredentials:

        session = botocore.session.Session()

        if config.api_profile:
            logging.debug("Using AWS profile {}".format(
                config.api_profile,
            ))
            session.set_config_variable('profile', config.api_profile)

        if config.region and config.region != "default":
            session.set_config_variable('region', config.region)
        elif config.storage_provider not in ['s3', 's3_compatible'] and config.region == "default":
            session.set_config_variable('region', S3BaseStorage._region_from_provider_name(config.storage_provider))
        else:
            session.set_config_variable('region', "us-east-1")

        if config.key_file:
            logging.debug("Setting AWS credentials file to {}".format(
                config.key_file,
            ))
            session.set_config_variable('credentials_file', config.key_file)

        boto_credentials = session.get_credentials()
        return CensoredCredentials(
            access_key_id=boto_credentials.access_key,
            secret_access_key=boto_credentials.secret_key,
            region=session.get_config_variable('region'),
        )

    @staticmethod
    def _region_from_provider_name(provider_name: str) -> str:
        if provider_name.upper() in LIBCLOUD_REGION_NAME_MAP.keys():
            return LIBCLOUD_REGION_NAME_MAP[provider_name.upper()]
        else:
            raise ValueError("Unknown provider name {}".format(provider_name))

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
        return 's3_base_storage'

    def list_container_objects(self, container_name, ex_prefix: t.Optional[str] = None) -> t.List[AbstractBlob]:
        blobs = self.list_blobs(prefix=ex_prefix)
        return blobs

    def check_dependencies(self):
        pass

    def read_blob_as_string(self, blob: AbstractBlob, encoding: t.Optional[str] = 'utf-8') -> str:
        return self.read_blob_as_bytes(blob).decode(encoding)

    def read_blob_as_bytes(self, blob: AbstractBlob) -> bytes:
        logging.debug("[Storage] Reading blob {}...".format(blob.name))
        loop = self._get_or_create_event_loop()
        b = loop.run_until_complete(self._read_blob_as_bytes(blob))
        return b

    async def _read_blob_as_bytes(self, blob: AbstractBlob) -> bytes:
        async with self.http_client.get(blob.name) as resp:
            return await resp.read()

    def list_blobs(self, prefix: t.Optional[str] = None) -> t.List[AbstractBlob]:
        loop = self._get_or_create_event_loop()
        objects = loop.run_until_complete(self._list_blobs(prefix))
        return objects

    async def _list_blobs(self, prefix: t.Optional[str] = None) -> t.List[AbstractBlob]:
        pages = self.http_client.list_objects_v2(prefix=prefix)
        blobs = []
        async for page in pages:
            for obj in page:
                obj_hash = obj.etag.replace('"', '')
                blobs.append(AbstractBlob(obj.key, obj.size, obj_hash, obj.last_modified))
        return blobs

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

    @retry(stop_max_attempt_number=MAX_UP_DOWN_LOAD_RETRIES, wait_fixed=5000)
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
        if file_size < int(self.config.multi_part_upload_threshold):
            await self._upload_small_blob(src, object_key)
        else:
            await self._upload_big_blob(src, object_key, file_size)

        blob = await self._stat_blob(object_key)
        mo = ManifestObject(blob.name, blob.size, blob.hash)
        return mo

    async def _upload_small_blob(self, src: str, object_key: str):
        logging.debug(
            '[Storage] Uploading single-part {} -> s3://{}/{}'.format(
                src, self.config.bucket_name, object_key
            )
        )
        await self.http_client.put_file(
            file_path=src,
            object_name=object_key,
            headers=self.additional_upload_headers()
        )
        # arbitrary wait to prevent server disconnection
        # see https://github.com/aio-libs/aiohttp/issues/4549#issuecomment-1514524289
        await asyncio.sleep(0.001)

    async def _upload_big_blob(self, src: str, object_key: str, file_size: int):
        logging.debug(
            '[Storage] Uploading multi-part {} ({}) -> s3://{}/{}'.format(
                src, _human_readable_size(file_size), self.config.bucket_name, object_key
            )
        )
        await self.http_client.put_file_multipart(
            file_path=src,
            object_name=object_key,
            headers=self.additional_upload_headers(),
            workers_count=2
        )

    def upload_object_via_stream(self, data: io.BytesIO, container, object_name: str, headers) -> AbstractBlob:
        loop = self._get_or_create_event_loop()
        o = loop.run_until_complete(self._upload_object(data, object_name, headers))
        return o

    async def _upload_object(self, data: io.BytesIO, object_key: str, headers: t.Dict[str, str]) -> AbstractBlob:
        logging.debug(
            '[Storage] Uploading object from stream -> s3://{}/{}'.format(
                self.config.bucket_name, object_key
            )
        )
        await self.http_client.put(
            object_key,
            data=data.getvalue(),
            headers=headers
        )
        blob = await self._stat_blob(object_key)
        return blob

    async def _stat_blob(self, object_key: str) -> AbstractBlob:
        async with self.http_client.head(object_key) as resp:
            if not resp.status == HTTPStatus.OK:
                raise ObjectDoesNotExistError(
                    value='Object {} does not exist'.format(object_key),
                    driver=self,
                    object_name=object_key
                )
        page = self.http_client.list_objects_v2(prefix=object_key)
        async for items in page:
            for item in items:
                # need to remove double quotes from the etags. libclud does this too
                item_hash = item.etag.replace('"', '')
                return AbstractBlob(item.key, int(item.size), item_hash, item.last_modified)

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
        await self.http_client.delete(obj.name)

    @retry(stop_max_attempt_number=MAX_UP_DOWN_LOAD_RETRIES, wait_fixed=5000)
    def get_blob_metadata(self, blob_key: str) -> AbstractBlobMetadata:
        loop = self._get_or_create_event_loop()
        return loop.run_until_complete(self._get_blob_metadata(blob_key))

    @retry(stop_max_attempt_number=MAX_UP_DOWN_LOAD_RETRIES, wait_fixed=5000)
    def get_blobs_metadata(self, blob_keys: t.List[str]) -> t.List[AbstractBlobMetadata]:
        loop = self._get_or_create_event_loop()
        return loop.run_until_complete(self._get_blobs_metadata(blob_keys))

    async def _get_blobs_metadata(self, blob_keys: t.List[str]) -> t.List[AbstractBlobMetadata]:
        coros = [self._get_blob_metadata(blob_key) for blob_key in blob_keys]
        return await asyncio.gather(*coros)

    async def _get_blob_metadata(self, blob_key: str) -> AbstractBlobMetadata:
        async with self.http_client.head(blob_key) as resp:
            # the headers come as some non-default dict, so we need to re-package them
            blob_metadata = dict(**resp.headers)

            sse_algo = blob_metadata.get('x-amz-server-side-encryption', None)
            if sse_algo == 'AES256':
                sse_enabled, sse_key_id = False, None
            elif sse_algo == 'aws:kms':
                sse_enabled = True
                # the metadata returns the entire ARN, so we just return the last part ~ the actual ID
                sse_key_id = blob_metadata['x-amz-server-side-encryption-aws-kms-key-id'].split('/')[-1]
            else:
                logging.warning('No SSE info found in blob {} metadata'.format(blob_key))
                sse_enabled, sse_key_id = False, None

            return AbstractBlobMetadata(blob_key, sse_enabled, sse_key_id)

    @retry(stop_max_attempt_number=MAX_UP_DOWN_LOAD_RETRIES, wait_fixed=5000)
    def download_blobs(self, srcs: t.List[t.Union[Path, str]], dest: t.Union[Path, str]):
        """
        Downloads a list of files from the remote storage system to the local storage

        :param srcs: a list of files to download from the remote storage system
        :param dest: the path where to download the objects locally
        :return:
        """
        loop = self._get_or_create_event_loop()
        loop.run_until_complete(self._download_blobs(srcs, dest))

    async def _download_blobs(self, srcs: t.List[t.Union[Path, str]], dest: t.Union[Path, str]):
        coros = [self._download_blob(src, dest) for src in map(str, srcs)]
        await asyncio.gather(*coros)

    async def _download_blob(self, src: str, dest: str):
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
            '[Storage] Downloading with {} workers: {} -> {}/{}'.format(
                workers, object_key, self.config.bucket_name, object_key
            )
        )

        try:
            await self.http_client.get_file_parallel(
                object_name=object_key,
                file_path=file_path,
                workers_count=workers
            )
        except Exception as e:
            logging.error('Error downloading file from s3://{}/{}: {}'.format(self.config.bucket_name, object_key, e))
            raise ObjectDoesNotExistError(
                value='Object {} does not exist'.format(object_key),
                driver=self,
                object_name=object_key
            )

    def get_object_datetime(self, blob: AbstractBlob) -> datetime.datetime:
        logging.debug(
            "Blob {} last modification time is {}".format(
                blob.name, blob.last_modified
            )
        )
        return blob.last_modified

    def get_cache_path(self, path: str) -> str:
        # Full path for files that will be taken from previous backups
        return path

    def _get_or_create_event_loop(self) -> asyncio.AbstractEventLoop:
        loop = asyncio.get_event_loop()
        if loop.is_closed():
            logging.warning("Having to make a new event loop unexpectedly")
            new_loop = asyncio.new_event_loop()
            if new_loop.is_closed():
                logging.error("Even the new event loop was not running, bailing out")
                raise RuntimeError("Could not create a new event loop")
            asyncio.set_event_loop(new_loop)
            return new_loop
        return loop

    @staticmethod
    def blob_matches_manifest(blob: AbstractBlob, object_in_manifest: dict, enable_md5_checks=False):
        return S3BaseStorage.compare_with_manifest(
            actual_size=blob.size,
            size_in_manifest=object_in_manifest['size'],
            actual_hash=str(blob.hash) if enable_md5_checks else None,
            hash_in_manifest=object_in_manifest['MD5']
        )

    @staticmethod
    def file_matches_cache(src, cached_item, threshold=None, enable_md5_checks=False):

        threshold = int(threshold) if threshold else -1

        # single or multi part md5 hash. Used by Azure and S3 uploads.
        if not enable_md5_checks:
            md5_hash = None
        elif src.stat().st_size >= threshold > 0:
            md5_hash = AbstractStorage.md5_multipart(src)
        else:
            md5_hash = AbstractStorage.generate_md5_hash(src)

        return S3BaseStorage.compare_with_manifest(
            actual_size=src.stat().st_size,
            size_in_manifest=cached_item['size'],
            actual_hash=md5_hash,
            hash_in_manifest=cached_item['MD5'],
            threshold=threshold
        )

    @staticmethod
    def compare_with_manifest(actual_size, size_in_manifest, actual_hash=None, hash_in_manifest=None, threshold=None):
        sizes_match = actual_size == size_in_manifest
        if not actual_hash:
            return sizes_match

        # md5 hash comparison
        if not threshold:
            threshold = -1
        else:
            threshold = int(threshold)

        if actual_size >= threshold > 0 or "-" in hash_in_manifest:
            multipart = True
        else:
            multipart = False

        if multipart:
            hashes_match = (
                actual_hash == hash_in_manifest
            )
        else:
            hashes_match = (
                actual_hash == base64.b64decode(hash_in_manifest).hex()
                or hash_in_manifest == base64.b64decode(actual_hash).hex()
                or actual_hash == hash_in_manifest
            )

        return sizes_match and hashes_match

    def prepare_upload(self):
        if self.config.transfer_max_bandwidth is not None:
            subprocess.check_call(
                [
                    "aws",
                    "configure",
                    "set",
                    "default.s3.max_bandwidth",
                    self.config.transfer_max_bandwidth,
                ]
            )

    def prepare_download(self):
        # Unthrottle downloads to speed up restores
        subprocess.check_call(
            [
                "aws",
                "configure",
                "set",
                "default.s3.max_bandwidth",
                "512MB/s",
            ]
        )

    def additional_upload_headers(self):
        headers = {}
        if self.config.kms_id:
            headers.update({
                "x-amz-server-side-encryption": "aws:kms",
                "x-amz-server-side-encryption-aws-kms-key-id": self.config.kms_id
            })
        return headers


def _group_by_parent(paths):
    by_parent = itertools.groupby(paths, lambda p: Path(p).parent.name)
    for parent, files in by_parent:
        yield parent, list(files)


def _human_readable_size(size, decimal_places=3):
    for unit in ["B", "KiB", "MiB", "GiB", "TiB"]:
        if size < 1024.0:
            break
        size /= 1024.0
    return '{:.{}f}{}'.format(size, decimal_places, unit)
