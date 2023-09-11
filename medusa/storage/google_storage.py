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

import aiohttp
import asyncio
import base64
import datetime
import io
import itertools
import logging
import typing as t

from libcloud.storage.types import ObjectDoesNotExistError
from pathlib import Path
from retrying import retry

from gcloud.aio.storage import Storage

from medusa.storage.abstract_storage import AbstractStorage, AbstractBlob, ManifestObject
from medusa.storage.s3_base_storage import S3BaseStorage


GOOGLE_MAX_FILES_PER_CHUNK = 64
MAX_UP_DOWN_LOAD_RETRIES = 5


class GoogleStorage(AbstractStorage):

    # no clue what these are used for
    api_version = '2006-03-01'

    def __init__(self, config):

        service_file = str(Path(config.key_file).expanduser())
        logging.info("Using service file: {}".format(service_file))

        self.bucket_name = config.bucket_name

        self.session = aiohttp.ClientSession(
            # TODO: will become shared from the base class
            loop=S3BaseStorage._get_or_create_event_loop(),
        )

        self.gcs_storage = Storage(session=self.session, service_file=service_file)

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
        return 'google_storage'

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
        content = await self.gcs_storage.download(
            bucket=self.bucket_name,
            object_name=blob.name,
            session=self.session,
            # disabling the timeout now because for small files we ought not to hit it
            # timeout=-1
        )
        return content

    def list_blobs(self, prefix: t.Optional[str] = None) -> t.List[AbstractBlob]:
        loop = S3BaseStorage._get_or_create_event_loop()
        objects = loop.run_until_complete(self._list_blobs(prefix))
        return objects

    async def _list_blobs(self, prefix: t.Optional[str] = None) -> t.List[AbstractBlob]:
        objects = await self.gcs_storage.list_objects(
            bucket=self.bucket_name,
            params={'prefix': prefix}
        )

        if objects.get('items') is None:
            return []

        return [
            AbstractBlob(
                o['name'],
                int(o['size']),
                o['md5Hash'],
                # datetime comes as a string like 2023-08-31T14:23:24.957Z
                datetime.datetime.strptime(o['timeCreated'], '%Y-%m-%dT%H:%M:%S.%fZ')
            )
            for o in objects.get('items')
        ]

    def get_object(self, bucket_name, object_key: str) -> t.Optional[AbstractBlob]:
        # Doesn't actually read the contents, just lists the thing
        try:
            loop = self._get_or_create_event_loop()
            o = loop.run_until_complete(self._get_object(object_key))
            return o
        except aiohttp.client_exceptions.ClientResponseError as cre:
            if cre.status == 404:
                return None
            raise cre

    async def _get_object(self, object_key: str) -> AbstractBlob:
        blob = await self._stat_blob(object_key)
        return blob

    async def _stat_blob(self, object_key: str) -> AbstractBlob:
        blob = await self.gcs_storage.download_metadata(
            bucket=self.bucket_name,
            object_name=object_key,
        )
        return AbstractBlob(
            blob['name'],
            int(blob['size']),
            blob['md5Hash'],
            # datetime comes as a string like 2023-08-31T14:23:24.957Z
            datetime.datetime.strptime(blob['timeCreated'], '%Y-%m-%dT%H:%M:%S.%fZ')
        )

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

    @retry(stop_max_attempt_number=MAX_UP_DOWN_LOAD_RETRIES, wait_fixed=5000)
    async def _upload_blob(self, src: str, dest: str) -> ManifestObject:
        src_chunks = src.split('/')
        parent_name, file_name = src_chunks[-2], src_chunks[-1]

        # check if objects resides in a sub-folder (e.g. secondary index). if it does, use the sub-folder in object path
        object_key = (
            "{}/{}/{}".format(dest, parent_name, file_name)
            if parent_name.startswith(".")
            else "{}/{}".format(dest, file_name)
        )

        logging.debug(
            '[Storage] Uploading {} -> gs://{}/{}'.format(
                src, self.config.bucket_name, object_key
            )
        )
        if src.startswith("gs"):
            resp = await self.gcs_storage.copy(
                bucket=self.bucket_name,
                object_name=f'{src}'.replace(f'gs://{self.bucket_name}/', ''),
                destination_bucket=self.bucket_name,
                new_name=object_key,
            )
            resp = resp['resource']
        else:
            resp = await self.gcs_storage.upload_from_filename(
                bucket=self.bucket_name,
                object_name=object_key,
                filename=src,
                # this is ~ to forcing s3 multipart upload, unsure if we wanna do it
                # force_resumable_upload=True
            )
        mo = ManifestObject(resp['name'], int(resp['size']), resp['md5Hash'])
        return mo

    def upload_object_via_stream(self, data: io.BytesIO, container, object_name: str, headers) -> AbstractBlob:
        loop = self._get_or_create_event_loop()
        o = loop.run_until_complete(self._upload_object(data, object_name))
        return o

    async def _upload_object(self, data: io.BytesIO, object_key: str) -> AbstractBlob:
        logging.debug(
            '[Storage] Uploading object from stream -> gcs://{}/{}'.format(
                self.config.bucket_name, object_key
            )
        )
        resp = await self.gcs_storage.upload(
            bucket=self.bucket_name,
            object_name=object_key,
            file_data=data
        )
        return AbstractBlob(resp['name'], int(resp['size']), resp['md5Hash'], resp['timeCreated'])

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
        await self.gcs_storage.delete(
            bucket=self.bucket_name,
            object_name=obj.name
        )

    @retry(stop_max_attempt_number=MAX_UP_DOWN_LOAD_RETRIES, wait_fixed=5000)
    def download_blobs(self, srcs: t.List[t.Union[Path, str]], dest: t.Union[Path, str]):
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

        logging.debug(
            '[Storage] Downloading gcs://{}/{} -> {}'.format(
                self.config.bucket_name, object_key, file_path
            )
        )

        try:
            await self.gcs_storage.download_to_filename(
                bucket=self.bucket_name,
                object_name=object_key,
                filename=file_path
            )

        except aiohttp.client_exceptions.ClientResponseError as cre:
            logging.error('Error downloading file from gs://{}/{}: {}'.format(self.config.bucket_name, object_key, cre))
            if cre.status == 404:
                raise ObjectDoesNotExistError(
                    value='Object {} does not exist'.format(object_key),
                    driver=self,
                    object_name=object_key
                )
            raise cre

    def get_object_datetime(self, blob: AbstractBlob) -> datetime.datetime:
        logging.debug(
            "Blob {} last modification time is {}".format(
                blob.name, blob.last_modified
            )
        )
        return blob.last_modified

    def get_path_prefix(self, path):
        return ""

    def get_download_path(self, path):
        if "gs://" in path:
            return path
        else:
            return "gs://{}/{}".format(self.bucket_name, path)

    def get_cache_path(self, path):
        # Full path for files that will be taken from previous backups
        return self.get_download_path(path)
        # return path

    @staticmethod
    def blob_matches_manifest(blob, object_in_manifest, enable_md5_checks=False):
        return GoogleStorage.compare_with_manifest(
            actual_size=blob.size,
            size_in_manifest=object_in_manifest['size'],
            actual_hash=str(blob.hash) if enable_md5_checks else None,
            hash_in_manifest=object_in_manifest['MD5']
        )

    @staticmethod
    def file_matches_cache(src, cached_item, threshold=None, enable_md5_checks=False):
        return GoogleStorage.compare_with_manifest(
            actual_size=src.stat().st_size,
            size_in_manifest=cached_item['size'],
            actual_hash=AbstractStorage.generate_md5_hash(src) if enable_md5_checks else None,
            hash_in_manifest=cached_item['MD5']
        )

    @staticmethod
    def compare_with_manifest(actual_size, size_in_manifest, actual_hash=None, hash_in_manifest=None, threshold=None):
        sizes_match = actual_size == size_in_manifest
        if not actual_hash:
            return sizes_match

        hashes_match = (
            # this case comes from comparing blob hashes to manifest entries (in context of GCS)
            actual_hash == base64.b64decode(hash_in_manifest).hex()
            # this comes from comparing files to a cache
            or hash_in_manifest == base64.b64decode(actual_hash).hex()
            # and perhaps we need the to check for match even without base64 encoding
            or actual_hash == hash_in_manifest
        )

        return sizes_match and hashes_match


def _is_in_folder(file_path, folder_path):
    return file_path.parent.name == Path(folder_path).name


def _group_by_parent(paths):
    by_parent = itertools.groupby(paths, lambda p: Path(p).parent.name)
    for parent, files in by_parent:
        yield parent, list(files)
