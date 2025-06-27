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

import datetime
import hashlib
import io
import logging
import os
import pathlib
import typing as t
from pathlib import Path
import aiofiles

from medusa.storage.abstract_storage import AbstractStorage, AbstractBlob, ManifestObject, ObjectDoesNotExistError


BUFFER_SIZE = 4 * 1024 * 1024


class LocalStorage(AbstractStorage):

    def __init__(self, config):
        self.config = config
        self.bucket_name = self.config.bucket_name

        self.root_dir = Path(config.base_path) / self.bucket_name
        self.root_dir.mkdir(parents=True, exist_ok=True)

        super().__init__(config)

    def connect(self):
        # nothing to connect when running locally
        pass

    def disconnect(self):
        # nothing to disconnect when running locally
        pass

    async def _list_blobs(self, prefix=None):
        if prefix is None:
            paths = list(self.root_dir.glob('**/*'))
        else:
            paths = [
                p for p in self.root_dir.glob('**/*')
                # relative_to() cuts off the base_path and bucket, so it works just like cloud storages
                if str(p.relative_to(self.root_dir)).startswith(str(prefix))
            ]

        return [
            AbstractBlob(
                str(p.relative_to(self.root_dir)),
                os.stat(self.root_dir / p).st_size,
                None,   # was self._md5(self.root_dir / p),  see Task1 for issue #829
                datetime.datetime.fromtimestamp(os.stat(self.root_dir / p).st_mtime),
                None
            )
            for p in paths if not p.is_dir()
        ]

    def _md5(self, file_path: str) -> str:
        with open(file_path, 'rb') as f:
            md5 = hashlib.md5()
            while True:
                data = f.read(BUFFER_SIZE)
                if not data:
                    break
                md5.update(data)
            return md5.hexdigest()

    async def _upload_object(self, data: io.BytesIO, object_key: str, headers: t.Dict[str, str]) -> AbstractBlob:
        object_path = self.root_dir / object_key
        object_path.parent.mkdir(parents=True, exist_ok=True)

        async with aiofiles.open(object_path, 'wb') as f:
            md5 = hashlib.md5()
            while True:
                chunk = data.read(BUFFER_SIZE)
                if not chunk:
                    break
                md5.update(chunk)
                await f.write(chunk)

        return AbstractBlob(
            object_key,
            os.stat(object_path).st_size,
            md5.hexdigest(),
            datetime.datetime.fromtimestamp(os.stat(object_path).st_mtime),
            None
        )

    async def _download_blob(self, src: str, dest: str):
        src_file = self.root_dir / src

        src_path = Path(src)
        dest_file = AbstractStorage.path_maybe_with_parent(dest, src_path)

        logging.debug(
            '[Local Storage] Downloading {} -> {}'.format(
                src_file, dest_file
            )
        )

        async with aiofiles.open(src_file, 'rb') as f:
            Path(dest_file).parent.mkdir(parents=True, exist_ok=True)
            async with aiofiles.open(dest_file, 'wb') as d:
                while True:
                    data = await f.read(BUFFER_SIZE)
                    if not data:
                        break
                    await d.write(data)

    async def _upload_blob(self, src: str, dest: str) -> ManifestObject:

        src_path = Path(src)

        # check if objects resides in a sub-folder (e.g. secondary index). if it does, use the sub-folder in object path
        dest_file = self.root_dir / AbstractStorage.path_maybe_with_parent(dest, src_path)

        file_size = os.stat(src_path).st_size
        logging.debug(
            '[Local Storage] Uploading {} ({}) -> {}'.format(
                src_path, self.human_readable_size(file_size), dest_file
            )
        )
        # remove root_dir from dest_file name
        dest_object_key = str(dest_file.relative_to(str(self.root_dir)))

        md5 = hashlib.md5()

        async with aiofiles.open(src_path, 'rb') as f:
            dest_file.parent.mkdir(parents=True, exist_ok=True)
            async with aiofiles.open(dest_file, 'wb') as d:
                while True:
                    data = await f.read(BUFFER_SIZE)
                    if not data:
                        break
                    await d.write(data)
                    md5.update(data)

        return ManifestObject(
            dest_object_key,
            os.stat(dest_file).st_size,
            md5.hexdigest(),
        )

    async def _get_object(self, object_key: t.Union[Path, str]) -> AbstractBlob:
        object_path = self.root_dir / object_key

        if not object_path.exists():
            raise ObjectDoesNotExistError(object_key)

        return AbstractBlob(
            str(object_key),
            os.stat(object_path).st_size,
            self._md5(object_path),
            datetime.datetime.fromtimestamp(os.stat(object_path).st_mtime),
            None
        )

    async def _read_blob_as_bytes(self, blob: AbstractBlob) -> bytes:
        object_path = self.root_dir / blob.name
        async with aiofiles.open(object_path, 'rb') as f:
            return await f.read()

    async def _delete_object(self, obj: AbstractBlob):
        object_path = self.root_dir / obj.name
        os.remove(object_path)

    def get_object_datetime(self, blob):
        return blob.last_modified

    def get_cache_path(self, path):
        # Full path for files that will be taken from previous backups
        return "{}/{}/{}".format(self.config.base_path, self.config.bucket_name, path)

    @staticmethod
    def blob_matches_manifest(blob, object_in_manifest, enable_md5_checks=False):
        return LocalStorage.compare_with_manifest(
            actual_size=blob.size,
            size_in_manifest=object_in_manifest['size']
        )

    @staticmethod
    def file_matches_storage(src: pathlib.Path, cached_item: ManifestObject, threshold=None, enable_md5_checks=False):
        return LocalStorage.compare_with_manifest(
            actual_size=src.stat().st_size,
            size_in_manifest=cached_item.size
        )

    @staticmethod
    def compare_with_manifest(actual_size, size_in_manifest, actual_hash=None, hash_in_manifest=None, threshold=None):
        return actual_size == size_in_manifest
