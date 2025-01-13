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
import io
import pathlib
import typing as t
import unittest
from pathlib import Path

from medusa.storage import ManifestObject, AbstractBlob
from medusa.storage.abstract_storage import AbstractStorage


class AttributeDict(dict):
    __slots__ = ()
    __getattr__ = dict.__getitem__
    __setattr__ = dict.__setitem__
    __dict__ = dict.__dict__


class TestAbstractStorage(AbstractStorage):
    def connect(self):
        pass

    def disconnect(self):
        pass

    async def _list_blobs(self, prefix=None):
        pass

    async def _upload_object(self, data: io.BytesIO, object_key: str, headers: t.Dict[str, str]) -> AbstractBlob:
        pass

    async def _download_blob(self, src: str, dest: str):
        pass

    async def _upload_blob(self, src: str, dest: str) -> ManifestObject:
        pass

    async def _get_object(self, object_key: t.Union[Path, str]) -> AbstractBlob:
        pass

    async def _read_blob_as_bytes(self, blob: AbstractBlob) -> bytes:
        pass

    async def _delete_object(self, obj: AbstractBlob):
        pass

    @staticmethod
    def blob_matches_manifest(blob, object_in_manifest, enable_md5_checks=False):
        pass

    @staticmethod
    def file_matches_storage(src: pathlib.Path, cached_item: ManifestObject, threshold=None, enable_md5_checks=False):
        pass

    @staticmethod
    def compare_with_manifest(actual_size, size_in_manifest, actual_hash=None, hash_in_manifest=None, threshold=None):
        pass

    def __init__(self, config):
        super().__init__(config)


class AbstractStorageTest(unittest.TestCase):

    def test_convert_human_friendly_size_to_bytes(self):
        self.assertEqual(50, AbstractStorage._human_size_to_bytes('50B'))
        self.assertEqual(50, AbstractStorage._human_size_to_bytes('50B/s'))
        self.assertEqual(50, AbstractStorage._human_size_to_bytes('50 B'))
        self.assertEqual(50, AbstractStorage._human_size_to_bytes('50 B '))
        self.assertEqual(50, AbstractStorage._human_size_to_bytes(' 50 B '))
        self.assertEqual(50, AbstractStorage._human_size_to_bytes(' 50 B / s'))
        self.assertEqual(50, AbstractStorage._human_size_to_bytes(' 50 B/s'))
        self.assertEqual(50, AbstractStorage._human_size_to_bytes(' 50 B /s'))
        self.assertEqual(50, AbstractStorage._human_size_to_bytes(' 50 B/ s'))
        self.assertEqual(2 * 1024, AbstractStorage._human_size_to_bytes('2KB'))
        self.assertEqual(2 * 1024 ** 2, AbstractStorage._human_size_to_bytes('2MB'))
        self.assertEqual(2.5 * 1024 ** 2, AbstractStorage._human_size_to_bytes('2.5MB'))
        self.assertEqual(2.5 * 1024 ** 2, AbstractStorage._human_size_to_bytes('2.5MB/s'))
        self.assertEqual(2.5 * 1024 ** 2, AbstractStorage._human_size_to_bytes('2.5 MB/s'))
        self.assertEqual(2 * 1024 ** 3, AbstractStorage._human_size_to_bytes('2GB'))
        self.assertEqual(2 * 1024 ** 4, AbstractStorage._human_size_to_bytes('2TB'))
        self.assertEqual(2 * 1024 ** 5, AbstractStorage._human_size_to_bytes('2PB'))

    def test_get_storage_class(self):
        config = AttributeDict({
            'bucket_name': 'must_be_set',
            'storage_class': 'hot'
        })
        storage = TestAbstractStorage(config)
        self.assertEqual('HOT', storage.get_storage_class())

        config.storage_class = None
        self.assertEqual(None, storage.get_storage_class())
        storage_class = storage.get_storage_class()
        self.assertEqual('unset', storage_class.capitalize() if storage_class else 'unset')
