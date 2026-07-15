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
import asyncio
import io
import pathlib
import time
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
        # nothing to connect when running locally
        pass

    def disconnect(self):
        # nothing to disconnect when running locally
        pass

    async def _list_blobs(self, prefix=None):
        # nothing to list when running locally
        pass

    async def _upload_object(self, data: io.BytesIO, object_key: str, headers: t.Dict[str, str]) -> AbstractBlob:
        # nothing to upload when running locally
        pass

    async def _download_blob(self, src: str, dest: str):
        # nothing to download when running locally
        pass

    async def _upload_blob(self, src: str, dest: str) -> ManifestObject:
        # nothing to upload when running locally
        pass

    async def _get_object(self, object_key: t.Union[Path, str]) -> AbstractBlob:
        # nothing to get when running locally
        pass

    async def _read_blob_as_bytes(self, blob: AbstractBlob) -> bytes:
        # nothing to read when running locally
        pass

    async def _delete_object(self, obj: AbstractBlob):
        # nothing to delete when running locally
        pass

    @staticmethod
    def blob_matches_manifest(blob, object_in_manifest, enable_md5_checks=False):
        # nothing to match when running locally
        pass

    @staticmethod
    def file_matches_storage(src: pathlib.Path, cached_item: ManifestObject, threshold=None, enable_md5_checks=False):
        # nothing to match when running locally
        pass

    @staticmethod
    def compare_with_manifest(actual_size, size_in_manifest, actual_hash=None, hash_in_manifest=None, threshold=None):
        # nothing to compare when running locally
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

    def test_convert_bare_byte_count_to_bytes(self):
        # backward compatibility with configs written before human-readable sizes were supported
        self.assertEqual(20971520, AbstractStorage._human_size_to_bytes('20971520'))
        self.assertEqual(20971520, AbstractStorage._human_size_to_bytes('20971520.0'))

    def test_convert_invalid_size_raises(self):
        with self.assertRaises(ValueError):
            AbstractStorage._human_size_to_bytes('not-a-size')

    def test_get_storage_class(self):
        config = AttributeDict({
            'bucket_name': 'must_be_set',
            'storage_class': 'hot'
        })
        storage = TestAbstractStorage(config)
        self.assertEqual('HOT', storage.get_storage_class())

        config.storage_class = None
        self.assertIsNone(storage.get_storage_class())
        storage_class = storage.get_storage_class()
        self.assertEqual('unset', storage_class.capitalize() if storage_class else 'unset')

    def test_upload_blobs_uses_sliding_window_not_fixed_batches(self):
        # concurrent_transfers=2, but 3 files: a fixed-batch-of-2 scheduler would
        # gather ['slow', 'fast1'] as one batch (bounded by 'slow') before even
        # starting 'fast2', while a sliding window starts 'fast2' as soon as
        # 'fast1' frees up a slot, well before 'slow' completes.
        config = AttributeDict({'bucket_name': 'must_be_set', 'concurrent_transfers': 2})
        storage = TestAbstractStorage(config)

        durations = {'slow': 0.3, 'fast1': 0.1, 'fast2': 0.1}
        active = 0
        max_active = 0

        async def fake_upload_blob(src, dest):
            nonlocal active, max_active
            active += 1
            max_active = max(max_active, active)
            await asyncio.sleep(durations[src])
            active -= 1
            return ManifestObject(src, 0, None)

        storage._upload_blob = fake_upload_blob

        srcs = ['slow', 'fast1', 'fast2']
        start = time.monotonic()
        results = asyncio.run(storage._upload_blobs(srcs, 'dest'))
        elapsed = time.monotonic() - start

        # A fixed-batch-of-2 scheduler would take ~0.3 + 0.1 = 0.4s (two sequential
        # batches). A sliding window finishes close to the slowest single file (~0.3s).
        self.assertLess(elapsed, 0.35)
        self.assertLessEqual(max_active, 2)
        self.assertEqual(['slow', 'fast1', 'fast2'], [mo.path for mo in results])
