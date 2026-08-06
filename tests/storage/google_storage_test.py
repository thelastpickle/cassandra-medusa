# -*- coding: utf-8 -*-
# Copyright 2019 Spotify AB. All rights reserved.
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
import aiohttp
import io
import itertools
import tempfile
import types
import unittest

from unittest import mock
from pathlib import Path

from tenacity import RetryError
from medusa.storage.google_storage import (
    _group_by_parent, _is_in_folder, GoogleStorage,
    DOWNLOAD_STREAM_CONSUMPTION_CHUNK_SIZE, MAX_UP_DOWN_LOAD_RETRIES,
)


class GoogleStorageTest(unittest.TestCase):

    def test_is_in_folder(self):
        folder = Path('foo/bar')
        in_file = Path('foo/bar/file.txt')
        out_file = Path('foo/bar/.baz/file.txt')
        self.assertTrue(_is_in_folder(in_file, folder))
        self.assertFalse(_is_in_folder(out_file, folder))

    def test_group_by_parent(self):
        p1, p2 = Path('foo/file1.txt'), Path('foo/file2.txt')
        p3, p4 = Path('foo/.bar/file3.txt'), Path('foo/.bar/file4.txt')
        files = [p1, p2, p3, p4]
        by_parent = dict(_group_by_parent(files))
        self.assertEqual(2, len(by_parent))
        self.assertEqual({'foo', '.bar', }, by_parent.keys())
        self.assertTrue(p1 in by_parent['foo'])
        self.assertTrue(p2 in by_parent['foo'])
        self.assertFalse(p3 in by_parent['foo'])
        self.assertFalse(p4 in by_parent['foo'])

    def test_iterator_hierarchy(self):

        def _inner_inner():
            return [n for n in range(0, 2)]

        def _inner():
            for i in range(0, 2):
                yield _inner_inner()

        g = _inner()
        self.assertTrue(isinstance(g, types.GeneratorType))
        c = itertools.chain(*g)
        self.assertTrue(isinstance(c, itertools.chain))
        rr = list(c)
        self.assertTrue(isinstance(rr, list))
        self.assertTrue(isinstance(rr[0], int))

    def test_upload_object_rate_limit_retry(self):

        # Create a dummy config
        class DummyConfig:
            key_file = None
            bucket_name = 'dummy-bucket'
            read_timeout = -1
        storage = GoogleStorage(DummyConfig())
        storage._ensure_session = lambda: None  # Bypass session creation
        storage.gcs_storage = mock.Mock()

        # Simulate upload always raising 429
        async def always_429(*args, **kwargs):
            raise aiohttp.ClientResponseError(
                request_info=mock.Mock(),
                history=(),
                status=429,
                message='Too Many Requests',
                headers={}
            )
        storage.gcs_storage.upload = always_429

        # Count how many times the upload is called
        call_counter = {'count': 0}

        async def counting_upload(*args, **kwargs):
            call_counter['count'] += 1
            raise aiohttp.ClientResponseError(
                request_info=mock.Mock(),
                history=(),
                status=429,
                message='Too Many Requests',
                headers={}
            )
        storage.gcs_storage.upload = counting_upload

        # Run the upload and expect it to raise after max retries
        with self.assertRaises(RetryError):
            asyncio.run(storage._upload_object(io.BytesIO(b'data'), 'key', {}))
        self.assertEqual(call_counter['count'], MAX_UP_DOWN_LOAD_RETRIES)

    def _make_gcs_storage(self, extra_config=None):
        """Build a GoogleStorage instance with a minimal config, bypassing session creation."""
        class DummyConfig:
            key_file = None
            bucket_name = 'dummy-bucket'
            read_timeout = -1
            concurrent_transfers = '0'

        if extra_config:
            for k, v in extra_config.items():
                setattr(DummyConfig, k, v)

        storage = GoogleStorage(DummyConfig())
        storage._ensure_session = lambda: None
        return storage

    def _make_fake_stream(self, data: bytes):
        """Return an async-compatible stream backed by in-memory bytes."""
        buf = io.BytesIO(data)

        class FakeStream:
            async def read(self, size=-1):
                return buf.read(size)

        return FakeStream()

    def test_download_blob_uses_multipart_chunksize(self):
        storage = self._make_gcs_storage({'multipart_chunksize': '10MB'})
        self.assertEqual(10 * 1024 * 1024, storage.multipart_chunksize_bytes)

        fake_stream = self._make_fake_stream(b'tiny payload')
        fake_blob = mock.MagicMock()
        fake_blob.name = 'some/object'

        read_sizes = []
        original_read = fake_stream.read

        async def recording_read(size=-1):
            read_sizes.append(size)
            return await original_read(size)

        fake_stream.read = recording_read

        storage.gcs_storage = mock.AsyncMock()
        storage.gcs_storage.download_stream = mock.AsyncMock(return_value=fake_stream)

        async def fake_stat(key):
            return fake_blob

        storage._stat_blob = fake_stat

        with tempfile.TemporaryDirectory() as tmp_dir:
            asyncio.run(storage._download_blob('some/object', tmp_dir))

        self.assertTrue(all(s == 10 * 1024 * 1024 for s in read_sizes))

    def test_download_blob_uses_default_chunk_size_when_not_configured(self):
        storage = self._make_gcs_storage()  # no multipart_chunksize
        self.assertEqual(DOWNLOAD_STREAM_CONSUMPTION_CHUNK_SIZE, storage.multipart_chunksize_bytes)

        fake_stream = self._make_fake_stream(b'tiny payload')
        fake_blob = mock.MagicMock()
        fake_blob.name = 'some/object'

        read_sizes = []
        original_read = fake_stream.read

        async def recording_read(size=-1):
            read_sizes.append(size)
            return await original_read(size)

        fake_stream.read = recording_read

        storage.gcs_storage = mock.AsyncMock()
        storage.gcs_storage.download_stream = mock.AsyncMock(return_value=fake_stream)

        async def fake_stat(key):
            return fake_blob

        storage._stat_blob = fake_stat

        with tempfile.TemporaryDirectory() as tmp_dir:
            asyncio.run(storage._download_blob('some/object', tmp_dir))

        self.assertTrue(all(s == DOWNLOAD_STREAM_CONSUMPTION_CHUNK_SIZE for s in read_sizes))
