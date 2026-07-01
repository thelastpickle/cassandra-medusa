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

import hashlib
import pathlib
import unittest

from medusa.storage.abstract_storage import ManifestObject
from medusa.storage.google_storage import GoogleStorage
from medusa.storage.s3_storage import S3Storage


def s3_multipart_etag(data: bytes, part_size_bytes: int) -> str:
    # independent reimplementation of S3's multipart ETag formula, so tests don't just
    # assert the code under test agrees with itself
    parts = [data[i:i + part_size_bytes] for i in range(0, len(data), part_size_bytes)]
    concatenated_part_digests = b''.join(hashlib.md5(part).digest() for part in parts)
    return '{}-{}'.format(hashlib.md5(concatenated_part_digests).hexdigest(), len(parts))


class RestoreNodeTest(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def test_single_part_s3_file(self):
        cached_item = ManifestObject('path', 113651, '620c203520494bb92811fddc6d88cd65')
        src = pathlib.Path(__file__).parent / "resources/s3/md-10-big-CompressionInfo.db"
        assert S3Storage.file_matches_storage(src, cached_item, 100 * 1024 * 1024, True)

    def test_multi_part_s3_file(self):
        # Multi part hashes have a special structure, with the number of chunks at the end
        cached_item = ManifestObject('path', 113651, 'e4344d1ea2b32372db7f7e1c81d154b9-1')
        src = pathlib.Path(__file__).parent / "resources/s3/md-10-big-CompressionInfo.db"
        assert S3Storage.file_matches_storage(src, cached_item, 100, True)

    def test_multi_part_s3_file_fail(self):
        # File size is below the multi part threshold, a single part hash will be computed
        cached_item = ManifestObject('path', 113651, 'e4344d1ea2b32372db7f7e1c81d154b9-1')
        src = pathlib.Path(__file__).parent / "resources/s3/md-10-big-CompressionInfo.db"
        assert not S3Storage.file_matches_storage(src, cached_item, 100 * 1024 * 1024, True)

    def test_multi_part_s3_file_uses_configured_chunk_size(self):
        # part size must match what boto3 actually used to upload, or the multipart hash won't match
        src = pathlib.Path(__file__).parent / "resources/s3/md-10-big-CompressionInfo.db"
        data = src.read_bytes()
        chunk_size_bytes = 50 * 1024
        expected_hash = s3_multipart_etag(data, chunk_size_bytes)
        cached_item = ManifestObject('path', len(data), expected_hash)
        assert S3Storage.file_matches_storage(src, cached_item, 100, True, '50KB')

    def test_multi_part_s3_file_wrong_chunk_size_fails(self):
        # hash was computed with 50KB parts, but verification assumes the 8MB default: must not match
        src = pathlib.Path(__file__).parent / "resources/s3/md-10-big-CompressionInfo.db"
        data = src.read_bytes()
        expected_hash = s3_multipart_etag(data, 50 * 1024)
        cached_item = ManifestObject('path', len(data), expected_hash)
        assert not S3Storage.file_matches_storage(src, cached_item, 100, True)

    def test_gcs_file(self):
        # GCS hashes are b64 encoded
        cached_item = ManifestObject('path', 148906, '2c6QmQGESWilicKJiNY1NQ==')
        src = pathlib.Path(__file__).parent / "resources/gcs/lb-21-big-Index.db"
        assert GoogleStorage.file_matches_storage(src, cached_item, True)


if __name__ == '__main__':
    unittest.main()
