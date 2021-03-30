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

import pathlib
import unittest

from medusa.storage.google_storage import GoogleStorage
from medusa.storage.s3_storage import S3Storage


class RestoreNodeTest(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def test_single_part_s3_file(self):
        cached_item = {'MD5': '620c203520494bb92811fddc6d88cd65',
                       'size': 113651}
        src = pathlib.Path(__file__).parent / "resources/s3/md-10-big-CompressionInfo.db"
        assert S3Storage.file_matches_cache(src, cached_item, 100 * 1024 * 1024, True)

    def test_multi_part_s3_file(self):
        # Multi part hashes have a special structure, with the number of chunks at the end
        cached_item = {'MD5': 'e4344d1ea2b32372db7f7e1c81d154b9-1',
                       'size': 113651}
        src = pathlib.Path(__file__).parent / "resources/s3/md-10-big-CompressionInfo.db"
        assert S3Storage.file_matches_cache(src, cached_item, 100, True)

    def test_multi_part_s3_file_fail(self):
        # File size is below the multi part threshold, a single part hash will be computed
        cached_item = {'MD5': 'e4344d1ea2b32372db7f7e1c81d154b9-1',
                       'size': 113651}
        src = pathlib.Path(__file__).parent / "resources/s3/md-10-big-CompressionInfo.db"
        assert not S3Storage.file_matches_cache(src, cached_item, 100 * 1024 * 1024, True)

    def test_gcs_file(self):
        # GCS hashes are b64 encoded
        cached_item = {'MD5': '2c6QmQGESWilicKJiNY1NQ==',
                       'size': 148906}
        src = pathlib.Path(__file__).parent / "resources/gcs/lb-21-big-Index.db"
        assert GoogleStorage.file_matches_cache(src, cached_item, True)


if __name__ == '__main__':
    unittest.main()
