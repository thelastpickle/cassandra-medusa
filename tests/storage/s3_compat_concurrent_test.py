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
import multiprocessing
import unittest
from unittest.mock import MagicMock

from medusa.storage.s3_compat_storage.concurrent import StorageJob
from medusa.storage.s3_compat_storage.concurrent import human_readable_size


class S3CompatConcurrentTest(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @staticmethod
    def test_storage_job_init():
        storage_job_1 = StorageJob(storage=MagicMock(), func=MagicMock(), max_workers=1)
        assert storage_job_1.max_workers == 1

        storage_job_2 = StorageJob(storage=MagicMock(), func=MagicMock(), max_workers=2)
        assert storage_job_2.max_workers == 2

        storage_job_3 = StorageJob(storage=MagicMock(), func=MagicMock(), max_workers=None)
        assert storage_job_3.max_workers == int(multiprocessing.cpu_count())

    @staticmethod
    def test_human_readable_size():
        assert human_readable_size(2.123) == "2.123B"
        assert human_readable_size(2048.123) == "2.000KiB"
        assert human_readable_size(20480.123) == "20.000KiB"
        assert human_readable_size(2048000.123) == "1.953MiB"
        assert human_readable_size(20480000000.123) == "19.073GiB"
        assert human_readable_size(20480000000000.123) == "18.626TiB"
        assert human_readable_size(2048.0, 1) == "2.0KiB"
        assert human_readable_size(2048.01, 2) == "2.00KiB"


if __name__ == '__main__':
    unittest.main()
