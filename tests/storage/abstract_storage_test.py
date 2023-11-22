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

import unittest

from medusa.storage.abstract_storage import AbstractStorage


class AttributeDict(dict):
    __slots__ = ()
    __getattr__ = dict.__getitem__
    __setattr__ = dict.__setitem__


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
