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

import itertools
import types
import unittest

from pathlib import Path

from medusa.storage.google_storage import _group_by_parent, _is_in_folder


class RestoreNodeTest(unittest.TestCase):

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
