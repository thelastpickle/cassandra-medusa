# -*- coding: utf-8 -*-
# Copyright 2021- Datastax, Inc. All rights reserved.
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

from cassandra.util import Version

from medusa.host_man import HostMan


class HostManTest(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def setUp(self):
        HostMan.reset()

    def test_get_release_version_not_set(self):
        with self.assertRaises(RuntimeError):
            HostMan.get_release_version()

    def test_set_release_version_missing_input(self):
        with self.assertRaises(RuntimeError):
            HostMan.set_release_version(None)

    def test_set_release_version_singleton_check(self):
        rv_1 = "1.2.3"
        HostMan.set_release_version(rv_1)

        with self.assertRaises(RuntimeError):
            HostMan()

    def test_get_release_version(self):
        rv_1 = "1.2.3"
        rv_2 = "5.6.7"
        rv_3 = "10.11.12"

        HostMan.set_release_version(rv_1)
        HostMan.set_release_version(rv_2)
        HostMan.set_release_version(rv_3)

        self.assertEqual(HostMan.get_release_version(), Version(rv_3))
