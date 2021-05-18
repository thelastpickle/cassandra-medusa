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
from unittest.mock import Mock

from medusa.host_man import HostMan


class HostManTest(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @staticmethod
    def test_get_release_version_no_host():
        assert HostMan.get_release_version(None) is None

    @staticmethod
    def test_get_release_version_invalid_host():
        invalid_host = "not-a-host"
        assert HostMan.get_release_version(invalid_host) is None

    @staticmethod
    def test_get_release_version_missing_host_release_version():
        host = Mock()
        assert HostMan.get_release_version(host) is None

    @staticmethod
    def test_get_release_versions_from_hosts():
        host_1 = "h1"
        host_2 = "h2"
        host_3 = "h3"
        rv_1 = "1.2.3.4"
        rv_2 = "5.6.7.8"
        rv_3 = "10.11.12.13"

        HostMan.set_release_version(host_1, rv_1)
        HostMan.set_release_version(host_2, rv_2)
        HostMan.set_release_version(host_3, rv_3)

        assert HostMan.get_release_version(host_1) == rv_1
        assert HostMan.get_release_version(host_2) == rv_2
