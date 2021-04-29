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
from unittest.mock import MagicMock

from cassandra.pool import Host

from medusa.host_man import HostMan


class HostManTest(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @staticmethod
    def test_get_release_version():
        host = Host(endpoint=MagicMock(), conviction_policy_factory=MagicMock(),
                    host_id="test-host-id")
        host.release_version = "test-rel-ver"

        rel_version = HostMan.get_release_version(host)
        assert rel_version
        assert rel_version == "test-rel-ver"

    @staticmethod
    def test_get_release_version_no_host():
        assert HostMan.get_release_version(None) is None

    @staticmethod
    def test_get_release_version_missing_host_id():
        host = Host(endpoint=MagicMock(), conviction_policy_factory=MagicMock(), host_id=None)
        host.release_version = "test-rel-ver"

        assert HostMan.get_release_version(host) is None

    @staticmethod
    def test_get_release_version_missing_host_release_version():
        host = Host(endpoint=MagicMock(), conviction_policy_factory=MagicMock(), host_id="test-host-id")
        host.release_version = None

        assert HostMan.get_release_version(host) is None

    @staticmethod
    def test_get_release_versions_from_hosts():
        host_1 = Host(endpoint=MagicMock(), conviction_policy_factory=MagicMock(), host_id="test-host_1-id")
        host_1.release_version = "test-host_1-version"

        host_2 = Host(endpoint=MagicMock(), conviction_policy_factory=MagicMock(), host_id="test-host_2-id")
        host_2.release_version = "test-host_2-version"

        assert HostMan.get_release_version(host_1) == "test-host_1-version"
        assert HostMan.get_release_version(host_2) == "test-host_2-version"
