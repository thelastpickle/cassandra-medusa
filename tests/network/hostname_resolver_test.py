# -*- coding: utf-8 -*-
# Copyright 2020- Datastax, Inc. All rights reserved.
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

from medusa.network.hostname_resolver import HostnameResolver


class HostnameResolverTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def test_no_address_resolving(self):
        hostname_resolver = HostnameResolver(resolve_addresses=False)
        self.assertEqual("127.0.0.1", hostname_resolver.resolve_fqdn("127.0.0.1"))

    def test_address_resolving(self):
        hostname_resolver = HostnameResolver(resolve_addresses=True)
        self.assertNotEqual("127.0.0.1", hostname_resolver.resolve_fqdn("127.0.0.1"))
