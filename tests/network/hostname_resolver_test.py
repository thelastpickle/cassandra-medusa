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
from unittest.mock import patch, MagicMock, Mock

from medusa.network.hostname_resolver import HostnameResolver

mock_fqdn = "k8ssandra-dc1-default-sts-0.k8ssandra-dc1-all-pods-service.k8ssandra2022040617103007.svc.cluster.local"
mock_invalid_fqdn = "127-0-0-1.k8ssandra-dc1-all-pods-service.k8ssandra2022040617103007.svc.cluster.local"
mock_alias = "k8ssandra-dc1-default-sts-0"
mock_resolve = Mock()
mock_resolve.to_text = MagicMock(return_value=mock_fqdn)
mock_resolve_invalid = Mock()
mock_resolve_invalid.to_text = MagicMock(return_value=mock_invalid_fqdn)
mock_reverse = Mock()
mock_reverse.to_text = MagicMock(return_value="1.0.0.127-in-addr.arpa.")


class HostnameResolverTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def test_no_address_resolving(self):
        hostname_resolver = HostnameResolver(resolve_addresses=False, k8s_mode=False)
        self.assertEqual("127.0.0.1", hostname_resolver.resolve_fqdn("127.0.0.1"))

    def test_address_resolving(self):
        hostname_resolver = HostnameResolver(resolve_addresses=True, k8s_mode=False)
        self.assertNotEqual("127.0.0.1", hostname_resolver.resolve_fqdn("127.0.0.1"))

    def test_address_for_kubernetes(self):
        with patch('medusa.network.hostname_resolver.socket') as mock_socket:
            with patch('medusa.network.hostname_resolver.dns.resolver') as mock_resolver:
                with patch('medusa.network.hostname_resolver.dns.reversename') as mock_reverser:
                    mock_socket.getfqdn.return_value = mock_fqdn
                    mock_resolver.resolve.return_value = [mock_resolve]
                    mock_reverser.reverse.return_value = mock_reverse
                    hostname_resolver = HostnameResolver(resolve_addresses=True, k8s_mode=True)
                    self.assertEqual(
                        mock_alias,
                        hostname_resolver.resolve_fqdn("127.0.0.1"))

    def test_invalid_address_for_kubernetes(self):
        with patch('medusa.network.hostname_resolver.socket') as mock_socket:
            with patch('medusa.network.hostname_resolver.dns.resolver') as mock_resolver:
                with patch('medusa.network.hostname_resolver.dns.reversename') as mock_reverser:
                    mock_socket.getfqdn.return_value = mock_invalid_fqdn
                    mock_resolver.resolve.return_value = [mock_resolve_invalid]
                    mock_reverser.reverse.return_value = mock_reverse
                    hostname_resolver = HostnameResolver(resolve_addresses=True, k8s_mode=True)
                    self.assertNotEqual(
                        mock_alias,
                        hostname_resolver.resolve_fqdn("127.0.0.1"))

    def test_address_no_kubernetes(self):
        with patch('medusa.network.hostname_resolver.socket') as mock_socket:
            mock_socket.getfqdn.return_value = mock_fqdn
            hostname_resolver = HostnameResolver(resolve_addresses=True, k8s_mode=False)
            self.assertEqual(
                mock_fqdn,
                hostname_resolver.resolve_fqdn("127.0.0.1"))
