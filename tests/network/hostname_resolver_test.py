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
import dns.resolver

from medusa.network.hostname_resolver import HostnameResolver

mock_fqdn = "k8ssandra-dc1-default-sts-0.k8ssandra-dc1-all-pods-service.k8ssandra2022040617103007.svc.cluster.local"
mock_invalid_fqdn = "127-0-0-1.k8ssandra-dc1-all-pods-service.k8ssandra2022040617103007.svc.cluster.local"
mock_invalid_ipv6_fqdn = "2001-db8-85a3-8d3-1319-8a2e-370-7348.k8ssandra-dc1-all-pods-service.test.svc.cluster.local"
mock_alias = "k8ssandra-dc1-default-sts-0"
mock_resolve = Mock()
mock_resolve.to_text = MagicMock(return_value=mock_fqdn)
mock_resolve_invalid = Mock()
mock_resolve_invalid.to_text = MagicMock(return_value=mock_invalid_fqdn)
mock_resolve_invalid_ipv6 = Mock()
mock_resolve_invalid_ipv6.to_text = MagicMock(return_value=mock_invalid_ipv6_fqdn)
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
                    mock_resolver.resolve.return_value = [mock_resolve_invalid, mock_resolve_invalid_ipv6, mock_resolve]
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
                    mock_resolver.resolve.return_value = [mock_resolve_invalid_ipv6]
                    mock_reverser.reverse.return_value = mock_reverse
                    hostname_resolver = HostnameResolver(resolve_addresses=True, k8s_mode=True)
                    self.assertNotEqual(
                        mock_alias,
                        hostname_resolver.resolve_fqdn("127.0.0.1"))

    def test_valid_address_for_kubernetes_ipv6(self):
        with patch('medusa.network.hostname_resolver.socket') as mock_socket:
            with patch('medusa.network.hostname_resolver.dns.resolver') as mock_resolver:
                with patch('medusa.network.hostname_resolver.dns.reversename') as mock_reverser:
                    mock_socket.getfqdn.return_value = mock_invalid_fqdn
                    mock_resolver.resolve.return_value = [mock_resolve]
                    mock_reverser.reverse.return_value = mock_reverse
                    hostname_resolver = HostnameResolver(resolve_addresses=True, k8s_mode=True)
                    self.assertEqual(
                        mock_alias,
                        hostname_resolver.resolve_fqdn("::1"))

    def test_invalid_address_for_kubernetes_ipv6(self):
        with patch('medusa.network.hostname_resolver.socket') as mock_socket:
            with patch('medusa.network.hostname_resolver.dns.resolver') as mock_resolver:
                with patch('medusa.network.hostname_resolver.dns.reversename') as mock_reverser:
                    mock_socket.getfqdn.return_value = mock_invalid_fqdn
                    mock_resolver.resolve.return_value = [mock_resolve_invalid_ipv6]
                    mock_reverser.reverse.return_value = mock_reverse
                    hostname_resolver = HostnameResolver(resolve_addresses=True, k8s_mode=True)
                    self.assertNotEqual(
                        mock_alias,
                        hostname_resolver.resolve_fqdn("::1"))

    def test_address_no_kubernetes(self):
        with patch('medusa.network.hostname_resolver.socket') as mock_socket:
            mock_socket.getfqdn.return_value = mock_fqdn
            hostname_resolver = HostnameResolver(resolve_addresses=True, k8s_mode=False)
            self.assertEqual(
                mock_fqdn,
                hostname_resolver.resolve_fqdn("127.0.0.1"))

    def test_kubernetes_nxdomain_fallback(self):
        """Test that NXDOMAIN exception is handled gracefully and falls back to IP address"""
        with patch('medusa.network.hostname_resolver.socket') as mock_socket:
            with patch('medusa.network.hostname_resolver.dns.resolver') as mock_resolver:
                with patch('medusa.network.hostname_resolver.dns.reversename') as mock_reverser:
                    mock_socket.getfqdn.return_value = mock_fqdn
                    mock_resolver.NXDOMAIN = dns.resolver.NXDOMAIN
                    mock_resolver.NoAnswer = dns.resolver.NoAnswer
                    mock_resolver.Timeout = dns.resolver.Timeout
                    mock_resolver.resolve.side_effect = dns.resolver.NXDOMAIN()
                    mock_reverser.from_address.return_value = mock_reverse
                    hostname_resolver = HostnameResolver(resolve_addresses=True, k8s_mode=True)
                    # When PTR record doesn't exist, should fall back to IP address
                    self.assertEqual(
                        "127.0.0.1",
                        hostname_resolver.resolve_fqdn("127.0.0.1"))

    def test_kubernetes_no_answer_fallback(self):
        """Test that NoAnswer exception is handled gracefully and falls back to IP address"""
        with patch('medusa.network.hostname_resolver.socket') as mock_socket:
            with patch('medusa.network.hostname_resolver.dns.resolver') as mock_resolver:
                with patch('medusa.network.hostname_resolver.dns.reversename') as mock_reverser:
                    mock_socket.getfqdn.return_value = mock_fqdn
                    mock_resolver.NXDOMAIN = dns.resolver.NXDOMAIN
                    mock_resolver.NoAnswer = dns.resolver.NoAnswer
                    mock_resolver.Timeout = dns.resolver.Timeout
                    mock_resolver.resolve.side_effect = dns.resolver.NoAnswer()
                    mock_reverser.from_address.return_value = mock_reverse
                    hostname_resolver = HostnameResolver(resolve_addresses=True, k8s_mode=True)
                    # When DNS query has no answer, should fall back to IP address
                    self.assertEqual(
                        "127.0.0.1",
                        hostname_resolver.resolve_fqdn("127.0.0.1"))

    def test_kubernetes_timeout_fallback(self):
        """Test that Timeout exception is handled gracefully and falls back to IP address"""
        with patch('medusa.network.hostname_resolver.socket') as mock_socket:
            with patch('medusa.network.hostname_resolver.dns.resolver') as mock_resolver:
                with patch('medusa.network.hostname_resolver.dns.reversename') as mock_reverser:
                    mock_socket.getfqdn.return_value = mock_fqdn
                    mock_resolver.NXDOMAIN = dns.resolver.NXDOMAIN
                    mock_resolver.NoAnswer = dns.resolver.NoAnswer
                    mock_resolver.Timeout = dns.resolver.Timeout
                    mock_resolver.resolve.side_effect = dns.resolver.Timeout()
                    mock_reverser.from_address.return_value = mock_reverse
                    hostname_resolver = HostnameResolver(resolve_addresses=True, k8s_mode=True)
                    # When DNS query times out, should fall back to IP address
                    self.assertEqual(
                        "127.0.0.1",
                        hostname_resolver.resolve_fqdn("127.0.0.1"))

    def test_kubernetes_hostname_skips_ptr_lookup(self):
        """Test that non-IP hostnames skip PTR lookups in k8s mode"""
        with patch('medusa.network.hostname_resolver.socket') as mock_socket:
            with patch('medusa.network.hostname_resolver.dns.resolver') as mock_resolver:
                # Set up hostname (not an IP)
                test_hostname = "cassandra-1-dc1-default-sts-0"
                mock_socket.getfqdn.return_value = test_hostname
                hostname_resolver = HostnameResolver(resolve_addresses=True, k8s_mode=True)

                # Should return hostname without attempting PTR lookup
                result = hostname_resolver.resolve_fqdn(test_hostname)

                # dns.resolver.resolve should never be called for non-IP hostnames
                mock_resolver.resolve.assert_not_called()
                # Result should be the original hostname
                self.assertEqual(test_hostname, result)
