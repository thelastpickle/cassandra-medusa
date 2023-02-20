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

import dns.resolver
import dns.reversename
import ipaddress
import logging
import socket


def resolve_name(name):
    try:
        return socket.gethostbyname(name)
    except Exception as e:
        try:
            return socket.getaddrinfo(name, None)[0][4][0]
        except Exception:
            raise e


class HostnameResolver:
    def __init__(self, resolve_addresses, k8s_mode):
        self.resolve_addresses = resolve_addresses
        self.k8s_mode = k8s_mode

    def resolve_fqdn(self, ip_address=''):
        logging.info(f"Resolving ip address {ip_address}")
        ip_address_to_resolve = ip_address if ip_address != '' else resolve_name(socket.getfqdn())
        logging.info(f"ip address to resolve {ip_address_to_resolve}")
        if str(self.resolve_addresses) == "False":
            logging.debug("Not resolving {} as requested".format(ip_address_to_resolve))
            return ip_address_to_resolve

        hostname = socket.getfqdn(ip_address_to_resolve)
        if self.k8s_mode:
            hostname = self.compute_k8s_hostname(ip_address_to_resolve)
        logging.debug("Resolved {} to {}".format(ip_address_to_resolve, hostname))

        return hostname

    def compute_k8s_hostname(self, ip_address):
        if (self.is_ipv4(ip_address) or self.is_ipv6(ip_address)):
            reverse_name = dns.reversename.from_address(ip_address).to_text()
            fqdns = dns.resolver.resolve(reverse_name, 'PTR')
            for fqdn in fqdns:
                if not self.is_ipv4(fqdn.to_text().split('.')[0].replace('-', '.')) \
                   and not self.is_ipv6(fqdn.to_text().split('.')[0].replace('-', ':')):
                    return fqdn.to_text().split('.')[0]

        return ip_address

    def is_ipv4(self, ip_address):
        try:
            ipaddress.IPv4Network(ip_address)
            return True
        except ValueError:
            return False

    def is_ipv6(self, ip_address):
        try:
            ipaddress.IPv6Network(ip_address)
            return True
        except ValueError:
            return False
