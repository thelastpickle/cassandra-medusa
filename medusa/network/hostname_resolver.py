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

import socket
import logging


class HostnameResolver():
    def __init__(self, resolve_addresses=True):
        self.resolve_addresses = resolve_addresses
        logging.debug

    def resolve_fqdn(self, ip_address=''):
        if str(self.resolve_addresses) == "False":
            logging.debug("Not resolving {} as requested".format(ip_address))
            return ip_address

        fqdn = socket.getfqdn(ip_address)
        logging.debug("Resolved {} to {}".format(ip_address, fqdn))
        return fqdn
