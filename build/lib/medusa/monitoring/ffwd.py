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

import ffwd
import json
import socket

from medusa.monitoring.abstract import AbstractMonitoring


class FfwdMonitoring(AbstractMonitoring):

    def __init__(self, config):
        super().__init__(config)
        self.ffwd_client = ffwd.FFWD(transport=MedusaTransport)

    def send(self, tags, value):
        if len(tags) != 3:
            raise AssertionError("FFWD monitoring implementation needs 3 tags: 'key', 'what' and 'backup_name'")

        key, what, backup_name = tags
        metric = self.ffwd_client.metric(key=key, what=what, backupname=backup_name)
        metric.send(value)


# MedusaTransport is very similar to the original UDPTransport
# The difference is that here we call .endcode() on the string before putting it to socket
# It's a python3 thing :(
class MedusaTransport:
    def __init__(self, **kw):
        self._host = kw.get('host', '127.0.0.1')
        self._port = kw.get('port', 19000)
        self._t = (self._host, self._port)
        self._s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def send_json(self, message):
        self._s.sendto(json.dumps(message).encode(), self._t)
