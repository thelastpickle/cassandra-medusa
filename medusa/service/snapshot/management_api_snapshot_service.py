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
import json
import requests

from medusa.service.snapshot.abstract_snapshot_service import AbstractSnapshotService


class ManagementAPISnapshotService(AbstractSnapshotService):

    def __init__(self, config):
        self.config = config
        self.session = requests.Session()
        if self.config.tls_cert is not None and len(self.config.tls_cert) > 0:
            self.session.verify = False
            self.session.cert = (self.config.tls_cert, self.config.tls_key)
            if self.config.cassandra_url.startswith('http://'):
                self.config.cassandra_url = self.config.cassandra_url.replace('http://', 'https://')

    def create_snapshot(self, *, tag):
        # get the Cassandra URL to POST the request
        post_url = self.config.cassandra_url
        # build the POST data
        data = {
            "snapshot_name": tag
        }
        # send the request
        response = self.session.post(post_url, data=json.dumps(data), headers={"Content-Type": "application/json"})
        # raise an Exception if the POST was not successful
        if response.status_code != 200:
            err_msg = "failed to create snapshot: {}".format(response.text)
            raise requests.RequestException(err_msg)

    def delete_snapshot(self, *, tag):
        # get the Cassandra URL to DELETE
        delete_url = self.config.cassandra_url + "?snapshotNames=" + tag
        # send the DELETE
        response = self.session.delete(delete_url)
        # raise an Exception if the DELETE was not successful
        if response.status_code != 200:
            err_msg = "failed to delete snapshot: {}".format(response.text)
            raise requests.RequestException(err_msg)
