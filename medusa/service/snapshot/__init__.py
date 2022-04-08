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
import medusa.utils

from medusa.service.snapshot.ccm_snapshot_service import CCMSnapshotService
from medusa.service.snapshot.jolokia_snapshot_service import JolokiaSnapshotService
from medusa.service.snapshot.nodetool_snapshot_service import NodetoolSnapshotService
from medusa.service.snapshot.management_api_snapshot_service import ManagementAPISnapshotService


class SnapshotService(object):
    def __init__(self, *, config):
        self._config = config
        self.snapshot_service = self._create_snapshot_service()

    def _create_snapshot_service(self):
        if medusa.utils.evaluate_boolean(self._config.kubernetes.enabled if self._config.kubernetes else False):
            if medusa.utils.evaluate_boolean(self._config.kubernetes.use_mgmt_api):
                return ManagementAPISnapshotService(self._config.kubernetes)
            else:
                return JolokiaSnapshotService(self._config.kubernetes)
        elif medusa.utils.evaluate_boolean(self._config.cassandra.is_ccm):
            return CCMSnapshotService(None)
        else:
            return NodetoolSnapshotService(self._config.cassandra)
