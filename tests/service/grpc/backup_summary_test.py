# -*- coding: utf-8 -*-
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
from unittest.mock import Mock

from medusa.service.grpc import medusa_pb2
from medusa.service.grpc.server import get_backup_summary
from medusa.storage import ClusterBackup


def make_node_backup(fqdn, finished, tokenmap):
    node_backup = Mock()
    node_backup.fqdn = fqdn
    node_backup.started = 1
    node_backup.finished = finished
    node_backup.tokenmap = json.dumps(tokenmap)
    node_backup.schema = ''
    node_backup.is_differential = True
    node_backup.size.return_value = 1
    node_backup.num_objects.return_value = 1
    return node_backup


def test_summary_ignores_unexpected_incomplete_node_backup():
    tokenmap = {
        'node1': {'tokens': [1], 'rack': 'rack1', 'dc': 'dc1'},
        'node2': {'tokens': [2], 'rack': 'rack1', 'dc': 'dc1'},
    }
    backup = ClusterBackup('backup1', [
        make_node_backup('node1', 10, tokenmap),
        make_node_backup('node2', 11, tokenmap),
        make_node_backup('old-node', None, tokenmap),
    ])

    summary = get_backup_summary(backup)

    assert summary.status == medusa_pb2.StatusType.SUCCESS
    assert summary.finishTime == 11
    assert summary.totalNodes == 2
    assert summary.finishedNodes == 2


def test_summary_does_not_count_unexpected_node_as_missing_node():
    tokenmap = {
        'node1': {'tokens': [1], 'rack': 'rack1', 'dc': 'dc1'},
        'node2': {'tokens': [2], 'rack': 'rack1', 'dc': 'dc1'},
        'node3': {'tokens': [3], 'rack': 'rack1', 'dc': 'dc1'},
    }
    backup = ClusterBackup('backup1', [
        make_node_backup('node1', 10, tokenmap),
        make_node_backup('node2', 11, tokenmap),
        make_node_backup('replacement-node', 12, tokenmap),
    ])

    summary = get_backup_summary(backup)

    assert summary.status == medusa_pb2.StatusType.IN_PROGRESS
    assert summary.finishTime == 0
    assert summary.totalNodes == 3
    assert summary.finishedNodes == 2
