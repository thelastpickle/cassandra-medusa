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

import configparser
import json
import unittest

from pathlib import Path
from unittest.mock import MagicMock
from unittest.mock import Mock

from medusa.restore_cluster import RestoreJob, expand_repeatable_option
from medusa.config import MedusaConfig, StorageConfig, _namedtuple_from_dict


class RestoreClusterTest(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def setUp(self):
        config = configparser.ConfigParser(interpolation=None)
        config['storage'] = {
            'host_file_separator': ','
        }
        self.config = MedusaConfig(
            storage=_namedtuple_from_dict(StorageConfig, config['storage']),
            monitoring={},
            cassandra=None,
            ssh=None,
            restore=None,
            logging=None
        )

    # Test that we can properly associate source and target nodes for restore using a host list
    def test_populate_ringmap(self):
        node_backups = list()
        node_backups.append(Mock())
        with open("tests/resources/restore_cluster_tokenmap.json", 'r') as f:
            cluster_backup = Mock()
            tokenmap = json.loads(f.read())
            cluster_backup.tokenmap.return_value = tokenmap
            host_list = "tests/resources/restore_cluster_host_list.txt"
            restoreJob = RestoreJob(cluster_backup, self.config, Path('/tmp'), host_list, None, False, False, None)
            restoreJob._populate_hostmap()

        self.assertEqual(restoreJob.host_map["node1.mydomain.net"]['source'], ["node1.mydomain.net"])
        self.assertEqual(restoreJob.host_map["node2.mydomain.net"]['source'], ["node2.mydomain.net"])
        self.assertEqual(restoreJob.host_map["node3.mydomain.net"]['source'], ["node4.mydomain.net"])

    # Test that we can properly associate source and target nodes for restore using a token map
    def test_populate_tokenmap(self):
        node_backups = list()
        node_backups.append(Mock())
        with open("tests/resources/restore_cluster_tokenmap.json", 'r') as f:
            with open("tests/resources/restore_cluster_tokenmap.target.json", 'r') as f_target:
                tokenmap = json.loads(f.read())
                cluster_backup = MagicMock()
                restoreJob = RestoreJob(
                    cluster_backup, self.config, Path('/tmp'), None, "node1.mydomain.net", False, False, None
                )

                target_tokenmap = json.loads(f_target.read())
                restoreJob._populate_ringmap(tokenmap, target_tokenmap)
                assert restoreJob.use_sstableloader is False

        self.assertEqual(restoreJob.host_map["node4.mydomain.net"]['source'], ["node1.mydomain.net"])
        self.assertEqual(restoreJob.host_map["node5.mydomain.net"]['source'], ["node2.mydomain.net"])
        self.assertEqual(restoreJob.host_map["node6.mydomain.net"]['source'], ["node3.mydomain.net"])

    # Test that we can't restore the cluster if the source and target topology have different sizes
    def test_populate_tokenmap_fail(self):
        node_backups = list()
        node_backups.append(Mock())
        with open("tests/resources/restore_cluster_tokenmap.json", 'r') as f:
            with open("tests/resources/restore_cluster_tokenmap.fail.json", 'r') as f_target:
                tokenmap = json.loads(f.read())
                cluster_backup = MagicMock()
                restoreJob = RestoreJob(
                    cluster_backup, self.config, Path('/tmp'), None, "node1.mydomain.net", False, False, None
                )

                target_tokenmap = json.loads(f_target.read())
                restoreJob._populate_ringmap(tokenmap, target_tokenmap)
                # topologies are different, which forces the use of the sstableloader
                assert restoreJob.use_sstableloader is True

    # Test that we can't restore the cluster if the source and target topology have different tokens
    def test_populate_tokenmap_fail_tokens(self):
        node_backups = list()
        node_backups.append(Mock())
        with open("tests/resources/restore_cluster_tokenmap.json", 'r') as f:
            with open("tests/resources/restore_cluster_tokenmap.fail_tokens.json", 'r') as f_target:
                tokenmap = json.loads(f.read())
                cluster_backup = MagicMock()
                restoreJob = RestoreJob(
                    cluster_backup, self.config, Path('/tmp'), None, "node1.mydomain.net", False, False, None
                )

                target_tokenmap = json.loads(f_target.read())
                restoreJob._populate_ringmap(tokenmap, target_tokenmap)
                # topologies are different, which forces the use of the sstableloader
                assert restoreJob.use_sstableloader is True

    def test_populate_ringmap_catches_mismatching_tokens_when_using_vnodes(self):
        node_backups = list()
        node_backups.append(Mock())
        with open("tests/resources/restore_cluster_tokenmap_vnodes.json", 'r') as f:
            with open("tests/resources/restore_cluster_tokenmap_vnodes_target_fail.json", 'r') as f_target:
                tokenmap = json.loads(f.read())
                cluster_backup = MagicMock()
                restoreJob = RestoreJob(
                    cluster_backup, self.config, Path('/tmp'), None, "node1.mydomain.net", False, False, None
                )

                target_tokenmap = json.loads(f_target.read())
                restoreJob._populate_ringmap(tokenmap, target_tokenmap)
                # topologies are different, which forces the use of the sstableloader
                assert restoreJob.use_sstableloader is True

    def test_expand_repeatable_option(self):

        option, values = 'keyspace', {}
        result = expand_repeatable_option(option, values)
        self.assertEqual('', result)

        option, values = 'keyspace', {'k1'}
        result = expand_repeatable_option(option, values)
        self.assertEqual('--keyspace k1', result)

        option, values = 'keyspace', {'k1', 'k2'}
        result = expand_repeatable_option(option, sorted(list(values)))
        self.assertEqual('--keyspace k1 --keyspace k2', result)


if __name__ == '__main__':
    unittest.main()
