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

from medusa.config import MedusaConfig, StorageConfig, _namedtuple_from_dict
from medusa import filtering


class FilteringTest(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def setUp(self):
        config = configparser.ConfigParser(interpolation=None)
        config['storage'] = {
            'host_file_separator': ','
        }
        self.config = MedusaConfig(
            file_path=None,
            storage=_namedtuple_from_dict(StorageConfig, config['storage']),
            monitoring={},
            cassandra=None,
            ssh=None,
            checks=None,
            logging=None,
            grpc=None,
            kubernetes=None
        )

    def test_get_sections_to_restore(self):

        # nothing skipped, both tables make it to restore
        keep_keyspaces = {}
        keep_tables = {}
        manifest = [
            {'keyspace': 'k1', 'columnfamily': 't1', 'objects': []},
            {'keyspace': 'k2', 'columnfamily': 't2', 'objects': []}
        ]

        to_restore, ignored = filtering.filter_fqtns(keep_keyspaces, keep_tables, json.dumps(manifest))
        self.assertEqual({'k1.t1', 'k2.t2'}, to_restore)
        self.assertEqual(set(), ignored)

        # skipping one table (must be specified as a fqtn)
        keep_keyspaces = {}
        keep_tables = {'k2.t2'}
        manifest = [
            {'keyspace': 'k1', 'columnfamily': 't1', 'objects': []},
            {'keyspace': 'k2', 'columnfamily': 't2', 'objects': []}
        ]
        to_restore, ignored = filtering.filter_fqtns(keep_keyspaces, keep_tables, json.dumps(manifest))
        self.assertEqual({'k2.t2'}, to_restore)
        self.assertEqual({'k1.t1'}, ignored)

        # saying only table name doesn't cause a keep
        keep_keyspaces = {}
        keep_tables = {'t2'}
        manifest = [
            {'keyspace': 'k1', 'columnfamily': 't1', 'objects': []},
            {'keyspace': 'k2', 'columnfamily': 't2', 'objects': []}
        ]
        to_restore, ignored = filtering.filter_fqtns(keep_keyspaces, keep_tables, json.dumps(manifest))
        self.assertEqual(set(), to_restore)
        self.assertEqual({'k1.t1', 'k2.t2'}, ignored)

        # keeping the whole keyspace
        keep_keyspaces = {'k2'}
        keep_tables = {}
        manifest = [
            {'keyspace': 'k1', 'columnfamily': 't1', 'objects': []},
            {'keyspace': 'k2', 'columnfamily': 't2', 'objects': []},
            {'keyspace': 'k2', 'columnfamily': 't3', 'objects': []}
        ]
        to_restore, ignored = filtering.filter_fqtns(keep_keyspaces, keep_tables, json.dumps(manifest))
        self.assertEqual({'k2.t2', 'k2.t3'}, to_restore)
        self.assertEqual({'k1.t1'}, ignored)

    def test_get_sections_to_restore_with_cfids(self):

        # kept tables must work also if the manifest has cfids
        keep_keyspaces = {}
        keep_tables = {'k2.t2'}
        manifest = [
            {'keyspace': 'k1', 'columnfamily': 't1-bigBadCfId', 'objects': []},
            {'keyspace': 'k2', 'columnfamily': 't2-81ffe430e50c11e99f91a15641db358f', 'objects': []},
        ]
        to_restore, ignored = filtering.filter_fqtns(keep_keyspaces, keep_tables, json.dumps(manifest))
        self.assertEqual({'k2.t2-81ffe430e50c11e99f91a15641db358f'}, to_restore)
        self.assertEqual({'k1.t1'}, ignored)

    def test_filter_out_system_keyspaces_if_requested(self):

        # system tables should be kept by default
        keep_keyspaces = {}
        keep_tables = {'k2.t2'}
        manifest = [
            {'keyspace': 'system', 'columnfamily': 'hints-2666e20573ef38b390fefecf96e8f0c7', 'objects': []},
            {'keyspace': 'system_distributed', 'columnfamily': 'repair_history', 'objects': []},
            {'keyspace': 'k2', 'columnfamily': 't2', 'objects': []},
        ]
        to_restore, ignored = filtering.filter_fqtns(keep_keyspaces, keep_tables, json.dumps(manifest))
        self.assertEqual({'system.hints-2666e20573ef38b390fefecf96e8f0c7', 'system_distributed.repair_history',
                          'k2.t2'}, to_restore)
        self.assertEqual(set(), ignored)

        # system tables should not be kept if requested
        keep_keyspaces = {}
        keep_tables = {'k2.t2'}
        manifest = [
            {'keyspace': 'system', 'columnfamily': 'hints-2666e20573ef38b390fefecf96e8f0c7', 'objects': []},
            {'keyspace': 'system_distributed', 'columnfamily': 'repair_history', 'objects': []},
            {'keyspace': 'k2', 'columnfamily': 't2', 'objects': []},
        ]
        to_restore, ignored = filtering.filter_fqtns(keep_keyspaces, keep_tables, json.dumps(manifest), True)
        self.assertEqual({'k2.t2'}, to_restore)
        self.assertEqual({'system.hints', 'system_distributed.repair_history'}, ignored)
