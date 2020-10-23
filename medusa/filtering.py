# -*- coding: utf-8 -*-
# Copyright 2020- Datastax, Inc. All rights reserved.
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

import json


def filter_fqtns(keep_keyspaces, keep_tables, manifest, ignore_system_keyspaces=False):
    retained = set()
    ignored = set()
    manifest = json.loads(manifest)

    for section in manifest:
        ks = section['keyspace']
        # in manifest, the table names have cfids, but from CLI we get it without
        # we need to take care and use both
        t = section['columnfamily'].split('-')[0]

        fqtn = '{}.{}'.format(ks, t)
        fqtn_with_id = '{}.{}'.format(section['keyspace'], section['columnfamily'])

        # if not keyspaces / tables were specified, we keep everything
        if len(keep_keyspaces) == 0 and len(keep_tables) == 0:
            retained.add(fqtn_with_id)
            continue

        # if the whole keyspace is a keep, or a system keyspace (C* internal)
        if keep_or_system_namespace(ks, keep_keyspaces, ignore_system_keyspaces):
            retained.add(fqtn_with_id)
            continue

        # if just the table is a keep
        if fqtn in keep_tables:
            retained.add(fqtn_with_id)
            continue

        ignored.add('{}.{}'.format(ks, t))

    return retained, ignored


def keep_or_system_namespace(ks, keep_keyspaces, ignore_system_keyspaces):
    SYSTEM_KEYSPACES = ['system', 'system_schema', 'system_auth', 'system_distributed']

    return ks in keep_keyspaces or (not ignore_system_keyspaces and (ks in SYSTEM_KEYSPACES))
