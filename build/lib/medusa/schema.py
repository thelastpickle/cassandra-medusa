#! /usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright 2018 Spotify AB
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

import re


def parse_schema(schema):
    types = r'TABLE|KEYSPACE|INDEX|CUSTOM INDEX|MATERIALIZED VIEW|TYPE|AGGREGATE'
    statement_regex = r'\s*(CREATE) (' + types + r') ([a-zA-Z0-9_"]+)\.{0,1}([a-zA-Z0-9_"]*) '
    keyspaces = {}
    current_keyspace = ''
    statements = schema.replace('\n\n', '').replace('\n', ' ').split(';')
    for statement in statements:
        parsed_statement = re.match(statement_regex, statement, re.I | re.M)
        if parsed_statement is not None:
            if parsed_statement.group(2) == 'KEYSPACE':
                # Keyspace
                keyspaces[parsed_statement.group(3)] = {'create_statement': statement,
                                                        'tables': {},
                                                        'indices': {},
                                                        'materialized_views': {},
                                                        'udt': {},
                                                        'uda': {}}
                current_keyspace = parsed_statement.group(3)
            elif parsed_statement.group(2) == 'INDEX' or parsed_statement.group(2) == 'CUSTOM INDEX':
                keyspaces[current_keyspace]['indices'][parsed_statement.group(3)] = statement
            else:
                object_kind = None
                if parsed_statement.group(2) == 'TABLE':
                    object_kind = 'tables'
                elif parsed_statement.group(2) == 'AGGREGATE':
                    object_kind = 'uda'
                elif parsed_statement.group(2) == 'TYPE':
                    object_kind = 'udt'
                elif 'CREATE MATERIALIZED VIEW' in statement:
                    # Yikes! Materialized view (╯°□°)╯︵ ┻━┻
                    object_kind = 'materialized_views'
                if object_kind is not None:
                    keyspaces[parsed_statement.group(3)][object_kind][parsed_statement.group(4)] = statement
    return keyspaces
