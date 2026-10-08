# -*- coding: utf-8 -*-
# Copyright 2021 DataStax, Inc.
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

import unittest

import medusa.utils


class RestoreNodeTest(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def test_null_if_empty(self):
        assert medusa.utils.null_if_empty("") is None
        assert medusa.utils.null_if_empty(None) is None
        assert medusa.utils.null_if_empty("test") == "test"
        assert medusa.utils.null_if_empty(1) == 1

    def test_redact_password_in_command_string(self):
        command = 'nodetool -u cassandra -pw s3cret -pwf /etc/cassandra/jmx.password snapshot -t medusa-backup1'
        assert medusa.utils.redact_password(command) == \
            'nodetool -u cassandra -pw *** -pwf /etc/cassandra/jmx.password snapshot -t medusa-backup1'
        assert medusa.utils.redact_password('nodetool --password s3cret status') == 'nodetool --password *** status'
        assert medusa.utils.redact_password('nodetool --password=s3cret status') == 'nodetool --password=*** status'

    def test_redact_password_in_command_list(self):
        command = ['nodetool', '-u', 'cassandra', '-pw', 's3cret with spaces', '-pwf', '/etc/cassandra/jmx.password',
                   'snapshot', '-t', 'medusa-backup1']
        assert medusa.utils.redact_password(command) == [
            'nodetool', '-u', 'cassandra', '-pw', '***', '-pwf', '/etc/cassandra/jmx.password',
            'snapshot', '-t', 'medusa-backup1'
        ]
        # The command that gets executed must not be modified
        assert command[4] == 's3cret with spaces'
        assert medusa.utils.redact_password(['nodetool', '--password', 's3cret']) == ['nodetool', '--password', '***']
        assert medusa.utils.redact_password(['nodetool', '--password=s3cret']) == ['nodetool', '--password=***']

    def test_redact_password_leaves_commands_without_password_untouched(self):
        command = 'mkdir -p /tmp/medusa-job; cd /tmp/medusa-job && medusa-wrapper medusa -vvv backup-node --mode full'
        assert medusa.utils.redact_password(command) == command
        command = ['nodetool', '-pwf', '/etc/cassandra/jmx.password', 'clearsnapshot', '-t', 'medusa-backup1']
        assert medusa.utils.redact_password(command) == command
        assert medusa.utils.redact_password(None) is None


if __name__ == '__main__':
    unittest.main()
