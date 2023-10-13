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


class Nodetool(object):

    def __init__(self, cassandra_config):
        nodetool_executable = cassandra_config.nodetool_executable
        nodetool_flags = cassandra_config.nodetool_flags.split(" ") if cassandra_config.nodetool_flags else []
        self._nodetool = [nodetool_executable] + nodetool_flags
        if cassandra_config.nodetool_ssl == "true":
            self._nodetool += ['--ssl']
        if cassandra_config.nodetool_username is not None:
            self._nodetool += ['-u', cassandra_config.nodetool_username]
        if cassandra_config.nodetool_password is not None:
            self._nodetool += ['-pw', cassandra_config.nodetool_password]
        if cassandra_config.nodetool_password_file_path is not None:
            self._nodetool += ['-pwf', cassandra_config.nodetool_password_file_path]
        if cassandra_config.nodetool_host is not None:
            self._nodetool += ['-h', cassandra_config.nodetool_host]
        if cassandra_config.nodetool_port is not None:
            self._nodetool += ['-p', cassandra_config.nodetool_port]

    @property
    def nodetool(self):
        return self._nodetool
