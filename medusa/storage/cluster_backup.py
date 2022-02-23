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
import json
import operator


class ClusterBackup(object):

    def __init__(self, name, node_backups):
        self._name = name
        node_backups = list(node_backups)
        self._first_nodebackup = next(iter(node_backups))
        self.node_backups = {node_backup.fqdn: node_backup for node_backup in node_backups}
        # Cached values
        self._tokenmap = None
        self._schema = None

    def __repr__(self):
        return 'ClusterBackup(name={0.name})'.format(self)

    @property
    def name(self):
        return self._name

    @property
    def started(self):
        return min(map(operator.attrgetter('started'), self.node_backups.values()))

    @property
    def finished(self):
        if any(self.missing_nodes()):
            return None

        finished_timestamps = list(map(operator.attrgetter('finished'), self.node_backups.values()))
        if all(finished_timestamps):
            return max(finished_timestamps)
        else:
            return None

    @property
    def tokenmap(self):
        if self._tokenmap is None:
            self._tokenmap = json.loads(self._first_nodebackup.tokenmap)
        return self._tokenmap

    @property
    def schema(self):
        if self._schema is None:
            self._schema = self._first_nodebackup.schema
        return self._schema

    @property
    def backup_type(self):
        return "differential" if self._first_nodebackup.is_differential else "full"

    def is_complete(self):
        return not self.missing_nodes() and all(map(operator.attrgetter('finished'), self.node_backups.values()))

    def missing_nodes(self):
        return set(self.tokenmap.keys()) - set(self.node_backups.keys())

    def complete_nodes(self):
        return [node_backup
                for node_backup in self.node_backups.values()
                if node_backup.finished]

    def incomplete_nodes(self):
        return [node_backup
                for node_backup in self.node_backups.values()
                if node_backup.finished is None]

    def size(self):
        return sum(
            node_backup.size()
            for node_backup in self.node_backups.values()
            if node_backup.finished
        )

    def num_objects(self):
        return sum(
            node_backup.num_objects()
            for node_backup in self.node_backups.values()
            if node_backup.finished
        )
