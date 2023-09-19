#! /usr/bin/env python
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

from datetime import datetime
from medusa.storage import Storage


TIMESTAMP_FORMAT = '%Y-%m-%d %H:%M:%S'


def get_backups(storage, config, show_all):

    cluster_backups = sorted(
        storage.list_cluster_backups(),
        key=lambda b: b.started
    )
    if not show_all:
        cluster_backups = filter(
            lambda cluster_backup: config.storage.fqdn in cluster_backup.node_backups,
            cluster_backups
        )

    return cluster_backups


def list_backups(config, show_all):
    with Storage(config=config.storage) as storage:
        list_backups_w_storage(config, show_all, storage)


def list_backups_w_storage(config, show_all, storage):
    cluster_backups = get_backups(storage, config, show_all)
    seen_incomplete_backup = False
    for cluster_backup in cluster_backups:
        finished = cluster_backup.finished
        if finished is not None:
            finished = datetime.fromtimestamp(finished).strftime(TIMESTAMP_FORMAT)
        else:
            seen_incomplete_backup = True
            finished_nodes = len(cluster_backup.complete_nodes())
            total_nodes = len(cluster_backup.tokenmap)
            finished = 'Incomplete [{} of {} nodes finished]'.format(
                finished_nodes,
                total_nodes
            )
        started = datetime.fromtimestamp(cluster_backup.started).strftime(TIMESTAMP_FORMAT)
        print('{} (started: {}, finished: {})'.format(cluster_backup.name, started, finished))

    if seen_incomplete_backup:
        print('')
        print('Incomplete backups found. You can run "medusa status --backup-name <name>" for more details')
    return cluster_backups
