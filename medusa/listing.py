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


async def get_backups(storage, config, show_all):

    cluster_backups = sorted(
        await storage.list_cluster_backups(),
        key=lambda b: b.started
    )
    if not show_all:
        cluster_backups = filter(
            lambda cluster_backup: config.storage.fqdn in cluster_backup.node_backups,
            cluster_backups
        )

    return cluster_backups


async def list_backups(config, show_all):
    async with Storage(config=config.storage) as storage:
        await list_backups_w_storage(config, show_all, storage)


class BackupStatus(object):
    name = None
    finished = None
    started = None
    complete = None
    finished_nodes = None
    total_nodes = None

    def __init__(self, name, started, finished, complete):
        self.name = name
        self.started = started
        self.finished = finished
        self.complete = complete


async def get_backup_statuses(cluster_backup):

    started = cluster_backup.started
    finished = await cluster_backup.finished
    complete = True

    if finished is None:
        complete = False
        finished_nodes = len(cluster_backup.complete_nodes())
        total_nodes = len(await cluster_backup.tokenmap)
        finished = 'Incomplete [{} of {} nodes finished]'.format(
            finished_nodes,
            total_nodes
        )

    return BackupStatus(cluster_backup.name, started, finished, complete)


async def list_backups_w_storage(config, show_all, storage):
    cluster_backups = await get_backups(storage, config, show_all)
    coros = [get_backup_statuses(cluster_backup) for cluster_backup in cluster_backups]
    import asyncio
    backup_statuses = await asyncio.gather(*coros)

    for bs in sorted(backup_statuses, key=lambda b: b.started):
        print('{} (started: {}, finished: {})'.format(
            bs.name,
            datetime.fromtimestamp(bs.started).strftime(TIMESTAMP_FORMAT),
            bs.finished if not bs.complete else datetime.fromtimestamp(bs.finished).strftime(TIMESTAMP_FORMAT)
        ))

    if any([not bs.complete for bs in backup_statuses]):
        print('')
        print('Incomplete backups found. You can run "medusa status --backup-name <name>" for more details')

    return cluster_backups
