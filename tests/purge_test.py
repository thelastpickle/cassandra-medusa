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
import hashlib
import os
import unittest

from datetime import datetime, timedelta
from libcloud.storage.base import Object
from random import randrange

from medusa.config import MedusaConfig, StorageConfig, _namedtuple_from_dict
from medusa.storage import NodeBackup, Storage
from medusa.purge import backups_to_purge_by_age, backups_to_purge_by_count
from medusa.purge import filter_differential_backups, filter_files_within_gc_grace


class PurgeTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def setUp(self):
        config = configparser.ConfigParser(interpolation=None)
        config['storage'] = {
            'host_file_separator': ',',
            'storage_provider': 'local',
            'base_path': '/tmp',
            'bucket_name': 'purge_test'
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
            kubernetes=None,
        )
        self.storage = Storage(config=self.config.storage)

    def test_purge_backups_by_date(self):
        backups = list()
        # Build a list of 40 daily backups
        for i in range(1, 80, 2):
            file_time = datetime.now() + timedelta(days=(i + 1) - 80)
            backups.append(self.make_backup(self.storage, str(i), file_time, differential=True))

        obsolete_backups = backups_to_purge_by_age(backups, 1)
        assert len(obsolete_backups) == 39

        obsolete_backups = backups_to_purge_by_age(backups, 10)
        assert len(obsolete_backups) == 35

        obsolete_backups = backups_to_purge_by_age(backups, 78)
        assert len(obsolete_backups) == 1

        obsolete_backups = backups_to_purge_by_age(backups, 80)
        assert len(obsolete_backups) == 0

        obsolete_backups = backups_to_purge_by_age(backups, 90)
        assert len(obsolete_backups) == 0

    def test_purge_backups_by_count(self):
        backups = list()

        # Build a list of 40 daily backups
        for i in range(1, 80, 2):
            file_time = datetime.now() + timedelta(days=(i + 1) - 80)
            backups.append(self.make_backup(self.storage, str(i), file_time, differential=True))

        obsolete_backups = backups_to_purge_by_count(backups, 10)
        assert(obsolete_backups[0].name == "1")

        obsolete_backups = backups_to_purge_by_count(backups, 20)
        assert len(obsolete_backups) == 20

        obsolete_backups = backups_to_purge_by_count(backups, 40)
        assert len(obsolete_backups) == 0

    def test_filter_differential_backups(self):
        backups = list()
        backups.append(self.make_backup(self.storage, "one", datetime.now(), differential=True))
        backups.append(self.make_backup(self.storage, "two", datetime.now(), differential=True))
        backups.append(self.make_backup(self.storage, "three", datetime.now(), differential=False))
        backups.append(self.make_backup(self.storage, "four", datetime.now(), differential=True))
        backups.append(self.make_backup(self.storage, "five", datetime.now(), differential=False))
        assert 3 == len(filter_differential_backups(backups))

    def test_filter_files_within_gc_grace(self):
        blobs = list()
        blobs.append(self.make_blob("file1", datetime.timestamp(datetime.now())))
        blobs.append(self.make_blob("file2", datetime.timestamp(datetime.now() + timedelta(hours=-12))))
        blobs.append(self.make_blob("file3", datetime.timestamp(datetime.now() + timedelta(days=-1))))
        blobs.append(self.make_blob("file4", datetime.timestamp(datetime.now() + timedelta(days=-2))))
        assert 2 == len(filter_files_within_gc_grace(self.storage.storage_driver, blobs, 1))

    def make_backup(self, storage, name, backup_date, differential=False):
        if differential is True:
            differential_blob = self.make_blob("localhost/{}/meta/differential".format(name), backup_date.timestamp())
        else:
            differential_blob = None
        manifest_blob = self.make_blob("localhost/{}/meta/manifest.json".format(name), backup_date.timestamp())
        tokenmap_blob = self.make_blob("localhost/{}/meta/tokenmap.json".format(name), backup_date.timestamp())
        schema_blob = self.make_blob("localhost/{}/meta/schema.cql".format(name), backup_date.timestamp())
        manifest_blob = self.make_blob("localhost/{}/meta/manifest.json".format(name), backup_date.timestamp())
        return NodeBackup(storage=storage, fqdn="localhost", name=str(name),
                          differential_blob=differential_blob, manifest_blob=manifest_blob,
                          tokenmap_blob=tokenmap_blob, schema_blob=schema_blob,
                          started_timestamp=backup_date.timestamp(), finished_timestamp=backup_date.timestamp())

    def make_blob(self, blob_name, blob_date):
        extra = {
            'creation_time': blob_date,
            'access_time': blob_date,
            'modify_time': blob_date
        }
        checksum = hashlib.md5()
        checksum.update(os.urandom(4))

        return Object(
            name=blob_name,
            size=randrange(100),
            extra=extra,
            driver=self,
            container=None,
            hash=checksum.hexdigest(),
            meta_data=None
        )


if __name__ == '__main__':
    unittest.main()
