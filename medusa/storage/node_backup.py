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
import logging
import pathlib


class NodeBackup(object):

    def __init__(self,
                 *,
                 storage,
                 fqdn,
                 name,
                 preloaded_blobs=None,
                 manifest_blob=None,
                 schema_blob=None,
                 tokenmap_blob=None,
                 preload_blobs=False,
                 started_timestamp=None,
                 started_blob=None,
                 finished_timestamp=None,
                 finished_blob=None,
                 differential_blob=None,
                 differential_mode=False,
                 server_version_blob=None):

        self._storage = storage
        self._fqdn = fqdn
        self._name = name

        if self._storage._prefix != '.':
            self._node_base_path = self._storage._prefix / fqdn
        else:
            self._node_base_path = fqdn

        self._node_backup_path = self._node_base_path / name
        self._meta_path = self._node_backup_path / 'meta'

        if differential_mode is True or differential_blob is not None:
            # Differential backup, storage tree is different than full backups
            self._data_path = self._node_base_path / 'data'
            self._differential = True
        else:
            self._data_path = self._node_backup_path / 'data'
            self._differential = False

        self._tokenmap_path = self._meta_path / 'tokenmap.json'
        self._schema_path = self._meta_path / 'schema.cql'
        self._manifest_path = self._meta_path / 'manifest.json'
        self._incremental_path = self._meta_path / 'incremental'
        self._differential_path = self._meta_path / 'differential'
        self._restore_verify_query_path = self._meta_path / 'restore_verify_query.json'
        self._server_version_path = self._meta_path / 'server_version.json'

        if preloaded_blobs is None:
            preloaded_blobs = []
            if preload_blobs:
                preloaded_blobs = storage.storage_driver.list_objects('{}/'.format(self._meta_path))

        self._cached_blobs = {pathlib.Path(blob.name): blob for blob in preloaded_blobs}

        self.cached_manifest = None
        self.cached_manifest_blob = manifest_blob
        self.cached_schema_blob = schema_blob
        self.cached_tokenmap_blob = tokenmap_blob
        self.cached_server_version_blob = server_version_blob
        self.started_blob = started_blob
        self.finished_blob = finished_blob
        self._started = started_timestamp
        self._finished = finished_timestamp

    def __repr__(self):
        return 'NodeBackup(name={0.name}, fqdn={0.fqdn}, schema_path={0.schema_path})'.format(self)

    def _blob(self, path):
        blob = self._cached_blobs.get(path)
        if blob is None:
            logging.debug("Blob {} was not found in cache.".format(path))
            blob = self._storage.storage_driver.get_blob(str(path))
            self._cached_blobs[path] = blob
        return blob

    @property
    def name(self):
        return self._name

    @property
    def fqdn(self):
        return self._fqdn

    @property
    def data_path(self):
        return self._data_path

    @property
    def bucket(self):
        return self._storage.bucket

    @property
    def storage(self):
        return self._storage

    @property
    def tokenmap_path(self):
        return self._tokenmap_path

    @property
    def tokenmap(self):
        if self.cached_tokenmap_blob is None:
            self.cached_tokenmap_blob = self._blob(self.tokenmap_path)
        return self._storage.storage_driver.read_blob_as_string(self.cached_tokenmap_blob)

    @tokenmap.setter
    def tokenmap(self, tokenmap):
        self._storage.storage_driver.upload_blob_from_string(self.tokenmap_path, tokenmap)

    @property
    def server_version_path(self):
        return self._server_version_path

    @property
    def server_version(self):
        try:
            if self.cached_server_version_blob is None:
                self.cached_server_version_blob = self._blob(self.server_version_path)
            return self._storage.storage_driver.read_blob_as_string(self.cached_server_version_blob)
        except Exception:
            # old versions of Medusa do not write the server_version.json file, so we return a default thing
            return json.dumps({"server_type": "cassandra", "release_version": "unknown"})

    @server_version.setter
    def server_version(self, version):
        self._storage.storage_driver.upload_blob_from_string(self.server_version_path, version)

    @property
    def server_type(self):
        return json.loads(self.server_version)["server_type"]

    @property
    def release_version(self):
        return json.loads(self.server_version)["release_version"]

    @property
    def is_dse(self):
        return self.server_type == "dse"

    @property
    def schema_path(self):
        return self._schema_path

    @property
    def schema(self):
        return self._storage.storage_driver.get_blob_content_as_string(self.schema_path)

    @schema.setter
    def schema(self, schema):
        self._storage.storage_driver.upload_blob_from_string(self.schema_path, schema)

    # Should be removed after a while. Here for short term backwards compatibility.
    @property
    def incremental_path(self):
        return self._incremental_path

    @property
    def differential_path(self):
        return self._differential_path

    @property
    def is_differential(self):
        return self._differential

    @schema.setter
    def differential(self, differential):
        self._storage.storage_driver.upload_blob_from_string(self.differential_path, differential)

    @property
    def restore_verify_query_path(self):
        return self._restore_verify_query_path

    @property
    def restore_verify_query(self):
        return self._storage.storage_driver.get_blob_content_as_string(self.restore_verify_query_path)

    @restore_verify_query.setter
    def restore_verify_query(self, restore_verify_query):
        self._storage.storage_driver.upload_blob_from_string(self.restore_verify_query_path, restore_verify_query)

    @property
    def started(self):

        # if we got the started timestamp straight from the constructor
        if self._started is not None:
            return self._started

        # otherwise set it from the schema blob
        if self.cached_schema_blob is None:
            self.cached_schema_blob = self._blob(self._schema_path)

        if self.cached_schema_blob is not None:
            dt = self._storage.storage_driver.get_object_datetime(self.cached_schema_blob)
            self._started = int(dt.timestamp())
            return self._started

        # if we still failed to work out the timestamp of schema blob, we are in trouble
        logging.debug("No schema blob for backup {} of fqdn {}".format(self._name, self._fqdn))
        return None

    @property
    def finished(self):
        # if we got the finished timestamp straight from the constructor
        if self._finished is not None:
            return self._finished

        # otherwise set it from the manifest blob
        if self.cached_manifest_blob is None:
            self.cached_manifest_blob = self._blob(self._manifest_path)

        if self.cached_manifest_blob is not None:
            dt = self._storage.storage_driver.get_object_datetime(self.cached_manifest_blob)
            self._finished = int(dt.timestamp())
            return self._finished

        # if we still failed to work out the timestamp of manifest blob, we are in trouble
        logging.debug("No manifest blob for backup {} of fqdn {}".format(self._name, self._fqdn))
        return None

    @property
    def manifest_path(self):
        return self._manifest_path

    @property
    def manifest(self):
        if self.cached_manifest is None:
            self.cached_manifest = self._storage.storage_driver.get_blob_content_as_string(self.manifest_path)
        return self.cached_manifest

    @manifest.setter
    def manifest(self, manifest):
        self.cached_manifest = None
        self._storage.storage_driver.upload_blob_from_string(self.manifest_path, manifest)

    def datapath(self, *, keyspace, columnfamily):
        return self.data_path / keyspace / columnfamily

    @property
    def backup_path(self):
        return self._node_backup_path

    def exists(self):
        return self._blob(self.schema_path) is not None

    def size(self):
        return sum(
            obj['size']
            for section in json.loads(self.manifest)
            for obj in section['objects']
        )

    def num_objects(self):
        return sum(
            len(section['objects'])
            for section in json.loads(self.manifest)
        )
