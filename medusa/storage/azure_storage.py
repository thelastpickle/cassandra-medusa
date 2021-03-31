import base64
import io
import json
import logging
import os
import subprocess
from subprocess import PIPE

from dateutil import parser
from libcloud.storage.drivers.azure_blobs import AzureBlobsStorageDriver

from medusa.storage.abstract_storage import AbstractStorage
import medusa.storage.azure_blobs_storage.concurrent
from medusa.storage.azure_blobs_storage.azcli import AzCli
import medusa


class AzureStorage(AbstractStorage):

    def connect_storage(self):
        with io.open(os.path.expanduser(self.config.key_file), 'r', encoding='utf-8') as json_fi:
            credentials = json.load(json_fi)

        if 'host' in credentials:
            # Hack for Azure connections with a host. Libcloud has a bug in this scenario.
            # Link to bug submitted against libcloud: https://github.com/apache/libcloud/issues/1551
            driver = AzureBlobsStorageDriver(
                key=None,
                secret=credentials['key'],
                host=credentials['host']
            )
            driver.connection.user_id = credentials['storage_account']
        else:
            driver = AzureBlobsStorageDriver(
                key=credentials['storage_account'],
                secret=credentials['key']
            )

        return driver

    def check_dependencies(self):
        az_cli_path = AzCli.find_az_cli()
        try:
            subprocess.check_call([az_cli_path, "help"], stdout=PIPE, stderr=PIPE)
        except Exception:
            raise RuntimeError(
                "Azure cli doesn't seem to be installed on this system and is a "
                + "required dependency for the Azure backend. "
                + "Please check https://docs.microsoft.com/en-us/cli/azure/install-azure-cli for guidelines."
            )

    def get_object_datetime(self, blob):
        logging.debug(
            "Blob {} last modification time is {}".format(
                blob.name, blob.extra["last_modified"]
            )
        )
        return parser.parse(blob.extra["last_modified"])

    def get_cache_path(self, path):
        logging.debug(
            "My cache path is {}".format(path)
        )
        return path

    def upload_blobs(self, srcs, dest):
        return medusa.storage.azure_blobs_storage.concurrent.upload_blobs(
            self, srcs, dest, self.bucket,
            max_workers=self.config.concurrent_transfers,
            multi_part_upload_threshold=int(self.config.multi_part_upload_threshold)
        )

    def download_blobs(self, srcs, dest):
        return medusa.storage.azure_blobs_storage.concurrent.download_blobs(
            self, srcs, dest, self.bucket,
            max_workers=self.config.concurrent_transfers,
            multi_part_upload_threshold=int(self.config.multi_part_upload_threshold)
        )

    @staticmethod
    def blob_matches_manifest(blob, object_in_manifest, enable_md5_checks=False):
        # Azure use hashed timespan in eTag header. It changes everytime
        # when the file is overwrote. "content-md5" is the right hash to
        # validate the file.
        return AzureStorage.compare_with_manifest(
            actual_size=blob.size,
            size_in_manifest=object_in_manifest['size'],
            actual_hash=blob.extra['md5_hash'] if enable_md5_checks else None,
            hash_in_manifest=object_in_manifest['MD5']
        )

    @staticmethod
    def file_matches_cache(src, cached_item, threshold=None, enable_md5_checks=False):
        return AzureStorage.compare_with_manifest(
            actual_size=src.stat().st_size,
            size_in_manifest=cached_item['size'],
            actual_hash=AbstractStorage.generate_md5_hash(src) if enable_md5_checks else None,
            hash_in_manifest=cached_item['MD5'],
        )

    @staticmethod
    def compare_with_manifest(actual_size, size_in_manifest, actual_hash=None, hash_in_manifest=None, threshold=None):
        sizes_match = actual_size == size_in_manifest
        if not actual_hash:
            return sizes_match

        hashes_match = (
            # this case comes from comparing blob hashes to manifest entries (in context of GCS)
            actual_hash == base64.b64decode(hash_in_manifest).hex()
            # this comes from comparing files to a cache
            or hash_in_manifest == base64.b64decode(actual_hash).hex()
            # and perhaps we need the to check for match even without base64 encoding
            or actual_hash == hash_in_manifest
        )

        return sizes_match and hashes_match
