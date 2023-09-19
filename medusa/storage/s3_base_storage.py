# -*- coding: utf-8 -*-
# Copyright 2019 Spotify AB
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

import base64
import boto3
import botocore.session
import logging
import io
import os
import typing as t

from boto3.s3.transfer import TransferConfig
from botocore.config import Config
from botocore.exceptions import ClientError
from pathlib import Path
from retrying import retry

from medusa.storage.abstract_storage import (
    AbstractStorage, AbstractBlob, AbstractBlobMetadata, ManifestObject, ObjectDoesNotExistError
)


MAX_UP_DOWN_LOAD_RETRIES = 5

"""
    S3BaseStorage supports all the S3 compatible storages. Certain providers might override this method
    to implement their own specialities (such as environment variables when running in certain clouds)
"""


class CensoredCredentials:

    access_key_id = None
    secret_access_key = None
    region = None

    def __init__(self, access_key_id, secret_access_key, region):
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key
        self.region = region

    def __repr__(self):
        if len(self.access_key_id) > 0:
            key = f"{self.access_key_id[0]}..{self.access_key_id[-1]}"
        else:
            key = "None"
        secret = "*****"
        return f"CensoredCredentials(access_key_id={key}, secret_access_key={secret}, region={self.region})"


LIBCLOUD_REGION_NAME_MAP = {
    'S3_US_EAST': 'us-east-1',
    'S3_US_EAST1': 'us-east-1',
    'S3_US_EAST2': 'us-east-2',
    'S3_US_WEST': 'us-west-1',
    'S3_US_WEST2': 'us-west-2',
    'S3_US_WEST_OREGON': 'us-west-2',
    'S3_US_GOV_EAST': 'us-gov-east-1',
    'S3_US_GOV_WEST': 'us-gov-west-1',
    'S3_EU_WEST': 'eu-west-1',
    'S3_EU_WEST2': 'eu-west-2',
    'S3_EU_CENTRAL': 'eu-central-1',
    'S3_EU_NORTH1': 'eu-north-1',
    'S3_AP_SOUTH': 'ap-south-1',
    'S3_AP_SOUTHEAST': 'ap-southeast-1',
    'S3_AP_SOUTHEAST2': 'ap-southeast-2',
    'S3_AP_NORTHEAST': 'ap-northeast-1',
    'S3_AP_NORTHEAST1': 'ap-northeast-1',
    'S3_AP_NORTHEAST2': 'ap-northeast-2',
    'S3_SA_EAST': 'sa-east-1',
    'S3_SA_EAST2': 'sa-east-2',
    'S3_CA_CENTRAL': 'ca-central-1',
    'S3_CN_NORTH': 'cn-north-1',
    'S3_CN_NORTHWEST': 'cn-northwest-1',
    'S3_AF_SOUTH': 'af-south-1',
    'S3_ME_SOUTH': 'me-south-1',
}

logging.getLogger('botocore').setLevel(logging.WARNING)
logging.getLogger('botocore.hooks').setLevel(logging.WARNING)
logging.getLogger('urllib3.connectionpool').setLevel(logging.WARNING)
logging.getLogger('s3transfer').setLevel(logging.WARNING)


class S3BaseStorage(AbstractStorage):

    def __init__(self, config):

        if config.kms_id:
            logging.debug("Using KMS key {}".format(config.kms_id))

        self.credentials = self._consolidate_credentials(config)

        logging.info('Using credentials {}'.format(self.credentials))

        self.bucket_name: str = config.bucket_name
        self.config = config

        super().__init__(config)

    def connect(self):

        if self.config.storage_provider != 's3_compatible':
            # assuming we're dealing with regular aws
            s3_url = "https://{}.s3.amazonaws.com".format(self.bucket_name)
        else:
            # we're dealing with a custom s3 compatible storage, so we need to craft the URL
            protocol = 'https' if self.config.secure.lower() == 'true' else 'http'
            port = '' if self.config.port is None else str(self.config.port)
            s3_url = '{}://{}:{}'.format(protocol, self.config.host, port)

        logging.info('Using S3 URL {}'.format(s3_url))

        logging.debug('Connecting to S3')
        extra_args = {}
        if self.config.storage_provider == 's3_compatible':
            extra_args['endpoint_url'] = s3_url
            extra_args['verify'] = False

        boto_config = Config(
            region_name=self.credentials.region,
            signature_version='v4',
            tcp_keepalive=True
        )

        self.trasnfer_config = TransferConfig(
            # we hard-code this one because the parallelism is for now applied to chunking the files
            max_concurrency=4,
            max_bandwidth=AbstractStorage._human_size_to_bytes(self.config.transfer_max_bandwidth),
        )

        self.s3_client = boto3.client(
            's3',
            config=boto_config,
            aws_access_key_id=self.credentials.access_key_id,
            aws_secret_access_key=self.credentials.secret_access_key,
            **extra_args
        )

    def disconnect(self):
        logging.debug('Disconnecting from S3...')
        try:
            self.s3_client.close()
        except Exception as e:
            logging.error('Error disconnecting from S3: {}'.format(e))

    @staticmethod
    def _consolidate_credentials(config) -> CensoredCredentials:

        session = botocore.session.Session()

        if config.api_profile:
            logging.debug("Using AWS profile {}".format(
                config.api_profile,
            ))
            session.set_config_variable('profile', config.api_profile)

        if config.region and config.region != "default":
            session.set_config_variable('region', config.region)
        elif config.storage_provider not in ['s3', 's3_compatible'] and config.region == "default":
            session.set_config_variable('region', S3BaseStorage._region_from_provider_name(config.storage_provider))
        else:
            session.set_config_variable('region', "us-east-1")

        if config.key_file:
            logging.debug("Setting AWS credentials file to {}".format(
                config.key_file,
            ))
            session.set_config_variable('credentials_file', config.key_file)

        boto_credentials = session.get_credentials()
        return CensoredCredentials(
            access_key_id=boto_credentials.access_key,
            secret_access_key=boto_credentials.secret_key,
            region=session.get_config_variable('region'),
        )

    @staticmethod
    def _region_from_provider_name(provider_name: str) -> str:
        if provider_name.upper() in LIBCLOUD_REGION_NAME_MAP.keys():
            return LIBCLOUD_REGION_NAME_MAP[provider_name.upper()]
        else:
            raise ValueError("Unknown provider name {}".format(provider_name))

    async def _list_blobs(self, prefix=None) -> t.List[AbstractBlob]:
        blobs = []
        for o in self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=str(prefix)).get('Contents', []):
            obj_hash = o['ETag'].replace('"', '')
            blobs.append(AbstractBlob(o['Key'], o['Size'], obj_hash, o['LastModified']))
        return blobs

    @retry(stop_max_attempt_number=MAX_UP_DOWN_LOAD_RETRIES, wait_fixed=5000)
    async def _upload_object(self, data: io.BytesIO, object_key: str, headers: t.Dict[str, str]) -> AbstractBlob:

        kms_args = {}
        if self.config.kms_id is not None:
            kms_args['ServerSideEncryption'] = 'aws:kms'
            kms_args['SSEKMSKeyId'] = self.config.kms_id

        logging.debug(
            '[S3 Storage] Uploading object from stream -> s3://{}/{}'.format(
                self.config.bucket_name, object_key
            )
        )

        try:
            # not passing in the transfer config because that is meant to cap a throughput
            # here we are uploading a small-ish file so no need to cap
            self.s3_client.put_object(
                Bucket=self.config.bucket_name,
                Key=object_key,
                Body=data,
                **kms_args,
            )
        except Exception as e:
            logging.error(e)
            raise e
        blob = await self._stat_blob(object_key)
        return blob

    @retry(stop_max_attempt_number=MAX_UP_DOWN_LOAD_RETRIES, wait_fixed=5000)
    async def _download_blob(self, src: str, dest: str):
        blob = await self._stat_blob(src)
        object_key = blob.name

        # we must make sure the blob gets stored under sub-folder (if there is any)
        # the dest variable only points to the table folder, so we need to add the sub-folder
        src_path = Path(src)
        file_path = (
            "{}/{}/{}".format(dest, src_path.parent.name, src_path.name)
            if src_path.parent.name.startswith(".")
            else "{}/{}".format(dest, src_path.name)
        )

        # print also object size
        logging.debug(
            '[S3 Storage] Downloading {} -> {}/{}'.format(
                object_key, self.config.bucket_name, object_key
            )
        )

        try:
            self.s3_client.download_file(
                Bucket=self.config.bucket_name,
                Key=object_key,
                Filename=file_path,
                Config=self.trasnfer_config,
            )
        except Exception as e:
            logging.error('Error downloading file from s3://{}/{}: {}'.format(self.config.bucket_name, object_key, e))
            raise ObjectDoesNotExistError('Object {} does not exist'.format(object_key))

    async def _stat_blob(self, object_key: str) -> AbstractBlob:
        try:
            resp = self.s3_client.head_object(Bucket=self.config.bucket_name, Key=object_key)
            item_hash = resp['ETag'].replace('"', '')
            return AbstractBlob(object_key, int(resp['ContentLength']), item_hash, resp['LastModified'])
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey' or e.response['Error']['Code'] == '404':
                logging.debug("[S3 Storage] Object {} not found".format(object_key))
                raise ObjectDoesNotExistError('Object {} does not exist'.format(object_key))
            else:
                # Handle other exceptions if needed
                logging.error("An error occurred:", e)
                logging.error('Error getting object from s3://{}/{}'.format(self.config.bucket_name, object_key))

    @retry(stop_max_attempt_number=MAX_UP_DOWN_LOAD_RETRIES, wait_fixed=5000)
    async def _upload_blob(self, src: str, dest: str) -> ManifestObject:
        src_chunks = src.split('/')
        parent_name, file_name = src_chunks[-2], src_chunks[-1]

        # check if objects resides in a sub-folder (e.g. secondary index). if it does, use the sub-folder in object path
        object_key = (
            "{}/{}/{}".format(dest, parent_name, file_name)
            if parent_name.startswith(".")
            else "{}/{}".format(dest, file_name)
        )

        kms_args = {}
        if self.config.kms_id is not None:
            kms_args['ServerSideEncryption'] = 'aws:kms'
            kms_args['SSEKMSKeyId'] = self.config.kms_id

        file_size = os.stat(src).st_size
        logging.debug(
            '[S3 Storage] Uploading {} ({}) -> {}'.format(
                src, self._human_readable_size(file_size), object_key
            )
        )

        self.s3_client.upload_file(
            Filename=src,
            Bucket=self.bucket_name,
            Key=object_key,
            Config=self.trasnfer_config,
            ExtraArgs=kms_args,
        )

        blob = await self._stat_blob(object_key)
        mo = ManifestObject(blob.name, blob.size, blob.hash)
        return mo

    async def _get_object(self, object_key: t.Union[Path, str]) -> AbstractBlob:
        blob = await self._stat_blob(str(object_key))
        return blob

    async def _read_blob_as_bytes(self, blob: AbstractBlob) -> bytes:
        return self.s3_client.get_object(Bucket=self.bucket_name, Key=blob.name)['Body'].read()

    async def _delete_object(self, obj: AbstractBlob):
        self.s3_client.delete_object(
            Bucket=self.config.bucket_name,
            Key=obj.name
        )

    async def _get_blob_metadata(self, blob_key: str) -> AbstractBlobMetadata:
        resp = self.s3_client.head_object(Bucket=self.config.bucket_name, Key=blob_key)

        # the headers come as some non-default dict, so we need to re-package them
        blob_metadata = resp.get('ResponseMetadata', {}).get('HTTPHeaders', {})

        if len(blob_metadata.keys()) == 0:
            raise ValueError('No metadata found for blob {}'.format(blob_key))

        sse_algo = blob_metadata.get('x-amz-server-side-encryption', None)
        if sse_algo == 'AES256':
            sse_enabled, sse_key_id = False, None
        elif sse_algo == 'aws:kms':
            sse_enabled = True
            # the metadata returns the entire ARN, so we just return the last part ~ the actual ID
            sse_key_id = blob_metadata['x-amz-server-side-encryption-aws-kms-key-id'].split('/')[-1]
        else:
            logging.warning('No SSE info found in blob {} metadata'.format(blob_key))
            sse_enabled, sse_key_id = False, None

        return AbstractBlobMetadata(blob_key, sse_enabled, sse_key_id)

    @staticmethod
    def blob_matches_manifest(blob: AbstractBlob, object_in_manifest: dict, enable_md5_checks=False):
        return S3BaseStorage.compare_with_manifest(
            actual_size=blob.size,
            size_in_manifest=object_in_manifest['size'],
            actual_hash=str(blob.hash) if enable_md5_checks else None,
            hash_in_manifest=object_in_manifest['MD5']
        )

    @staticmethod
    def file_matches_cache(src, cached_item, threshold=None, enable_md5_checks=False):

        threshold = int(threshold) if threshold else -1

        # single or multi part md5 hash. Used by Azure and S3 uploads.
        if not enable_md5_checks:
            md5_hash = None
        elif src.stat().st_size >= threshold > 0:
            md5_hash = AbstractStorage.md5_multipart(src)
        else:
            md5_hash = AbstractStorage.generate_md5_hash(src)

        return S3BaseStorage.compare_with_manifest(
            actual_size=src.stat().st_size,
            size_in_manifest=cached_item['size'],
            actual_hash=md5_hash,
            hash_in_manifest=cached_item['MD5'],
            threshold=threshold
        )

    @staticmethod
    def compare_with_manifest(actual_size, size_in_manifest, actual_hash=None, hash_in_manifest=None, threshold=None):
        sizes_match = actual_size == size_in_manifest
        if not actual_hash:
            return sizes_match

        # md5 hash comparison
        if not threshold:
            threshold = -1
        else:
            threshold = int(threshold)

        if actual_size >= threshold > 0 or "-" in hash_in_manifest:
            multipart = True
        else:
            multipart = False

        if multipart:
            hashes_match = (
                actual_hash == hash_in_manifest
            )
        else:
            hashes_match = (
                actual_hash == base64.b64decode(hash_in_manifest).hex()
                or hash_in_manifest == base64.b64decode(actual_hash).hex()
                or actual_hash == hash_in_manifest
            )

        return sizes_match and hashes_match
