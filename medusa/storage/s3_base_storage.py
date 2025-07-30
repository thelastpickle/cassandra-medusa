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
import asyncio
import base64
import pathlib

import boto3
import botocore.session
import concurrent.futures
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
AWS_KMS_ENCRYPTION = 'aws:kms'

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
        if self.access_key_id and len(self.access_key_id) > 0:
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

        self.kms_id = None
        if config.kms_id is not None:
            logging.debug("Using KMS key {}".format(config.kms_id))
            self.kms_id = config.kms_id

        self.sse_c_key = None
        if config.sse_c_key is not None:
            logging.debug("Using SSE-C key *****")
            self.sse_c_key = base64.b64decode(config.sse_c_key)

        self.credentials = self._consolidate_credentials(config)
        logging.info('Using credentials {}'.format(self.credentials))

        self.bucket_name: str = config.bucket_name

        self.storage_provider = config.storage_provider

        self.connection_extra_args = self._make_connection_arguments(config)
        self.transfer_config = self._make_transfer_config(config)

        self.executor = concurrent.futures.ThreadPoolExecutor(int(config.concurrent_transfers))

        self.read_timeout = int(config.read_timeout) if 'read_timeout' in dir(config) and config.read_timeout else None

        super().__init__(config)

    def connect(self):
        logging.info(
            'Connecting to {} with args {}'.format(
                self.storage_provider, self.connection_extra_args
            )
        )

        # make the pool size double of what we will have going on
        # helps urllib (used by boto) to reuse connections better and not WARN us about evicting connections
        max_pool_size = int(self.config.concurrent_transfers) * 2

        boto_config = Config(
            region_name=self.credentials.region,
            tcp_keepalive=True,
            max_pool_connections=max_pool_size,
            read_timeout=self.read_timeout,
            s3={'addressing_style': self.config.s3_addressing_style},
        )
        if self.credentials.access_key_id is not None:
            self.s3_client = boto3.client(
                's3',
                config=boto_config,
                aws_access_key_id=self.credentials.access_key_id,
                aws_secret_access_key=self.credentials.secret_access_key,
                **self.connection_extra_args
            )
        else:
            self.s3_client = boto3.client(
                's3',
                config=boto_config,
                **self.connection_extra_args
            )

    def disconnect(self):
        logging.debug('Disconnecting from S3...')
        try:
            self.s3_client.close()
            self.executor.shutdown()
        except Exception as e:
            logging.error('Error disconnecting from S3: {}'.format(e))

    def _make_connection_arguments(self, config) -> t.Dict[str, str]:

        secure = config.secure or 'True'
        ssl_verify = config.ssl_verify or 'False'   # False until we work out how to specify custom certs
        host = config.host
        port = config.port

        if self.storage_provider != 's3_compatible':
            # when we're dealing with regular AWS, we don't need anything extra
            return {}
        else:
            # we're dealing with a custom s3 compatible storage, so we need to craft the URL
            protocol = 'https' if secure.lower() == 'true' else 'http'
            port = '' if port is None else str(port)
            s3_url = '{}://{}:{}'.format(protocol, host, port)
            return {
                'endpoint_url': s3_url,
                'verify': ssl_verify.lower() == 'true'
            }

    def _make_transfer_config(self, config):

        transfer_max_bandwidth = config.transfer_max_bandwidth or None
        multipart_chunksize = config.multipart_chunksize or None

        # we hard-code this one because the parallelism is for now applied to chunking the files
        transfer_config = {
            'max_concurrency': 4
        }

        if transfer_max_bandwidth is not None:
            transfer_config['max_bandwidth'] = AbstractStorage._human_size_to_bytes(transfer_max_bandwidth)

        if multipart_chunksize is not None:
            transfer_config['multipart_chunksize'] = AbstractStorage._human_size_to_bytes(multipart_chunksize)
        return TransferConfig(**transfer_config)

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
        else:
            return CensoredCredentials(
                access_key_id=None,
                secret_access_key=None,
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

        response = self.s3_client.get_paginator('list_objects_v2').paginate(
            Bucket=self.bucket_name,
            Prefix=str(prefix),
            PaginationConfig={'PageSize': 1000}
        ).build_full_result()

        for o in response.get('Contents', []):
            obj_hash = o['ETag'].replace('"', '')
            blobs.append(AbstractBlob(o['Key'], o['Size'], obj_hash, o['LastModified'], o['StorageClass']))

        return blobs

    @retry(stop_max_attempt_number=MAX_UP_DOWN_LOAD_RETRIES, wait_fixed=5000)
    async def _upload_object(self, data: io.BytesIO, object_key: str, headers: t.Dict[str, str]) -> AbstractBlob:

        extra_args = {}
        if self.kms_id is not None:
            extra_args['ServerSideEncryption'] = AWS_KMS_ENCRYPTION
            extra_args['SSEKMSKeyId'] = self.kms_id

        if self.sse_c_key is not None:
            extra_args['SSECustomerAlgorithm'] = 'AES256'
            extra_args['SSECustomerKey'] = self.sse_c_key

        storage_class = self.get_storage_class()
        if storage_class is not None:
            extra_args['StorageClass'] = storage_class

        logging.debug(
            '[S3 Storage] Uploading object from stream -> s3://{}/{}'.format(
                self.bucket_name, object_key
            )
        )

        try:
            # not passing in the transfer config because that is meant to cap a throughput
            # here we are uploading a small-ish file so no need to cap
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=object_key,
                Body=data,
                **extra_args,
            )
        except Exception as e:
            logging.error(e)
            raise e
        blob = await self._stat_blob(object_key)
        return blob

    @retry(stop_max_attempt_number=MAX_UP_DOWN_LOAD_RETRIES, wait_fixed=5000)
    async def _download_blob(self, src: str, dest: str):
        # boto has a connection pool, but it does not support the asyncio API
        # so we make things ugly and submit the whole download as a task to an executor
        # which allows us to download several files in parallel
        loop = asyncio.get_event_loop()
        future = loop.run_in_executor(self.executor, self.__download_blob, src, dest)
        await future

    def __download_blob(self, src: str, dest: str):
        blob = self.__stat_blob(src)
        object_key = blob.name

        # we must make sure the blob gets stored under sub-folder (if there is any)
        # the dest variable only points to the table folder, so we need to add the sub-folder
        src_path = Path(src)
        file_path = AbstractStorage.path_maybe_with_parent(dest, src_path)

        # print also object size
        logging.debug(
            '[S3 Storage] Downloading s3://{}/{} -> {}'.format(
                self.bucket_name, object_key, file_path
            )
        )

        extra_args = {}
        if self.sse_c_key is not None:
            extra_args['SSECustomerAlgorithm'] = 'AES256'
            extra_args['SSECustomerKey'] = self.sse_c_key

        try:
            Path(file_path).parent.mkdir(parents=True, exist_ok=True)
            self.s3_client.download_file(
                Bucket=self.bucket_name,
                Key=object_key,
                Filename=file_path,
                ExtraArgs=extra_args,
                Config=self.transfer_config,
            )
        except Exception as e:
            logging.error('Error downloading file from s3://{}/{}: {}'.format(self.bucket_name, object_key, e))
            raise ObjectDoesNotExistError('Object {} does not exist'.format(object_key))

    async def _stat_blob(self, object_key: str) -> AbstractBlob:
        try:
            extra_args = {}
            if self.sse_c_key is not None:
                extra_args['SSECustomerAlgorithm'] = 'AES256'
                extra_args['SSECustomerKey'] = self.sse_c_key

            resp = self.s3_client.head_object(Bucket=self.bucket_name, Key=object_key, **extra_args)
            item_hash = resp['ETag'].replace('"', '')
            return AbstractBlob(object_key, int(resp['ContentLength']), item_hash, resp['LastModified'], None)
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey' or e.response['Error']['Code'] == '404':
                logging.debug("[S3 Storage] Object {} not found".format(object_key))
                raise ObjectDoesNotExistError('Object {} does not exist'.format(object_key))
            else:
                # Handle other exceptions if needed
                logging.error("An error occurred: %s", e)
                logging.error('Error getting object from s3://{}/{}'.format(self.bucket_name, object_key))

    def __stat_blob(self, key):
        extra_args = {}
        if self.sse_c_key is not None:
            extra_args['SSECustomerAlgorithm'] = 'AES256'
            extra_args['SSECustomerKey'] = self.sse_c_key

        resp = self.s3_client.head_object(Bucket=self.bucket_name, Key=key, **extra_args)
        item_hash = resp['ETag'].replace('"', '')
        return AbstractBlob(key, int(resp['ContentLength']), item_hash, resp['LastModified'], None)

    @retry(stop_max_attempt_number=MAX_UP_DOWN_LOAD_RETRIES, wait_fixed=5000)
    async def _upload_blob(self, src: str, dest: str) -> ManifestObject:
        src_path = Path(src)

        # check if objects resides in a sub-folder (e.g. secondary index). if it does, use the sub-folder in object path
        object_key = AbstractStorage.path_maybe_with_parent(dest, src_path)

        extra_args = {}
        if self.kms_id is not None:
            extra_args['ServerSideEncryption'] = AWS_KMS_ENCRYPTION
            extra_args['SSEKMSKeyId'] = self.kms_id

        if self.sse_c_key is not None:
            extra_args['SSECustomerAlgorithm'] = 'AES256'
            extra_args['SSECustomerKey'] = self.sse_c_key

        storage_class = self.get_storage_class()
        if storage_class is not None:
            extra_args['StorageClass'] = storage_class

        file_size = os.stat(src).st_size
        logging.debug(
            '[S3 Storage] Uploading {} ({}) -> {}'.format(
                src, self.human_readable_size(file_size), object_key
            )
        )

        upload_conf = {
            'Filename': src,
            'Bucket': self.bucket_name,
            'Key': object_key,
            'Config': self.transfer_config,
            'ExtraArgs': extra_args,
        }
        # we are going to combine asyncio with boto's threading
        # we do this by submitting the upload into an executor
        loop = asyncio.get_event_loop()
        future = loop.run_in_executor(self.executor, self.__upload_file, upload_conf)
        # and then ask asyncio to yield until it completes
        mo = await future
        return mo

    def __upload_file(self, upload_conf):
        self.s3_client.upload_file(**upload_conf)

        extra_args = {}
        if self.sse_c_key is not None:
            extra_args['SSECustomerAlgorithm'] = 'AES256'
            extra_args['SSECustomerKey'] = self.sse_c_key

        resp = self.s3_client.head_object(Bucket=upload_conf['Bucket'], Key=upload_conf['Key'], **extra_args)
        blob_name = upload_conf['Key']
        blob_size = int(resp['ContentLength'])
        blob_hash = resp['ETag'].replace('"', '')
        return ManifestObject(blob_name, blob_size, blob_hash)

    async def _get_object(self, object_key: t.Union[Path, str]) -> AbstractBlob:
        blob = await self._stat_blob(str(object_key))
        return blob

    async def _read_blob_as_bytes(self, blob: AbstractBlob) -> bytes:
        extra_args = {}
        if self.sse_c_key is not None:
            extra_args['SSECustomerAlgorithm'] = 'AES256'
            extra_args['SSECustomerKey'] = self.sse_c_key

        return self.s3_client.get_object(Bucket=self.bucket_name, Key=blob.name, **extra_args)['Body'].read()

    @retry(stop_max_attempt_number=MAX_UP_DOWN_LOAD_RETRIES, wait_fixed=5000)
    async def _delete_object(self, obj: AbstractBlob):
        self.s3_client.delete_object(
            Bucket=self.bucket_name,
            Key=obj.name
        )

    async def _get_blob_metadata(self, blob_key: str) -> AbstractBlobMetadata:
        extra_args = {}
        if self.sse_c_key is not None:
            extra_args['SSECustomerAlgorithm'] = 'AES256'
            extra_args['SSECustomerKey'] = self.sse_c_key

        resp = self.s3_client.head_object(Bucket=self.bucket_name, Key=blob_key, **extra_args)

        # the headers come as some non-default dict, so we need to re-package them
        blob_metadata = resp.get('ResponseMetadata', {}).get('HTTPHeaders', {})

        if len(blob_metadata.keys()) == 0:
            raise ValueError('No metadata found for blob {}'.format(blob_key))

        sse_algo = blob_metadata.get('x-amz-server-side-encryption', None)
        sse_customer_key_md5 = blob_metadata.get('x-amz-server-side-encryption-customer-key-md5', None)
        if sse_algo == 'AES256' and sse_customer_key_md5 is None:
            sse_enabled, sse_key_id = False, None
        elif sse_customer_key_md5 is not None:
            sse_enabled = True
            sse_key_id = None
        elif sse_algo == AWS_KMS_ENCRYPTION:
            sse_enabled = True
            # the metadata returns the entire ARN, so we just return the last part ~ the actual ID
            sse_key_id = blob_metadata['x-amz-server-side-encryption-aws-kms-key-id'].split('/')[-1]
        else:
            logging.warning('No SSE info found in blob {} metadata'.format(blob_key))
            sse_enabled, sse_key_id = False, None

        return AbstractBlobMetadata(blob_key, sse_enabled, sse_key_id, sse_customer_key_md5)

    @staticmethod
    def blob_matches_manifest(blob: AbstractBlob, object_in_manifest: dict, enable_md5_checks=False):
        return S3BaseStorage.compare_with_manifest(
            actual_size=blob.size,
            size_in_manifest=object_in_manifest['size'],
            actual_hash=str(blob.hash) if enable_md5_checks else None,
            hash_in_manifest=object_in_manifest['MD5']
        )

    @staticmethod
    def file_matches_storage(src: pathlib.Path, cached_item: ManifestObject, threshold=None, enable_md5_checks=False):

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
            size_in_manifest=cached_item.size,
            actual_hash=md5_hash,
            hash_in_manifest=cached_item.MD5,
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
            actual_matches_manifest = actual_hash == base64.b64decode(hash_in_manifest).hex()
            manifest_matches_actual = hash_in_manifest == base64.b64decode(actual_hash).hex()
            hashes_match = actual_matches_manifest or manifest_matches_actual or actual_hash == hash_in_manifest

        return sizes_match and hashes_match
