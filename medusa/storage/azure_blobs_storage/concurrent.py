# -*- coding: utf-8 -*-
# Copyright 2019 The Last Pickle
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

import concurrent.futures
import logging
import multiprocessing
import os
import pathlib
import threading

from libcloud.storage.types import ObjectDoesNotExistError
from retrying import retry

import medusa
from medusa.storage.azure_blobs_storage.azcli import AzCli

MAX_UP_DOWN_LOAD_RETRIES = 5


class StorageJob:
    """
    Manages concurrency and storage connection pools for tasks like uploading or downloading files. The libcloud
    drivers are not thread safe, so each thread will a separate connection. If the function executed by StorageJob
    uses any shared state, then it is the responsibility of that function to manage concurrent access to that state.
    """

    def __init__(self, storage, func, max_workers=None):
        self.storage = storage
        self.lock = threading.Lock()
        self.func = func
        self.connection_pool = []
        if max_workers is None:
            self.max_workers = int(multiprocessing.cpu_count())
        else:
            self.max_workers = int(max_workers)

    def execute(self, iterables):
        with concurrent.futures.ThreadPoolExecutor(self.max_workers) as executor:
            return list(executor.map(self.with_storage, iterables))

    def with_storage(self, iterable):
        with self.lock:
            if not self.connection_pool:
                connection = self.storage.connect_storage()
            else:
                connection = self.connection_pool.pop()
        try:
            return self.func(self.storage, connection, iterable)
        finally:
            with self.lock:
                self.connection_pool.append(connection)


def upload_blobs(
    storage, src, dest, bucket, max_workers=None, multi_part_upload_threshold=0
):
    """
    Uploads a list of files from local storage concurrently to the remote storage.

    :param storage: An AbstractStorage instance, needed to create a connection pool
    :param src: A list of files to upload
    :param dest: The location where to upload the files in the target bucket (doesn't contain the filename)
    :param bucket: The remote bucket in which files will be stored
    :param max_workers: The max number of worker threads to use. Defaults to the number of CPUs.
    :return: A list of ManifestObject describing all the uploaded files
    """

    job = StorageJob(
        storage,
        lambda storage, connection, src_file: __upload_file(
            storage, connection, src_file, dest, bucket, multi_part_upload_threshold
        ),
        max_workers,
    )
    return job.execute(list(src))


def __upload_file(storage, connection, src, dest, bucket, multi_part_upload_threshold):
    """
    This function is called by StorageJob. It may be called concurrently by multiple threads.

    :param connection: A storage connection which is created and managed by StorageJob
    :param src: The file to upload
    :param dest: The location where to upload the file
    :param bucket: The remote bucket where the file will be stored
    :return: A ManifestObject describing the uploaded file
    """
    if not isinstance(src, pathlib.Path):
        src = pathlib.Path(src)

    file_size = os.stat(str(src)).st_size
    logging.info("Uploading {} ({})".format(src, human_readable_size(file_size)))
    # check if objects resides in a sub-folder (e.g. secondary index). if it does, use the sub-folder in object path
    obj_name = (
        "{}/{}".format(src.parent.name, src.name)
        if src.parent.name.startswith(".")
        else src.name
    )
    full_object_name = str("{}/{}".format(dest, obj_name))

    if file_size >= multi_part_upload_threshold:
        # Files larger than the configured threshold should be uploaded as multi part
        logging.debug("Uploading {} as multi part".format(full_object_name))
        obj = _upload_multi_part(storage, connection, src, bucket, full_object_name)
    else:
        logging.debug("Uploading {} as single part".format(full_object_name))
        obj = _upload_single_part(storage, connection, src, bucket, full_object_name)

    return medusa.storage.ManifestObject(obj.name, int(obj.size), obj.extra['md5_hash'])


@retry(stop_max_attempt_number=MAX_UP_DOWN_LOAD_RETRIES, wait_fixed=5000)
def _upload_single_part(storage, connection, src, bucket, object_name):
    _ = connection.upload_object(
        str(src), container=bucket, object_name=object_name
    )
    # Returning object from upload doesn't have md5_hash property
    # That's why we need to retrieve it again.
    blob = AzCli(storage).get_blob(object_name)
    return blob


def _upload_multi_part(storage, connection, src, bucket, object_name):
    with AzCli(storage) as azcli:
        objects = azcli.cp_upload(srcs=[src], bucket_name=bucket.name, dest=object_name)
    return objects[0]


def download_blobs(storage, src, dest, bucket, max_workers=None, multi_part_upload_threshold=0):
    """
    Download files concurrently to local storage

    :param storage: An AbstractStorage instance, needed to create a connection pool
    :param src: A list of files to download from the remote storage system
    :param dest: The path to where objects should be downloaded locally
    :param bucket: Storage bucket from which files will be downloaded
    :param max_workers: The max number of threads to use. Defaults to the number of CPUS.
    :return:
    """
    job = StorageJob(
        storage,
        lambda storage, connection, src_file: __download_blob(
            storage, connection, src_file, str(dest), bucket, multi_part_upload_threshold
        ),
        max_workers,
    )
    job.execute(list(src))


@retry(stop_max_attempt_number=MAX_UP_DOWN_LOAD_RETRIES, wait_exponential_multiplier=10000, wait_exponential_max=120000)
def __download_blob(storage, connection, src, dest, bucket, multi_part_upload_threshold):
    """
    This function is called by StorageJob. It may be called concurrently by multiple threads.

    :param connection: A storage connection which is created and managed by StorageJob
    :param src: The file to download
    :param dest: The path to where the file should be downloaded
    :param bucket: Bucket from which the file will be downloaded
    :return:
    """
    try:
        logging.debug("[Storage] Getting object {}".format(src))
        blob = connection.get_object(bucket.name, str(src))

        # we must make sure the blob gets stored under sub-folder (if there is any)
        # the dest variable only points to the table folder, so we need to add the sub-folder
        src_path = pathlib.Path(src)
        blob_dest = (
            "{}/{}".format(dest, src_path.parent.name)
            if src_path.parent.name.startswith(".")
            else dest
        )

        if int(blob.size) >= multi_part_upload_threshold:
            # Files larger than the configured threshold should be uploaded as multi part
            logging.debug("Downloading {} as multi part".format(blob_dest))
            _download_multi_part(storage, connection, src_path, bucket, blob_dest)
        else:
            logging.debug("Downloading {} as single part".format(blob_dest))
            _download_single_part(connection, blob, blob_dest)

    except ObjectDoesNotExistError:
        return None


@retry(stop_max_attempt_number=MAX_UP_DOWN_LOAD_RETRIES, wait_exponential_multiplier=10000, wait_exponential_max=120000)
def _download_single_part(connection, blob, blob_dest):
    index = blob.name.rfind("/")
    if index > 0:
        file_name = blob.name[blob.name.rfind("/") + 1:]
    else:
        file_name = blob.name
    blob.download("{}/{}".format(blob_dest, file_name), overwrite_existing=True)


def _download_multi_part(storage, connection, src, bucket, blob_dest):
    with AzCli(storage) as azcli:
        azcli.cp_download(src=src, bucket_name=bucket.name, dest=blob_dest)


def human_readable_size(size, decimal_places=3):
    for unit in ["B", "KiB", "MiB", "GiB", "TiB"]:
        if size < 1024.0:
            break
        size /= 1024.0
    return '{:.{}f}{}'.format(size, decimal_places, unit)
