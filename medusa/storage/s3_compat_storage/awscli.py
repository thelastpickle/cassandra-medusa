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


import logging
import os
import subprocess
import uuid
import sys

from retrying import retry

from libcloud.storage.providers import get_driver, Provider
from medusa import utils

MAX_UP_DOWN_LOAD_RETRIES = 5


class AwsCli(object):
    def __init__(self, storage):
        self._config = storage.config
        self.storage = storage

    @property
    def bucket_name(self):
        return self._config.bucket_name

    def __enter__(self):
        if self._config.key_file:
            self._env = dict(os.environ, AWS_SHARED_CREDENTIALS_FILE=self._config.key_file)
        else:
            self._env = dict(os.environ)

        if self._config.aws_cli_path == 'dynamic':
            self._aws_cli_path = self.find_aws_cli()
        else:
            self._aws_cli_path = self._config.aws_cli_path

        self.endpoint_url = None
        if self._config.host is not None:
            self.endpoint_url = '{}:{}'.format(self._config.host, self._config.port) \
                if self._config.port is not None else self._config.host
            if utils.evaluate_boolean(self._config.secure):
                self.endpoint_url = 'https://{}'.format(self.endpoint_url)
            else:
                self.endpoint_url = 'http://{}'.format(self.endpoint_url)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._env = dict(os.environ)
        return False

    @staticmethod
    def find_aws_cli():
        """
        Construct the AWS command line with parameters and variables
        Also includes a lookup for the AWS binary, in case we are running
        under a venv
        """
        aws_bin = 'aws'
        binary_paths = ['/usr/bin', '/usr/local/bin']
        paths = sys.path + binary_paths
        for path in paths:
            if not path:
                continue
            tpath = '/'.join([path, 'aws'])
            if os.path.exists(tpath) and os.path.isfile(tpath) and os.access(tpath, os.X_OK):
                aws_bin = tpath
                break
        return aws_bin

    def cp_upload(self, *, srcs, bucket_name, dest, max_retries=5):
        job_id = str(uuid.uuid4())
        awscli_output = "/tmp/awscli_{0}.output".format(job_id)
        objects = []
        for src in srcs:
            cmd = self._create_s3_cmd()
            cmd.extend(["s3", "cp", str(src), "s3://{}/{}".format(bucket_name, dest)])
            objects.append(self.upload_file(cmd, dest, awscli_output))

        return objects

    def cp_download(self, *, src, bucket_name, dest, max_retries=5):
        job_id = str(uuid.uuid4())
        awscli_output = "/tmp/awscli_{0}.output".format(job_id)
        objects = []
        cmd = self._create_s3_cmd()
        cmd.extend(["s3", "cp", "s3://{}/{}".format(bucket_name, src), dest])
        self.download_file(cmd, dest, awscli_output)

        return objects

    def _create_s3_cmd(self):
        cmd = [self._aws_cli_path]

        if self.endpoint_url is not None:
            cmd.extend(["--endpoint-url", self.endpoint_url])

        if self._config.region is not None and self._config.region != "default":
            cmd.extend(["--region", self._config.region])
        elif not (self._config.storage_provider in [Provider.S3, "s3_compatible"]) and self._config.region == "default":
            # Legacy libcloud S3 providers that were tied to a specific region such as s3_us_west_oregon
            cmd.extend(["--region", get_driver(self._config.storage_provider).region_name])

        return cmd

    @retry(
        stop_max_attempt_number=MAX_UP_DOWN_LOAD_RETRIES,
        wait_exponential_multiplier=10000, wait_exponential_max=120000)
    def upload_file(self, cmd, dest, awscli_output):
        logging.debug(" ".join(cmd))
        with open(awscli_output, "w") as output:
            process = subprocess.Popen(
                cmd,
                env=self._env,
                bufsize=0,
                stdout=output,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
            )

            if process.wait() == 0:
                obj = self.get_blob(dest)
                os.remove(awscli_output)
                return obj

        raise IOError(
            "awscli cp failed. Max attempts exceeded. Check {} for more informations.".format(
                awscli_output
            )
        )

    @retry(
        stop_max_attempt_number=MAX_UP_DOWN_LOAD_RETRIES,
        wait_exponential_multiplier=10000, wait_exponential_max=120000)
    def download_file(self, cmd, dest, awscli_output):
        logging.debug(" ".join(cmd))
        with open(awscli_output, "w") as output:
            process = subprocess.Popen(
                cmd,
                env=self._env,
                bufsize=0,
                stdout=output,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
            )

            if process.wait() == 0:
                os.remove(awscli_output)
                return

        raise IOError(
            "awscli cp failed. Max attempts exceeded. Check {} for more informations.".format(
                awscli_output
            )
        )

    @retry(
        stop_max_attempt_number=MAX_UP_DOWN_LOAD_RETRIES,
        wait_exponential_multiplier=10000, wait_exponential_max=120000)
    def get_blob(self, blob_name):
        # This needs to be retried as S3 is eventually consistent
        obj = self.storage.get_blob(blob_name)
        if obj is None:
            raise IOError("Failed to find uploaded object {} in S3".format(blob_name))

        return obj
