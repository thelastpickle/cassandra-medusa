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


import csv
import logging
import os
import subprocess
import tempfile
import time
import uuid

import medusa.storage


class GSUtil(object):
    def __init__(self, config):
        self._config = config

    @property
    def bucket_name(self):
        return self._config.bucket_name

    def __enter__(self):
        self._gcloud_config = tempfile.TemporaryDirectory()
        self._env = dict(os.environ, CLOUDSDK_CONFIG=self._gcloud_config.name)
        cmd = ['gcloud', 'auth', 'activate-service-account',
               '--key-file={}'.format(os.path.expanduser(self._config.key_file))]

        max_retries = 5
        attempts = 0

        while attempts < max_retries:
            try:
                subprocess.check_call(cmd, env=self._env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                break
            except subprocess.CalledProcessError as e:
                logging.warning('Activating service account failed: {}. Will retry'.format(e))
                attempts += 1
                if attempts == max_retries:
                    logging.error('All attempts to activate service account failed')
                    raise e

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._gcloud_config.cleanup()
        self._env = dict(os.environ)
        return False

    def cp(self, *, srcs, dst, max_retries=5, parallel_process_count=1):

        job_id = str(uuid.uuid4())
        manifest_log = '/tmp/gsutil_{0}.manifest'.format(job_id)
        gsutil_output = '/tmp/gsutil_{0}.output'.format(job_id)

        # We ran into surprising behaviour when trying to control network bandwidth consumption by tweaking
        # process and thread counts.
        # Despite reading all we could:
        #  - Default settings of process and thread counts (https://git.io/JTISP)
        #  - Allowance of parallelism considering OS (https://git.io/JTzpL)
        #  - The actual implication of -m (https://git.io/JTzpY)
        # We ended up seeing that it's only the presence of `-m` that makes any material difference.
        #
        # So if Medusa doesn't want parallelism (parallel_process_count==1), we'll skip setting -m.
        # Otherwise we control process count and let the threads being default (5).
        #
        # '-o', 'GSUtil:parallel_process_count={}'.format(parallel_process_count),
        # '-o', 'GSUtil:parallel_thread_count={}'.format(parallel_thread_count),

        if parallel_process_count == str(1):
            parallel_options = []
        else:
            parallel_options = [
                '-m',
                '-o', '\"GSUtil:parallel_process_count={}\"'.format(parallel_process_count),
            ]

        cmd = ['gsutil',
               *parallel_options,
               'cp', '-c',
               '-L', manifest_log, '-I', str(dst)]

        logging.debug(' '.join(cmd))

        for retry in range(max_retries):
            if retry > 0:
                time.sleep(5)  # TODO: Move this magic number
                logging.debug('Retrying ({}/{})....'.format(
                    retry + 1,
                    max_retries
                ))
            try:
                with open(gsutil_output, 'w') as output:
                    process = subprocess.Popen(cmd, env=self._env,
                                               bufsize=0,
                                               stdin=subprocess.PIPE,
                                               stdout=output,
                                               stderr=subprocess.STDOUT,
                                               universal_newlines=True)
                for src in srcs:
                    process.stdin.write(str(src) + '\n')
                process.stdin.close()
                if process.wait() == 0:
                    with open(manifest_log) as f:
                        manifestobjects = [
                            medusa.storage.ManifestObject(row['Destination'], int(row['Source Size']), row['Md5'])
                            for row in csv.DictReader(f, delimiter=',')
                        ]

                    # remove the temporary files
                    os.remove(manifest_log)
                    os.remove(gsutil_output)
                    return manifestobjects
            except Exception as e:
                logging.debug("Exception encountered: {} / {}"
                              .format(type(e), str(e)))
        raise IOError('gsutil failed. Max attempts ({}) exceeded'.format(max_retries))
