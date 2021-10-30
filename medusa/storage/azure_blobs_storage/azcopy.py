import io
import json
import logging
import os
import subprocess
import sys
import uuid
from datetime import datetime, timedelta

from retrying import retry

from medusa.storage.abstract_storage import AbstractStorage


class AzCopy(object):
    def __init__(self, storage):
        self._config = storage.config
        self.storage = storage

    def __enter__(self):
        with io.open(os.path.expanduser(self._config.key_file), 'r', encoding='utf-8') as json_fi:
            credentials = json.load(json_fi)

        if 'host' in credentials:
            self._env = dict(
                os.environ,
                AZURE_STORAGE_HOST=credentials['host']
            )
        else:
            self._env = dict(
                os.environ,
                AZURE_STORAGE_HOST=credentials['storage_account'] + '.blob.core.windows.net'
            )

        self._azcopy_path = self.get_azcopy_if_exists()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._env = dict(os.environ)
        return False

    @staticmethod
    def get_azcopy_if_exists():
        """
        Construct the AZ command line with parameters and variables
        Also includes a lookup for the AZ binary, in case we are running
        under a venv
        """
        azcopy_bin = 'azcopy'
        if 'command not found' not in subprocess.check_output([azcopy_bin]):
            return azcopy_bin

        binary_paths = ['/usr/bin', '/usr/local/bin']
        paths = sys.path + binary_paths
        for path in paths:
            if not path:
                continue
            tpath = '/'.join([path, 'azcopy'])
            if os.path.exists(tpath) and os.path.isfile(tpath) and os.access(tpath, os.X_OK):
                return tpath

        return False

    def download_blobs(self, srcs, dest, bucket_name, sas):
        src_list_param = ';'.join(srcs)

        host = self._env.get('AZURE_STORAGE_HOST')
        az_copy_src_str = "https://{}/{}?{}".format(host, bucket_name, sas)

        cmd = [self._azcopy_path, 'copy', az_copy_src_str, dest, '--include-path', src_list_param]

        job_id = str(uuid.uuid4())
        azcopy_output = "/tmp/azcopy_{0}.output".format(job_id)
        self.run_azcopy_cmd(cmd, azcopy_output, "copy")
        return

    @retry(stop_max_attempt_number=5, wait_fixed=5000)
    def run_azcopy_cmd(self, cmd, azcopy_output, func):
        logging.debug(" ".join(cmd))
        with open(azcopy_output, "w") as output:
            process = subprocess.Popen(
                cmd,
                bufsize=0,
                stdout=output,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
            )

            if process.wait() == 0:
                os.remove(azcopy_output)
                return

        raise IOError(
            "azcopy {} failed. Max attempts exceeded. Check {} for more informations.".format(
                func, azcopy_output
            )
        )


