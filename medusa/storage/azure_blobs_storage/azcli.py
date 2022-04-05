import io
import json
import logging
import os
import subprocess
import sys
import uuid

from retrying import retry

from medusa.storage.abstract_storage import AbstractStorage


class AzCli(object):

    def __init__(self, storage):
        self._config = storage.config
        self.storage = storage

    @property
    def bucket_name(self):
        return self._config.bucket_name

    def __enter__(self):
        with io.open(os.path.expanduser(self._config.key_file), 'r', encoding='utf-8') as json_fi:
            credentials = json.load(json_fi)

        if 'connection_string' in credentials:
            self._env = dict(
                os.environ,
                AZURE_STORAGE_CONNECTION_STRING=credentials['connection_string']
            )
        else:
            self._env = dict(
                os.environ,
                AZURE_STORAGE_ACCOUNT=credentials['storage_account'],
                AZURE_STORAGE_KEY=credentials['key']
            )
        self._az_cli_cmd = self.cmd()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._env = dict(os.environ)
        return False

    @staticmethod
    def cmd():
        return [sys.executable, '-m', 'azure.cli']

    def cp_upload(self, *, srcs, bucket_name, dest, max_retries=5):
        job_id = str(uuid.uuid4())
        azcli_output = "/tmp/azcli_{0}.output".format(job_id)
        objects = []
        # Az cli expects the client to provide the MD5 hash of the upload
        for src in srcs:
            cmd = self._az_cli_cmd + ["storage", "blob", "upload", "-f", str(src), "-c", bucket_name, "-n", dest,
                                      "--content-md5", AbstractStorage.generate_md5_hash(src)]
            objects.append(self.upload_file(cmd, dest, azcli_output))

        return objects

    def cp_download(self, *, src, bucket_name, dest, max_retries=5):
        job_id = str(uuid.uuid4())
        azcli_output = "/tmp/azcli_{0}.output".format(job_id)
        objects = []
        dest_path = os.path.join(str(dest), str(src).split("/")[-1])
        cmd = self._az_cli_cmd + ["storage", "blob", "download", "-f", dest_path, "-c", bucket_name, "-n", str(src)]
        self.download_file(cmd, dest, azcli_output)
        return objects

    @retry(stop_max_attempt_number=5, wait_fixed=5000)
    def upload_file(self, cmd, dest, azcli_output):
        logging.debug(" ".join(cmd))
        with open(azcli_output, "w") as output:
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
                os.remove(azcli_output)
                return obj

        raise IOError(
            "az cli cp failed. Max attempts exceeded. Check {} for more informations.".format(
                azcli_output
            )
        )

    @retry(stop_max_attempt_number=5, wait_fixed=5000)
    def download_file(self, cmd, dest, azcli_output):
        logging.debug(" ".join(cmd))
        with open(azcli_output, "w") as output:
            process = subprocess.Popen(
                cmd,
                env=self._env,
                bufsize=0,
                stdout=output,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
            )

            if process.wait() == 0:
                os.remove(azcli_output)
                return

        raise IOError(
            "az cli cp failed. Max attempts exceeded. Check {} for more informations.".format(
                azcli_output
            )
        )

    @retry(stop_max_attempt_number=10, wait_fixed=1000)
    def get_blob(self, blob_name):
        # This needs to be retried as AZ is eventually consistent
        obj = self.storage.get_blob(blob_name)
        if obj is None:
            raise IOError("Failed to find uploaded object {} in Azure".format(blob_name))
        return obj
