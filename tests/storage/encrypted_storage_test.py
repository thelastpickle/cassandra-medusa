# -*- coding: utf-8 -*-
# Copyright 2024 DataStax, Inc.
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

import unittest
import os
import tempfile
import pathlib
import io
import typing as t
from unittest.mock import MagicMock, patch
from cryptography.fernet import Fernet

from medusa.storage.abstract_storage import AbstractStorage, ManifestObject, AbstractBlob
from medusa.storage.encryption import EncryptionManager

class MockStorage(AbstractStorage):
    def connect(self):
        pass
    def disconnect(self):
        pass
    async def _list_blobs(self, prefix=None):
        return []
    async def _upload_object(self, data: io.BytesIO, object_key: str, headers: t.Dict[str, str]) -> AbstractBlob:
        return MagicMock()
    async def _download_blob(self, src: str, dest: str):
        pass
    async def _upload_blob(self, src: str, dest: str) -> ManifestObject:
        src_path = pathlib.Path(src)
        object_key = AbstractStorage.path_maybe_with_parent(dest, src_path)
        return ManifestObject(path=object_key, size=100, MD5="enc_hash")
    async def _get_object(self, object_key):
        pass
    async def _read_blob_as_bytes(self, blob: AbstractBlob) -> bytes:
        pass
    async def _delete_object(self, obj: AbstractBlob):
        pass
    @staticmethod
    def blob_matches_manifest(blob, object_in_manifest, enable_md5_checks=False):
        pass
    @staticmethod
    def file_matches_storage(src, cached_item, threshold=None, enable_md5_checks=False):
        pass
    @staticmethod
    def compare_with_manifest(actual_size, size_in_manifest, actual_hash=None, hash_in_manifest=None, threshold=None):
        pass

class EncryptedStorageTest(unittest.TestCase):
    def setUp(self):
        self.key = Fernet.generate_key().decode('utf-8')

        # Setup config
        config_dict = {
            'storage_provider': 'mock',
            'bucket_name': 'test_bucket',
            'concurrent_transfers': '1',
            'key_secret_base64': self.key,
            'encryption_tmp_dir': None
        }

        # Create a mock config object to access config_dict attributes via dot notation
        self.mock_config = MagicMock()
        for k, v in config_dict.items():
            setattr(self.mock_config, k, v)

        self.storage = MockStorage(self.mock_config)

    def test_upload_encrypted_blobs(self):
        test_msg = b"plaintext content"
        test_msg_size = len(test_msg)
        with tempfile.TemporaryDirectory() as temp_dir:
            src_file = os.path.join(temp_dir, "test.txt")
            with open(src_file, "wb") as f:
                f.write(test_msg)

            srcs = [pathlib.Path(src_file)]
            dest = "backup/data"

            # We want to check the result of upload_blobs.
            # upload_blobs use MockStorage._upload_blob internally and return a constant ManifestObject.
            manifests = self.storage.upload_blobs(srcs, dest)

            self.assertEqual(len(manifests), 1)
            mo = manifests[0]

            # Check if source metadata is populated
            self.assertEqual(mo.source_size, test_msg_size)
            self.assertIsNotNone(mo.source_MD5)

            # The MockStorage._upload_blob returns size=100, MD5="enc_hash"
            self.assertEqual(mo.size, 100)
            self.assertEqual(mo.MD5, "enc_hash")

            # Verify the path is correct
            self.assertEqual(mo.path, f"{dest}/test.txt")

    def test_upload_encrypted_blobs_with_secondary_index(self):
        test_msg = b"index content"
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a structure like .../table/.index_name/file.db
            index_dir = os.path.join(temp_dir, ".test_idx")
            os.mkdir(index_dir)

            src_file = os.path.join(index_dir, "test.db")
            with open(src_file, "wb") as f:
                f.write(test_msg)

            srcs = [pathlib.Path(src_file)]
            dest = "backup/data"

            # Mock _upload_blob to verify the object key passed to it
            # We want to ensure it includes the .test_idx/ parent
            original_upload_blob = self.storage._upload_blob

            async def mock_upload_blob(src, dest):
                # We can inspect the src path here.
                # src should be the path to the encrypted temp file.
                # It should reside in a .test_idx subdir of the temp dir.
                src_path = pathlib.Path(src)
                if not src_path.parent.name.startswith("."):
                     raise ValueError(f"Temp file {src} is not in a secondary index folder")

                # Check that path_maybe_with_parent works as expected
                key = AbstractStorage.path_maybe_with_parent(dest, src_path)
                if ".test_idx" not in key:
                     raise ValueError(f"Object key {key} does not contain index name")

                return await original_upload_blob(src, dest)

            with patch.object(self.storage, '_upload_blob', side_effect=mock_upload_blob):
                manifests = self.storage.upload_blobs(srcs, dest)

            self.assertEqual(len(manifests), 1)
            mo = manifests[0]
            self.assertIn(".test_idx", mo.path)

    @patch("medusa.storage.abstract_storage.AbstractStorage._download_blobs")
    def test_download_encrypted_blobs(self, mock_download_blobs_impl):
        original_content = b"restored content"

        # Test with a specific temp dir configuration
        self.storage.config.encryption_tmp_dir = tempfile.gettempdir()

        with tempfile.TemporaryDirectory() as temp_dir:
            manager = EncryptionManager(self.key)
            # We need to mock the internal _download_blobs to simulate downloading the ENCRYPTED file
            # to the temporary directory. To do that, we setup side_effect to mock_download_blobs_impl
            async def side_effect(srcs, dest_dir):
                # srcs is list of strings (paths relative to bucket)
                # dest_dir is the temp dir
                for src in srcs:
                    file_name = pathlib.Path(src).name
                    dest_path = os.path.join(dest_dir, file_name)
                    # encrypt content to file
                    with tempfile.NamedTemporaryFile() as tmp_src:
                        tmp_src.write(original_content)
                        tmp_src.flush()
                        manager.encrypt_file(tmp_src.name, dest_path)

            mock_download_blobs_impl.side_effect = side_effect

            # Test parameters
            srcs = ["backup/data/restored.txt"]
            dest = pathlib.Path(temp_dir) / "final_dest"

            self.storage.download_blobs(srcs, dest)

            # Check if file exists in final destination and is decrypted
            final_file = dest / "restored.txt"
            self.assertTrue(final_file.exists())

            with open(final_file, "rb") as f:
                self.assertEqual(f.read(), original_content)

    @patch("medusa.storage.abstract_storage.AbstractStorage._download_blobs")
    def test_download_encrypted_blobs_skips_plaintext_files(self, mock_download_blobs_impl):
        # Verify that metadata files are NOT decrypted but moved directly
        manager = EncryptionManager(self.key)

        # Test with a specific temp dir configuration
        self.storage.config.encryption_tmp_dir = tempfile.gettempdir()

        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a plaintext content
            original_content = b'{"json": "plaintext"}'

            async def side_effect(srcs, dest_dir):
                for src in srcs:
                    file_name = pathlib.Path(src).name
                    dest_path = os.path.join(dest_dir, file_name)
                    # Write PLAINTEXT, do NOT encrypt
                    with open(dest_path, 'wb') as f:
                        f.write(original_content)

            mock_download_blobs_impl.side_effect = side_effect

            # Test parameters with various metadata files matching the regex
            srcs = [
                "backup/meta/manifest.json",
                "backup/meta/manifest_fqdn.json",
                "backup/meta/schema.cql",
                "backup/meta/tokenmap.json",
                "backup/meta/server_version.json",
                "backup/index/backup_name.txt"
            ]
            dest = pathlib.Path(temp_dir) / "final_dest"

            # This should NOT raise invalid chunk error
            self.storage.download_blobs(srcs, dest)

            # Check if files exist in final destination and are plaintext
            for src in srcs:
                final_file = dest / pathlib.Path(src).name
                self.assertTrue(final_file.exists(), f"File {final_file} should exist")

                with open(final_file, "rb") as f:
                    self.assertEqual(f.read(), original_content, f"Content mismatch for {final_file}")

if __name__ == '__main__':
    unittest.main()
