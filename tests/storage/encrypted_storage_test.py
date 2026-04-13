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
from unittest.mock import MagicMock
import base64

from medusa.storage.abstract_storage import AbstractStorage, ManifestObject, AbstractBlob
from medusa.storage.encryption import EncryptionManager, HAS_AWS_CRYPT


class MockStorage(AbstractStorage):
    """
    A mock implementation of AbstractStorage specifically designed to test the client-side encryption flow
    in EncryptedStorageTest.

    It encapsulates the behavior of a storage backend that supports streaming uploads by overriding
    _upload_object_from_stream. In production, subclasses like S3BaseStorage use this method to stream
    data directly to the remote storage. During these streaming uploads, the EncryptedStream wrapper
    encrypts data on the fly and calculates the source metadata (source_size, source_md5) simultaneously.

    This mock consumes the provided stream (which simulates the upload process) and extracts the
    dynamically calculated source properties to build a ManifestObject. This allows tests to verify
    that the encryption layer correctly processes the data and computes the necessary metadata
    without requiring a real storage backend or performing actual network I/O.
    """
    def connect(self):
        # Mock implementation - no connection needed for testing
        pass

    def disconnect(self):
        # Mock implementation - no disconnection needed for testing
        pass

    async def _list_blobs(self, prefix=None):
        return []

    async def _upload_object(self, data: io.BytesIO, object_key: str, headers: t.Dict[str, str]) -> AbstractBlob:
        # Mock implementation: return a Blob with a fixed size/hash for basic tests
        return AbstractBlob(name=object_key, size=100, hash="enc_hash", last_modified=None, storage_class=None)

    async def _upload_object_from_stream(
            self, stream: io.BytesIO, object_key: str,
            headers: t.Dict[str, str]) -> ManifestObject:
        # Consume the stream so that size/md5 are calculated
        data = stream.read()

        # In a real scenario, this 'data' is the encrypted data
        blob_size = len(data)

        # If the stream is an EncryptedStream, we can get the source info from it
        source_size = getattr(stream, 'source_size', None)
        source_md5 = getattr(stream, 'md5_source', None)

        return ManifestObject(
            path=object_key,
            size=blob_size,
            MD5="enc_hash_of_stream",
            source_size=source_size,
            source_MD5=source_md5
        )

    async def _download_blob(self, src: str, dest: str):
        # Mock implementation - actual download behavior is mocked in individual tests.
        # Behavior will be defined by the side_effect of the mock in the test case with AsyncMock.
        pass

    async def _upload_blob(self, src: str, dest: str) -> ManifestObject:
        src_path = pathlib.Path(src)
        object_key = AbstractStorage.path_maybe_with_parent(dest, src_path)
        return ManifestObject(path=object_key, size=100, MD5="enc_hash")

    async def _get_object(self, object_key):
        # Mock implementation - not used in encryption tests
        pass

    async def _read_blob_as_bytes(self, blob: AbstractBlob) -> bytes:
        # Mock implementation - not used in encryption tests
        pass

    async def _delete_object(self, obj: AbstractBlob):
        # Mock implementation - not used in encryption tests
        pass

    @staticmethod
    def blob_matches_manifest(blob, object_in_manifest, enable_md5_checks=False):
        # Mock implementation - not used in encryption tests
        pass

    @staticmethod
    def file_matches_storage(src, cached_item, threshold=None, enable_md5_checks=False):
        # Mock implementation - not used in encryption tests
        pass

    @staticmethod
    def compare_with_manifest(actual_size, size_in_manifest, actual_hash=None, hash_in_manifest=None,
                              threshold=None):
        # Mock implementation - not used in encryption tests
        pass


@unittest.skipIf(not HAS_AWS_CRYPT, "aws-encryption-sdk is not installed")
class EncryptedStorageTest(unittest.TestCase):
    # Define sub folder suffix used in tests
    TEST_SUB_FOLDER_SUFFIX = ".test_sub_folder"

    def setUp(self):
        self.key = base64.b64encode(os.urandom(32)).decode('utf-8')

        # Setup config
        config_dict = {
            'storage_provider': 'mock',
            'bucket_name': 'test_bucket',
            'concurrent_transfers': '1',
            'key_secret_base64': self.key,
            'encryption_tmp_dir': None,
            'encryption_frame_length': '8388608',
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

            # This funcion will call our MockStorage._upload_object_from_stream which will
            # return a ManifestObject with the path set to dest + filename
            manifests = self.storage.upload_blobs(srcs, dest)

            self.assertEqual(len(manifests), 1)
            mo = manifests[0]

            # Check if source metadata is populated correctly from the stream
            self.assertEqual(mo.source_size, test_msg_size)
            self.assertIsNotNone(mo.source_MD5)

            # Check encrypted metadata (calculated by MockStorage._upload_object_from_stream)
            # The size should be > 0 (encrypted content)
            self.assertTrue(mo.size > 0)
            self.assertEqual(mo.MD5, "enc_hash_of_stream")

            # Verify the path is correct
            self.assertEqual(mo.path, f"{dest}/test.txt")

    def test_upload_encrypted_blobs_with_subfolder(self):
        test_msg = b"subfolder content"
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a structure like .../table/.subfolder/file.db
            subfolder_dir = os.path.join(temp_dir, self.TEST_SUB_FOLDER_SUFFIX)
            os.mkdir(subfolder_dir)

            src_file = os.path.join(subfolder_dir, "test.db")
            with open(src_file, "wb") as f:
                f.write(test_msg)

            srcs = [pathlib.Path(src_file)]
            dest = "backup/data"

            # This funcion will call our MockStorage._upload_object_from_stream which will
            # return a ManifestObject with the path set to dest + filename
            manifests = self.storage.upload_blobs(srcs, dest)

            self.assertEqual(len(manifests), 1)
            mo = manifests[0]
            self.assertIn(self.TEST_SUB_FOLDER_SUFFIX, mo.path)

            # Verify it preserved the file name
            self.assertTrue(mo.path.endswith("/test.db"))

    def test_download_encrypted_blobs(self):
        from unittest.mock import AsyncMock
        original_content = b"restored content"

        # Test with a specific temp dir configuration
        self.storage.config.encryption_tmp_dir = tempfile.gettempdir()

        with tempfile.TemporaryDirectory() as temp_dir:
            manager = EncryptionManager(self.key)

            # We need to mock the internal _download_blob to simulate downloading the ENCRYPTED file
            # to the temporary directory
            async def side_effect(src, dest_dir):
                # src is a string (path relative to bucket)
                # dest_dir is the temp dir
                file_name = pathlib.Path(src).name
                dest_path = os.path.join(dest_dir, file_name)
                # encrypt content to file
                with tempfile.NamedTemporaryFile(delete=False) as tmp_src:
                    tmp_src.write(original_content)
                    tmp_src.flush()
                    manager.encrypt_file(tmp_src.name, dest_path)
                    os.remove(tmp_src.name)

            # Mock the _download_blob method directly on the instance
            self.storage._download_blob = AsyncMock(side_effect=side_effect)

            # Test parameters
            srcs = ["backup/data/restored.txt"]
            dest = pathlib.Path(temp_dir) / "final_dest"

            self.storage.download_blobs(srcs, dest)

            # Check if file exists in final destination and is decrypted
            final_file = dest / "restored.txt"
            self.assertTrue(final_file.exists())

            with open(final_file, "rb") as f:
                self.assertEqual(f.read(), original_content)

    def test_download_encrypted_blobs_skips_plaintext_files(self):
        from unittest.mock import AsyncMock
        # Verify that metadata files are NOT decrypted but moved directly.
        # Test with a specific temp dir configuration
        self.storage.config.encryption_tmp_dir = tempfile.gettempdir()

        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a plaintext content
            original_content = b'{"json": "plaintext"}'

            async def side_effect(src, dest_dir):
                # src is a string (path relative to bucket)
                # dest_dir is the temp dir
                file_name = pathlib.Path(src).name
                dest_path = os.path.join(dest_dir, file_name)
                # Write PLAINTEXT, do NOT decrypt
                with open(dest_path, 'wb') as f:
                    f.write(original_content)

            # Mock the _download_blob method directly on the instance
            self.storage._download_blob = AsyncMock(side_effect=side_effect)

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

    def test_download_encrypted_blobs_via_stream(self):
        from unittest.mock import AsyncMock

        original_content = b"restored content via streaming"

        # Create encrypted content in memory
        src_stream = io.BytesIO(original_content)
        from medusa.storage.encryption import EncryptedStream
        enc_stream = EncryptedStream(src_stream, self.key)
        encrypted_content = enc_stream.read()

        # Mock _download_object_as_stream to return the encrypted content
        self.storage._download_object_as_stream = AsyncMock(return_value=io.BytesIO(encrypted_content))
        # Also mock _download_blob so it doesn't try to download anything for streaming path
        self.storage._download_blob = AsyncMock()

        with tempfile.TemporaryDirectory() as temp_dir:
            srcs = ["backup/data/restored_stream.txt"]
            dest = pathlib.Path(temp_dir) / "final_dest"

            self.storage.download_blobs(srcs, dest)

            final_file = dest / "restored_stream.txt"
            self.assertTrue(final_file.exists())

            with open(final_file, "rb") as f:
                self.assertEqual(f.read(), original_content)

            self.storage._download_object_as_stream.assert_called_with("backup/data/restored_stream.txt")
            self.storage._download_blob.assert_not_called()

    def test_download_encrypted_blobs_streaming_plaintext_file(self):
        # Verify that PLAINTEXT files are downloaded directly even when using streaming logic
        from unittest.mock import AsyncMock

        original_content = b'{"json": "plaintext"}'

        # Mock _download_blob to simulate downloading the file directly
        async def side_effect(src, dest):
            # src is a string (path relative to bucket)
            # dest is the destination DIRECTORY (or full path depending on implementation)
            # AbstractStorage.path_maybe_with_parent constructs the final path
            # But _download_blob contract usually takes src (key) and dest (local path/dir)

            # In _download_encrypted_blob, we call _download_blob(src, dest)
            # where dest is the target directory.
            # However, looking at _download_encrypted_blob implementation:
            # await self._download_blob(src, dest)
            # So the mock should write to dest / basename(src)

            src_path = pathlib.Path(src)
            # Replicate path construction
            file_path = AbstractStorage.path_maybe_with_parent(str(dest), src_path)

            pathlib.Path(file_path).parent.mkdir(parents=True, exist_ok=True)
            with open(file_path, 'wb') as f:
                f.write(original_content)

        self.storage._download_blob = AsyncMock(side_effect=side_effect)
        self.storage._download_object_as_stream = AsyncMock()

        with tempfile.TemporaryDirectory() as temp_dir:
            # Use a filename that matches PLAINTEXT_FILES_REGEX
            srcs = ["backup/meta/manifest.json"]
            dest = pathlib.Path(temp_dir) / "final_dest"

            self.storage.download_blobs(srcs, dest)

            final_file = dest / "manifest.json"
            self.assertTrue(final_file.exists())

            with open(final_file, "rb") as f:
                self.assertEqual(f.read(), original_content)

            # Ensure we called _download_blob directly
            self.storage._download_blob.assert_called_once()
            # Ensure we did NOT try to stream/decrypt
            self.storage._download_object_as_stream.assert_not_called()


if __name__ == '__main__':
    unittest.main()
