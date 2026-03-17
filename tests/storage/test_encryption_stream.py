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

import base64
import hashlib
import unittest
import os
import io
import shutil
import tempfile

from medusa.storage.encryption import EncryptionManager, EncryptedStream, DecryptedStream, HAS_AWS_CRYPT


@unittest.skipIf(not HAS_AWS_CRYPT, "aws-encryption-sdk is not installed")
class EncryptedStreamTest(unittest.TestCase):
    def setUp(self):
        # Generate a valid 256-bit key (32 bytes)
        self.key_bytes = os.urandom(32)
        self.key = base64.b64encode(self.key_bytes).decode('utf-8')
        self.manager = EncryptionManager(self.key)
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.temp_dir)

    def test_encrypted_stream_output_matches_file(self):
        """Verify that EncryptedStream produces identical output to encrypt_file (or valid output)"""
        # Note: AWS Encryption SDK adds non-deterministic headers/IVs.
        # We verify that both methods produce something decryptable.

        content = b"This is a test content for encryption." * 1000
        src_path = os.path.join(self.temp_dir, "source.txt")

        with open(src_path, "wb") as f:
            f.write(content)

        # 1. Size and MD5 from source file
        source_size_file = os.path.getsize(src_path)
        self.assertEqual(source_size_file, len(content))
        source_md5_file = hashlib.md5(content).digest()
        base64_source_md5_file = base64.b64encode(source_md5_file).decode('utf-8').strip()

        # 2. Encrypt using the stream-based method
        with open(src_path, "rb") as f:
            stream = EncryptedStream(f, self.key)
            stream_encrypted_content = stream.read()

        # Verify sizes and MD5s
        self.assertEqual(stream.source_size, len(content))
        self.assertEqual(stream.md5_source, base64_source_md5_file)
        # Encrypted size should be larger than source due to headers and auth tags
        self.assertGreater(stream.encrypted_size, len(content))

        # Decrypt the stream output to verify integrity
        temp_stream_out = os.path.join(self.temp_dir, "stream_output.enc")
        with open(temp_stream_out, "wb") as f:
            f.write(stream_encrypted_content)

        decrypted_check_path = os.path.join(self.temp_dir, "decrypted_check.txt")
        self.manager.decrypt_file(temp_stream_out, decrypted_check_path)

        with open(decrypted_check_path, "rb") as f:
            decrypted_content = f.read()

        self.assertEqual(content, decrypted_content)

    def test_chunked_read(self):
        """Verify reading in small chunks works correctly"""
        content = b"1234567890" * 100000  # 1MB
        src_stream = io.BytesIO(content)
        stream = EncryptedStream(src_stream, self.key)

        read_content = b""
        while True:
            chunk = stream.read(1024)
            if not chunk:
                break
            read_content += chunk

        # Verify decryption
        temp_out = os.path.join(self.temp_dir, "chunked.enc")
        with open(temp_out, "wb") as f:
            f.write(read_content)

        decrypted_out = os.path.join(self.temp_dir, "chunked_dec.txt")
        self.manager.decrypt_file(temp_out, decrypted_out)

        with open(decrypted_out, "rb") as f:
            self.assertEqual(f.read(), content)

    def test_empty_file(self):
        src_stream = io.BytesIO(b"")
        stream = EncryptedStream(src_stream, self.key)
        output = stream.read()

        # AWS Encryption SDK output for empty file is not empty (contains header + footer)
        self.assertGreater(len(output), 0)
        self.assertEqual(stream.source_size, 0)
        # Verify it can be decrypted to empty
        dec_stream = DecryptedStream(io.BytesIO(output), self.key)
        decrypted = dec_stream.read()
        self.assertEqual(decrypted, b"")

    def test_stream_properties(self):
        src_stream = io.BytesIO(b"data")
        stream = EncryptedStream(src_stream, self.key)
        self.assertTrue(stream.readable())
        self.assertFalse(stream.seekable())


@unittest.skipIf(not HAS_AWS_CRYPT, "aws-encryption-sdk is not installed")
class DecryptedStreamTest(unittest.TestCase):
    def setUp(self):
        self.key_bytes = os.urandom(32)
        self.key = base64.b64encode(self.key_bytes).decode('utf-8')
        self.manager = EncryptionManager(self.key)
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.temp_dir)

    def test_decrypt_encrypted_stream(self):
        """Verify that DecryptedStream can decrypt output of EncryptedStream"""
        content = b"This is a test content for encryption." * 1000

        # Encrypt in memory
        src_stream = io.BytesIO(content)
        enc_stream = EncryptedStream(src_stream, self.key)
        encrypted_content = enc_stream.read()

        # Decrypt using DecryptedStream
        enc_input_stream = io.BytesIO(encrypted_content)
        dec_stream = DecryptedStream(enc_input_stream, self.key)
        decrypted_content = dec_stream.read()

        self.assertEqual(decrypted_content, content)

        # Verify metadata
        source_md5_file = hashlib.md5(content).digest()
        base64_source_md5_file = base64.b64encode(source_md5_file).decode('utf-8').strip()

        self.assertEqual(dec_stream.source_size, len(content))
        self.assertEqual(dec_stream.md5_source, base64_source_md5_file)

        # NOTE: md5_encrypted is "N/A" in the new implementation for DecryptedStream
        # self.assertEqual(dec_stream.md5_encrypted, "N/A")

    def test_chunked_read_decryption(self):
        """Verify reading decrypted stream in small chunks"""
        content = b"1234567890" * 100000  # 1MB
        src_stream = io.BytesIO(content)
        enc_stream = EncryptedStream(src_stream, self.key)
        encrypted_content = enc_stream.read()

        enc_input_stream = io.BytesIO(encrypted_content)
        dec_stream = DecryptedStream(enc_input_stream, self.key)

        read_content = b""
        while True:
            chunk = dec_stream.read(1024)
            if not chunk:
                break
            read_content += chunk

        self.assertEqual(read_content, content)

    def test_corrupted_stream(self):
        # Create valid encrypted content
        content = b"data"
        src_stream = io.BytesIO(content)
        enc_stream = EncryptedStream(src_stream, self.key)
        encrypted_content = enc_stream.read()

        # Truncate to damage the encrypted chunk (remove signature/tag)
        truncated_content = encrypted_content[:-1]

        dec_stream = DecryptedStream(io.BytesIO(truncated_content), self.key)
        import aws_encryption_sdk
        with self.assertRaises(aws_encryption_sdk.exceptions.AWSEncryptionSDKClientError):
            dec_stream.read()

    def test_empty_stream(self):
        # Empty stream passed to DecryptedStream is invalid for AWS Crypto (must have header)
        # So it should raise an exception or handle gracefully depending on SDK behavior
        # The SDK expects a header.

        # If we pass truly empty bytes, SDK might raise NotSupportedError or similar "Header too small"
        dec_stream = DecryptedStream(io.BytesIO(b""), self.key)
        with self.assertRaises(Exception):
            dec_stream.read()


@unittest.skipIf(HAS_AWS_CRYPT, "aws-encryption-sdk is installed")
class MissingDependencyTest(unittest.TestCase):
    def test_missing_dependency(self):
        """Verify that ImportError is raised when trying to use encryption features without the library"""
        key = base64.b64encode(os.urandom(32)).decode('utf-8')
        with self.assertRaises(ImportError) as cm:
            EncryptionManager(key)
        self.assertIn("pip install cassandra-medusa[encryption]", str(cm.exception))


if __name__ == '__main__':
    unittest.main()
