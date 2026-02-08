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
from cryptography.fernet import Fernet
from medusa.storage.encryption import EncryptionManager, EncryptedStream


class EncryptedStreamTest(unittest.TestCase):
    def setUp(self):
        self.key = Fernet.generate_key()
        self.manager = EncryptionManager(self.key)
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.temp_dir)

    def test_encrypted_stream_output_matches_file(self):
        """Verify that EncryptedStream produces identical output to encrypt_file"""
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
        # Since Fernet uses a random IV (salt) for each encryption call,
        # We must decrypt the stream output and verify it matches the original content.
        with open(src_path, "rb") as f:
            stream = EncryptedStream(f, self.key)
            stream_encrypted_content = stream.read()
        # Decrypt the stream output
        temp_stream_out = os.path.join(self.temp_dir, "stream_output.enc")
        with open(temp_stream_out, "wb") as f:
            f.write(stream_encrypted_content)

        decrypted_check_path = os.path.join(self.temp_dir, "decrypted_check.txt")
        self.manager.decrypt_file(temp_stream_out, decrypted_check_path)

        with open(decrypted_check_path, "rb") as f:
            decrypted_content = f.read()

        self.assertEqual(content, decrypted_content)

        # Verify sizes and MD5s
        self.assertEqual(stream.source_size, len(content))
        self.assertEqual(stream.md5_source, base64_source_md5_file)
        self.assertEqual(stream.encrypted_size, len(stream_encrypted_content))

        # We can't compare encrypted MD5s, but we can verify the stream reported MD5 matches the actual stream output
        encrypted_md5_file = hashlib.md5(stream_encrypted_content).digest()
        base64_encrypted_md5_file = base64.b64encode(encrypted_md5_file).decode('utf-8').strip()
        self.assertEqual(stream.md5_encrypted, base64_encrypted_md5_file)

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
        self.assertEqual(output, b"")
        self.assertEqual(stream.source_size, 0)
        self.assertEqual(stream.encrypted_size, 0)

    def test_stream_properties(self):
        src_stream = io.BytesIO(b"data")
        stream = EncryptedStream(src_stream, self.key)
        self.assertTrue(stream.readable())
        self.assertFalse(stream.seekable())


if __name__ == '__main__':
    unittest.main()
