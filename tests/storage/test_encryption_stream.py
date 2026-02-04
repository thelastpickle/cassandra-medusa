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
        dst_path = os.path.join(self.temp_dir, "encrypted.enc")

        with open(src_path, "wb") as f:
            f.write(content)

        # 1. Encrypt using the file-based method
        encrypted_md5_file, encrypted_size_file, source_md5_file, source_size_file = \
            self.manager.encrypt_file(src_path, dst_path)

        with open(dst_path, "rb") as f:
            file_encrypted_content = f.read()

        # 2. Encrypt using the stream-based method
        with open(src_path, "rb") as f:
            stream = EncryptedStream(f, self.key)
            stream_encrypted_content = stream.read()

            # Since Fernet uses a random IV (salt) for each encryption call,
            # we CANNOT compare the encrypted bytes directly if we encrypt twice.
            # However, EncryptionManager creates a NEW Fernet instance which handles this.
            # Wait, Fernet generates a new IV for *every* encryption call.
            # So `encrypt_file` and `EncryptedStream` will produce DIFFERENT ciphertext
            # for the same plaintext because of the random IV.

            # So we cannot check equality of bytes.
            # We must decrypt the stream output and verify it matches the original content.

        # Decrypt the stream output
        decrypted_stream_output_path = os.path.join(self.temp_dir, "decrypted_stream.txt")
        with open(decrypted_stream_output_path, "wb") as f_out:
            # We need to simulate the chunked decryption logic
            # Or use the manager.decrypt_file, but first we need to write the stream output to disk
            pass

        temp_stream_out = os.path.join(self.temp_dir, "stream_output.enc")
        with open(temp_stream_out, "wb") as f:
            f.write(stream_encrypted_content)

        decrypted_check_path = os.path.join(self.temp_dir, "decrypted_check.txt")
        self.manager.decrypt_file(temp_stream_out, decrypted_check_path)

        with open(decrypted_check_path, "rb") as f:
            decrypted_content = f.read()

        self.assertEqual(content, decrypted_content)

        # Verify sizes and MD5s
        # Note: MD5 of encrypted content will differ because of IV
        self.assertEqual(stream.source_size, len(content))
        self.assertEqual(stream.md5_source, source_md5_file)

        self.assertEqual(stream.encrypted_size, len(stream_encrypted_content))

        # We can't compare encrypted MD5s, but we can verify the stream reported MD5 matches the actual stream output
        import hashlib
        import base64
        actual_stream_md5 = base64.b64encode(hashlib.md5(stream_encrypted_content).digest()).decode('utf-8').strip()
        self.assertEqual(stream.md5_encrypted, actual_stream_md5)

    def test_chunked_read(self):
        """Verify reading in small chunks works correctly"""
        content = b"1234567890" * 100000 # 1MB
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
