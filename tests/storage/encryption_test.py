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
import base64
from cryptography.fernet import Fernet
from medusa.storage.encryption import EncryptionManager


class EncryptionManagerTest(unittest.TestCase):
    def setUp(self):
        self.key = Fernet.generate_key()
        self.key_b64 = self.key.decode('utf-8')
        self.manager = EncryptionManager(self.key_b64)

    def test_init_invalid_key(self):
        with self.assertRaises(Exception):
            EncryptionManager("invalid_base64_key")

        with self.assertRaises(ValueError):
            EncryptionManager(None)

    def test_encrypt_decrypt_file(self):
        content = b"Hello, Medusa!" * 1000  # 14KB

        with tempfile.TemporaryDirectory() as temp_dir:
            src_path = os.path.join(temp_dir, "source.txt")
            enc_path = os.path.join(temp_dir, "encrypted.enc")
            dec_path = os.path.join(temp_dir, "decrypted.txt")

            with open(src_path, "wb") as f:
                f.write(content)

            # Encrypt
            enc_md5, enc_size, src_md5, src_size = self.manager.encrypt_file(src_path, enc_path)

            self.assertEqual(src_size, len(content))
            self.assertTrue(os.path.exists(enc_path))
            self.assertGreater(enc_size, src_size) # Encrypted file should be larger due to overhead

            # Decrypt
            self.manager.decrypt_file(enc_path, dec_path)

            self.assertTrue(os.path.exists(dec_path))
            with open(dec_path, "rb") as f:
                decrypted_content = f.read()

            self.assertEqual(content, decrypted_content)

    def test_encrypt_decrypt_large_file(self):
        # Slightly larger than CHUNK_SIZE (1MB) to test chunking
        content = os.urandom(1024 * 1024 + 500)

        with tempfile.TemporaryDirectory() as temp_dir:
            src_path = os.path.join(temp_dir, "source_large.txt")
            enc_path = os.path.join(temp_dir, "encrypted_large.enc")
            dec_path = os.path.join(temp_dir, "decrypted_large.txt")

            with open(src_path, "wb") as f:
                f.write(content)

            # Encrypt
            self.manager.encrypt_file(src_path, enc_path)

            # Decrypt
            self.manager.decrypt_file(enc_path, dec_path)

            with open(dec_path, "rb") as f:
                decrypted_content = f.read()

            self.assertEqual(content, decrypted_content)

if __name__ == '__main__':
    unittest.main()
