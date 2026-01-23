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
import struct
import logging
import os
from cryptography.fernet import Fernet

# Chunk size for reading/encrypting.
# 1MB seems reasonable balance between memory usage and overhead.
CHUNK_SIZE = 1024 * 1024
# Max reasonable chunk size (1MB + Fernet overhead + padding). Safety check.
MAX_CHUNK_SIZE = 2 * 1024 * 1024


class EncryptionManager:
    """Manages encryption and decryption of backup files using Fernet symmetric encryption."""

    def __init__(self, key_secret_base64):
        if not key_secret_base64:
            raise ValueError("Encryption key is not provided")
        
        # Validate base64 encoding and key length
        # Fernet uses URL-safe base64 encoding (44 chars for a 32-byte key)
        try:
            # Convert to bytes if string
            key_bytes = key_secret_base64 if isinstance(key_secret_base64, bytes) else key_secret_base64.encode('utf-8')
            # Decode using URL-safe base64 (Fernet standard)
            decoded_key = base64.urlsafe_b64decode(key_bytes)
        except Exception as e:
            raise ValueError(
                f"Encryption key is not properly base64-encoded. "
                f"Please ensure the key is base64-encoded. Details: {e}"
            )
        
        # Validate key length (Fernet requires exactly 32 bytes when decoded)
        if len(decoded_key) != 32:
            raise ValueError(
                f"Encryption key has invalid length. "
                f"Expected 32 bytes when base64-decoded, but got {len(decoded_key)} bytes. "
                f"Generate a valid key using: "
                f"python3 -c \"from cryptography.fernet import Fernet; "
                f"print(Fernet.generate_key().decode())\""
            )
        
        try:
            self.fernet = Fernet(key_secret_base64)
        except Exception as e:
            logging.error(f"Failed to initialize encryption with provided key: {e}")
            raise ValueError(f"Failed to initialize encryption with provided key: {e}")

    def encrypt_file(self, src_path, dst_path):
        source_hash = hashlib.md5()
        encrypted_hash = hashlib.md5()
        source_size = 0
        encrypted_size = 0

        with open(src_path, 'rb') as f_in, open(dst_path, 'wb') as f_out:
            while True:
                chunk = f_in.read(CHUNK_SIZE)
                if not chunk:
                    break

                source_size += len(chunk)
                source_hash.update(chunk)

                encrypted_chunk = self.fernet.encrypt(chunk)

                # Write length of the encrypted chunk (4 bytes, big endian)
                chunk_len = len(encrypted_chunk)
                len_bytes = struct.pack('>I', chunk_len)
                f_out.write(len_bytes)
                # Write the encrypted chunk
                f_out.write(encrypted_chunk)

                encrypted_size += 4 + chunk_len
                # For encrypted hash, we hash the exact bytes we write to disk
                encrypted_hash.update(len_bytes)
                encrypted_hash.update(encrypted_chunk)

        return (
            base64.b64encode(encrypted_hash.digest()).decode('utf-8').strip(),
            encrypted_size,
            base64.b64encode(source_hash.digest()).decode('utf-8').strip(),
            source_size
        )

    def decrypt_file(self, src_path, dst_path):
        with open(src_path, 'rb') as f_in, open(dst_path, 'wb') as f_out:
            while True:
                len_bytes = f_in.read(4)
                if not len_bytes:
                    break

                chunk_len = struct.unpack('>I', len_bytes)[0]

                # Sanity check for chunk length to detect non-encrypted files or corruption early
                if chunk_len > MAX_CHUNK_SIZE:
                    file_size = os.path.getsize(src_path)
                    header_hex = len_bytes.hex()
                    raise IOError(
                        f"Corrupted encrypted file or plaintext file detected: {src_path} "
                        f"(size: {file_size}). Chunk length {chunk_len} exceeds max {MAX_CHUNK_SIZE}. "
                        f"Header bytes: {header_hex}"
                    )

                encrypted_chunk = f_in.read(chunk_len)

                if len(encrypted_chunk) != chunk_len:
                    raise IOError(
                        f"Corrupted encrypted file: {src_path}. "
                        f"Expected {chunk_len} bytes, got {len(encrypted_chunk)}"
                    )

                decrypted_chunk = self.fernet.decrypt(encrypted_chunk)
                f_out.write(decrypted_chunk)
