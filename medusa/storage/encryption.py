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
import io

try:
    import aws_encryption_sdk
    from aws_encryption_sdk import CommitmentPolicy
    from aws_encryption_sdk.identifiers import WrappingAlgorithm
    from aws_encryption_sdk.key_providers.raw import RawMasterKeyProvider
    from aws_encryption_sdk.identifiers import EncryptionKeyType, Algorithm
    from aws_encryption_sdk.internal.crypto.wrapping_keys import WrappingKey
    from aws_encryption_sdk.materials_managers.caching import CachingCryptoMaterialsManager
    from aws_encryption_sdk.caches.local import LocalCryptoMaterialsCache
    HAS_AWS_CRYPT = True
except ImportError:
    HAS_AWS_CRYPT = False
    RawMasterKeyProvider = object


class HashingStreamWrapper(io.RawIOBase):
    """
    Wraps a stream to calculate MD5 and size of data read from it.
    """
    def __init__(self, stream):
        self.stream = stream
        self.hash = hashlib.md5()
        self.size = 0

    def read(self, size=-1):
        chunk = self.stream.read(size)
        if chunk:
            self.hash.update(chunk)
            self.size += len(chunk)
        return chunk

    def readall(self):
        return self.read()

    def readable(self):
        return True

    def seekable(self):
        return False


class _StaticKeyProvider(RawMasterKeyProvider):
    """
    A custom key provider for AWS Encryption SDK that provides a single static AES key.

    This class overrides the private `_get_raw_key` method as mandated by the
    AWS Encryption SDK's `RawMasterKeyProvider` interface. It must provide the
    raw cryptographic key (WrappingKey) corresponding to the requested key ID.
    """

    provider_id = "medusa-backup"

    def configure(self, key_name: str, key_bytes: bytes):
        """
        Explicitly configures the key name and bytes since they cannot be safely
        passed through the constructor due to the AWS Encryption SDK's internal
        use of __new__ to handle configuration parsing.
        """
        self._key_name = key_name
        self._key_bytes = key_bytes

    def _get_raw_key(self, key_id):
        """
        Provides the raw WrappingKey for the given key_id.
        This method is required by the `RawMasterKeyProvider` parent class
        from the AWS Encryption SDK.
        """
        key_id_str = key_id.decode('utf-8') if isinstance(key_id, bytes) else key_id
        expected_id = self._key_name.decode('utf-8') if isinstance(self._key_name, bytes) else self._key_name
        if key_id_str == expected_id:
            return WrappingKey(
                wrapping_algorithm=WrappingAlgorithm.AES_256_GCM_IV12_TAG16_NO_PADDING,
                wrapping_key=self._key_bytes,
                wrapping_key_type=EncryptionKeyType.SYMMETRIC
            )
        raise ValueError("Invalid key id")


class EncryptionManager:
    """Manages encryption and decryption of backup files using AWS Encryption SDK."""

    def __init__(self, key_secret_base64, frame_length=8388608):
        if not HAS_AWS_CRYPT:
            raise ImportError(
                "aws-encryption-sdk is not installed. "
                "Please install it using 'pip install cassandra-medusa[encryption]'"
            )

        if not key_secret_base64:
            raise ValueError("Encryption key is not provided")

        # Validate base64 encoding and key length
        try:
            # Convert to bytes if string
            key_bytes = key_secret_base64 if isinstance(key_secret_base64, bytes) else key_secret_base64.encode('utf-8')
            # Decode with strict validation with validate=True raises an error on any invalid characters or padding.
            self.decoded_key = base64.b64decode(key_bytes, validate=True)
        except Exception as e:
            raise ValueError(
                f"Encryption key is not properly base64-encoded. "
                f"Please ensure the key is base64-encoded. Details: {e}"
            )

        # Validate key length (AWS Encryption SDK supports 128, 192, 256 bits for AES)
        if len(self.decoded_key) != 32:
            raise ValueError(
                f"Encryption key has invalid length. "
                f"Expected 32 bytes (256 bits) when base64-decoded, but got {len(self.decoded_key)} bytes."
            )

        # Initialize AWS Encryption SDK client
        self.client = aws_encryption_sdk.EncryptionSDKClient(
            commitment_policy=CommitmentPolicy.REQUIRE_ENCRYPT_REQUIRE_DECRYPT
        )

        self.key_name = "raw-aes-key"

        self.master_key_provider = _StaticKeyProvider()
        self.master_key_provider.configure(self.key_name, self.decoded_key)
        self.master_key_provider.add_master_key(self.key_name)

        # Initialize cache and CMM to prevent generating a new data key for each file
        self.cache = LocalCryptoMaterialsCache(capacity=100)
        self.cmm = CachingCryptoMaterialsManager(
            master_key_provider=self.master_key_provider,
            cache=self.cache,
            max_age=3600.0,
            max_messages_encrypted=100000,
            max_bytes_encrypted=100 * 1024 * 1024 * 1024  # 100 GB
        )
        self.frame_length = int(frame_length)
        self.algorithm = Algorithm.AES_256_GCM_HKDF_SHA512_COMMIT_KEY

    def encrypt_file(self, src_path, dst_path):
        encrypted_hash = hashlib.md5()
        encrypted_size = 0

        with open(src_path, 'rb') as f_in, open(dst_path, 'wb') as f_out:
            # Wrap f_in to calculate MD5 on the fly
            hashing_source = HashingStreamWrapper(f_in)

            with self.client.stream(
                mode='e',
                source=hashing_source,
                materials_manager=self.cmm,
                frame_length=self.frame_length,
                algorithm=self.algorithm
            ) as encryptor:
                for chunk in encryptor:
                    # Update encrypted metrics
                    f_out.write(chunk)
                    encrypted_size += len(chunk)
                    encrypted_hash.update(chunk)

            source_size = hashing_source.size
            source_hash = hashing_source.hash

        return (
            base64.b64encode(encrypted_hash.digest()).decode('utf-8').strip(),
            encrypted_size,
            base64.b64encode(source_hash.digest()).decode('utf-8').strip(),
            source_size
        )

    def decrypt_file(self, src_path, dst_path):
        with open(src_path, 'rb') as f_in, open(dst_path, 'wb') as f_out:
            with self.client.stream(
                mode='d',
                source=f_in,
                materials_manager=self.cmm
            ) as decryptor:
                for chunk in decryptor:
                    f_out.write(chunk)


class EncryptionStreamBase(io.RawIOBase):
    def __init__(self, source_stream, key_secret_base64=None, frame_length=8388608, *, manager=None):
        if manager is not None:
            self.manager = manager
        else:
            if not HAS_AWS_CRYPT:
                raise ImportError(
                    "aws-encryption-sdk is not installed. "
                    "Please install it using 'pip install cassandra-medusa[encryption]'"
                )
            self.manager = EncryptionManager(key_secret_base64, frame_length)
        self.source_stream = source_stream

        self.encrypted_hash = hashlib.md5()
        self.encrypted_size = 0

        self.buffer = b""
        self.eof = False

        self.aws_stream = None

    def readable(self):
        return True

    def seekable(self):
        return False

    def readall(self):
        return self.read()

    def close(self):
        if not self.closed:
            if self.aws_stream is not None and hasattr(self.aws_stream, 'close'):
                self.aws_stream.close()
            if self.source_stream is not None and hasattr(self.source_stream, 'close'):
                self.source_stream.close()
            super().close()

    @property
    def md5_encrypted(self):
        return base64.b64encode(self.encrypted_hash.digest()).decode('utf-8').strip()


class EncryptedStream(EncryptionStreamBase):
    def __init__(self, source_stream, key_secret_base64=None, frame_length=8388608, *, manager=None):
        super().__init__(source_stream, key_secret_base64, frame_length, manager=manager)

        self.hashing_source = HashingStreamWrapper(source_stream)

        self.aws_stream = self.manager.client.stream(
            mode='e',
            source=self.hashing_source,
            materials_manager=self.manager.cmm,
            frame_length=self.manager.frame_length,
            algorithm=self.manager.algorithm
        )
        self.iterator = iter(self.aws_stream)

    def read(self, size=-1):
        if size == -1:
            # Read everything
            chunks = [self.buffer] if self.buffer else []
            for chunk in self.iterator:
                chunks.append(chunk)
                self.encrypted_size += len(chunk)
                self.encrypted_hash.update(chunk)

            self.buffer = b""
            self.eof = True
            return b"".join(chunks)

        # Fill buffer from iterator if we don't have enough data
        if len(self.buffer) < size:
            chunks = [self.buffer] if self.buffer else []
            current_len = len(self.buffer)
            while current_len < size:
                try:
                    chunk = next(self.iterator)
                    self.encrypted_size += len(chunk)
                    self.encrypted_hash.update(chunk)
                    chunks.append(chunk)
                    current_len += len(chunk)
                except StopIteration:
                    self.eof = True
                    break
            self.buffer = b"".join(chunks)

        # Return requested size from buffer
        data = self.buffer[:size]
        self.buffer = self.buffer[size:]

        return data

    @property
    def source_size(self):
        return self.hashing_source.size

    @property
    def md5_source(self):
        return base64.b64encode(self.hashing_source.hash.digest()).decode('utf-8').strip()


class DecryptedStream(EncryptionStreamBase):
    def __init__(self, source_stream, key_secret_base64=None, frame_length=8388608, *, manager=None):
        super().__init__(source_stream, key_secret_base64, frame_length, manager=manager)

        self.aws_stream = self.manager.client.stream(
            mode='d',
            source=source_stream,
            materials_manager=self.manager.cmm
        )
        self.iterator = iter(self.aws_stream)

        self.plaintext_hash = hashlib.md5()
        self.plaintext_size = 0

    def read(self, size=-1):
        if size == -1:
            # Read everything
            chunks = [self.buffer] if self.buffer else []
            for chunk in self.iterator:
                chunks.append(chunk)
                self.plaintext_size += len(chunk)
                self.plaintext_hash.update(chunk)

            self.buffer = b""
            self.eof = True
            return b"".join(chunks)

        # Fill buffer from iterator if we don't have enough data
        if len(self.buffer) < size:
            chunks = [self.buffer] if self.buffer else []
            current_len = len(self.buffer)
            while current_len < size:
                try:
                    chunk = next(self.iterator)
                    self.plaintext_size += len(chunk)
                    self.plaintext_hash.update(chunk)
                    chunks.append(chunk)
                    current_len += len(chunk)
                except StopIteration:
                    self.eof = True
                    break
            self.buffer = b"".join(chunks)

        # Return requested size from buffer
        data = self.buffer[:size]
        self.buffer = self.buffer[size:]

        return data

    @property
    def source_size(self):
        return self.plaintext_size

    @property
    def md5_source(self):
        return base64.b64encode(self.plaintext_hash.digest()).decode('utf-8').strip()

    @property
    def md5_encrypted(self):
        return "N/A"
