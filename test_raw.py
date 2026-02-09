import aws_encryption_sdk
from aws_encryption_sdk.key_providers.raw import RawMasterKeyProvider
import os
import io

key_bytes = os.urandom(32)

class StaticKeyProvider(RawMasterKeyProvider):
    provider_id = "medusa-backup"

    def __init__(self, key_bytes, key_name):
        self._key_bytes = key_bytes
        self._key_name = key_name
        # Add master key without calling the parent initializer as it's not strictly necessary if we bypass MasterKeyProviderConfig
        # But let's see how it behaves in AWS Encryption SDK 3.x
        super(StaticKeyProvider, self).__init__()
        self.add_master_key(self._key_name)

    def _get_raw_key(self, key_id):
        if key_id == self._key_name:
            return self._key_bytes
        raise ValueError("Invalid key id")

provider = StaticKeyProvider(key_bytes, "raw-aes-key")

client = aws_encryption_sdk.EncryptionSDKClient()
source = io.BytesIO(b"hello world")

with client.stream(mode="e", source=source, key_provider=provider) as encryptor:
    ciphertext = encryptor.read()
    print("Encrypted:", len(ciphertext))

with client.stream(mode="d", source=io.BytesIO(ciphertext), key_provider=provider) as decryptor:
    print("Decrypted:", decryptor.read())
