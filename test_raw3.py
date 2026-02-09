import aws_encryption_sdk
from aws_encryption_sdk.key_providers.raw import RawMasterKeyProvider
from aws_encryption_sdk.identifiers import EncryptionKeyType, WrappingAlgorithm
from aws_encryption_sdk.internal.crypto.wrapping_keys import WrappingKey
import os
import io

key_bytes = os.urandom(32)

class StaticKeyProvider(RawMasterKeyProvider):
    provider_id = "medusa-backup"

    def _get_raw_key(self, key_id):
        if hasattr(self, '_key_name') and key_id == self._key_name:
            wrapping_key = WrappingKey(
                wrapping_algorithm=WrappingAlgorithm.AES_256_GCM_IV12_TAG16_NO_PADDING,
                wrapping_key=self._key_bytes,
                wrapping_key_type=EncryptionKeyType.SYMMETRIC
            )
            # AWS SDK expects _get_raw_key to return just the wrapping_key object, NOT a RawMasterKey!
            return wrapping_key
        raise ValueError("Invalid key id")

provider = StaticKeyProvider()
provider._key_bytes = key_bytes
provider._key_name = b"raw-aes-key"
provider.add_master_key(provider._key_name)

client = aws_encryption_sdk.EncryptionSDKClient()
source = io.BytesIO(b"hello world")

with client.stream(mode="e", source=source, key_provider=provider) as encryptor:
    ciphertext = encryptor.read()
    print("Encrypted:", len(ciphertext))

with client.stream(mode="d", source=io.BytesIO(ciphertext), key_provider=provider) as decryptor:
    print("Decrypted:", decryptor.read())
