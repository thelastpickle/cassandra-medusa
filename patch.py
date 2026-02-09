import re

with open("medusa/storage/encryption.py", "r") as f:
    content = f.read()

# Replace imports
content = content.replace("from aws_encryption_sdk.keyrings.raw import RawAESKeyring", """from aws_encryption_sdk.key_providers.raw import RawMasterKeyProvider
    from aws_encryption_sdk.identifiers import EncryptionKeyType
    from aws_encryption_sdk.internal.crypto.wrapping_keys import WrappingKey""")

# Replace the __init__ setup of keyring
search = """        self.keyring = RawAESKeyring(
            key_namespace=self.key_provider,
            key_name=self.key_name,
            wrapping_key=self.decoded_key,
            wrapping_algorithm=WrappingAlgorithm.AES_256_GCM_IV12_TAG16_NO_PADDING
        )"""

replace = """        class StaticKeyProvider(RawMasterKeyProvider):
            provider_id = self.key_provider

            def _get_raw_key(self, key_id):
                if hasattr(self, '_key_name') and key_id == self._key_name:
                    return WrappingKey(
                        wrapping_algorithm=WrappingAlgorithm.AES_256_GCM_IV12_TAG16_NO_PADDING,
                        wrapping_key=self._key_bytes,
                        wrapping_key_type=EncryptionKeyType.SYMMETRIC
                    )
                raise ValueError("Invalid key id")

        self.master_key_provider = StaticKeyProvider()
        self.master_key_provider._key_bytes = self.decoded_key
        self.master_key_provider._key_name = self.key_name
        self.master_key_provider.add_master_key(self.key_name)"""

content = content.replace(search, replace)

# Replace kwargs in client.stream
content = content.replace("keyring=self.keyring", "key_provider=self.master_key_provider")
content = content.replace("keyring=self.manager.keyring", "key_provider=self.manager.master_key_provider")

with open("medusa/storage/encryption.py", "w") as f:
    f.write(content)
