import re

with open("tests/storage/encryption_test.py", "r") as f:
    content = f.read()

# Replace Fernet.generate_key() with proper 256-bit base64 key
content = content.replace("self.key = Fernet.generate_key()", "self.key_bytes = os.urandom(32)\n        self.key = base64.b64encode(self.key_bytes)")
content = content.replace("from cryptography.fernet import Fernet", "import os")

with open("tests/storage/encryption_test.py", "w") as f:
    f.write(content)
