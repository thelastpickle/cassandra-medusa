from ecies import encrypt, decrypt

import logging
import medusa.utils

ENCRYPTED_FILE_PATH_SUFFIX = ".ecdh_enc"
def encrypt(pk: str, data: bytes) -> bytes:
    """Encrypt data with public key."""
    return encrypt(pk, data)


def decrypt(sk: str, data: bytes) -> bytes:
    """Decrypt data with private key."""
    return decrypt(sk, data)


def get_enc_file_path(file_path: str) -> str:
    return file_path + ENCRYPTED_FILE_PATH_SUFFIX
def encrypt_file(pk: str, file_path: str) -> str:
    """Encrypt file with public key."""
    logging.debug("encrypting file: {}".format(file_path))
    with open(file_path, "rb") as f:
        data = f.read()
    f.close()

    new_file_path = get_enc_file_path(file_path)
    with open(new_file_path, "wb") as f:
        f.write(encrypt(pk, data))
    f.close()

    return new_file_path


def decrypt_file(sk: str, file_path: str) -> None:
    """Decrypt file with private key."""
    logging.debug("decrypting file: {}".format(file_path))
    with open(file_path, "rb") as f:
        data = f.read()
    f.close()

    with open(file_path, "wb") as f:
        f.write(decrypt(sk, data))
    f.close()

def decrypt_dir(sk: str, path: str, batch_size: str = None):
    """
    process all files of a directory
    """
    files = medusa.utils.find_all(path)
    if batch_size is None:
        batch_size = len(files)

    encryptors = []
    for f in files:
        encryptors.append(lambda: decrypt_file(sk, f))

    medusa.utils.batch_executor(encryptors)