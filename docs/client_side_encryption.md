# Client-Side Encryption

## Overview

Medusa supports client-side encryption (CSE) to encrypt backup files before uploading them to cloud storage.
This provides an additional layer of security, ensuring that data is encrypted in transit and at rest, independent of server-side encryption capabilities.

**Important**: Encrypted and unencrypted backups are **not compatible** in differential backup chains.

## How It Works

When client-side encryption is enabled:

1. **During Backup**:
   - SSTable files are encrypted locally using Fernet symmetric encryption
   - Each file is processed in 1MB chunks to manage memory usage
   - Encrypted files are uploaded to cloud storage
   - Metadata files (`manifest.json`, `schema.cql`, etc.) remain unencrypted for compatibility

2. **During Restore**:
   - Encrypted files are downloaded from cloud storage
   - Files are decrypted locally before being restored to Cassandra
   - Metadata files are copied directly without decryption

3. **Differential Backups**:
   - The manifest stores both encrypted and original file metadata (`source_MD5`, `source_size`)
   - This allows comparison with local files without decrypting remote files
   - Reduces unnecessary uploads and improves backup efficiency

## Configuration

### Encryption Key Generation

Generate a Fernet-compatible 32-byte key:

```bash
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

This will output a base64-encoded key like:
```
DrMxa6NEhBuKcBqffvw675eHo/9J/W3WqXZ3spyI1/U=
```

### medusa.ini Configuration

Add the following parameters to the `[storage]` section:

```ini
[storage]
# ... other storage configuration ...

# Base64-encoded 32-byte encryption key (required for CSE)
key_secret_base64 = DrMxa6NEhBuKcBqffvw675eHo/9J/W3WqXZ3spyI1/U=

# Temporary directory for encryption/decryption operations (optional)
# Defaults to system temp directory if not specified
# Directory must have sufficient space for concurrent file operations
encryption_tmp_dir = /tmp
```

## Security Best Practices

The configuration file containing the encryption key must be protected with restricted file permissions:

```bash
chmod 0600 /path/to/medusa.ini
```

Ensure that:
- Only the user running Medusa has read/write access to the configuration file
- Group and other users have no access to the file
- The configuration file is owned by the appropriate user/service account


The encryption key is required to decrypt all encrypted backups. Without it, **data cannot be recovered**.
So backup the key and test recovery procedures to verify you can restore encrypted backups

## Usage

### Creating Encrypted Backups

Once configured, backups are automatically encrypted:

```bash
medusa backup --backup-name my-encrypted-backup
```

Files uploaded to storage will be encrypted.
The manifest will include:
- `MD5` and `size`: Hash and size of the **encrypted** file
- `source_MD5` and `source_size`: Hash and size of the **original** file (before encryption)

### Restoring Encrypted Backups

Restoration works transparently:

```bash
medusa restore --backup-name my-encrypted-backup
```

### Verifying Encrypted Backups

```bash
medusa verify --backup-name my-encrypted-backup
```

Verification checks both encrypted file integrity and manifest consistency.

## What is Encrypted

**Encrypted**:
- SSTable data files (`*.db`, `*.txt`, etc.)
- Index files (secondary indexes in `.index_name/` directories)
- All user data files

**Not Encrypted** (stored as plaintext):
- `manifest*.json`
- `schema.cql`
- `tokenmap.json`
- `server_version.json`
- `backup_name.txt`

These metadata files must be accessible without decryption for backup discovery and validation.

## Performance Considerations

### Resource Usage

- **CPU**: Encryption/decryption adds CPU overhead. Impact depends on backup size and concurrent transfers.
- **Disk**: Temporary encrypted files are stored in `encryption_tmp_dir` during upload/download.
  - Ensure sufficient disk space (at least `concurrent_transfers * largest_file_size`)
- **Memory**: Processing is chunked (1MB) to limit memory usage.

### Optimization

- Adjust `concurrent_transfers` in `medusa.ini` to balance throughput and resource usage
- Use dedicated `encryption_tmp_dir` on fast storage (SSD) for better performance

