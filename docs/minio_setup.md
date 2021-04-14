MinIO Storage setup (and other S3 compatible backends)
======================================================

### Create a bucket

Create a new storage bucket in MinIO that will be used to store the backups.

### Configure Medusa

Copy your MinIO access credentials in a file called `medusa-minio-credentials` using the following format:

```
[default]
aws_access_key_id = <access_key_id>
aws_secret_access_key = <secret_access_key>
```

Place this file on all Apache Cassandra™ nodes running medusa under `/etc/medusa` and set the rights appropriately so that only users running Medusa can read/modify it.
Set the `key_file` value in the `[storage]` section of `/etc/medusa/medusa.ini` to the credentials file:  

```
[storage]
storage_provider = s3_compatible
bucket_name = <bucket name>
key_file = /etc/medusa/medusa-minio-credentials
host = <minio host>
port = <minio API port, default is 9000>
secure = False
```

Medusa should now be able to access the bucket and perform all required operations.
*Note: MinIO and other self hosted S3 compatible storage systems can only be used in unsecured (non SSL) mode with Medusa due to limitations in Apache Libcloud. Cloud hosted S3 compatible backends (such as IBM) should be able/require to use secured access.*
