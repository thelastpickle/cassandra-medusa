Ceph Object Gateway S3 Setup
=============================

### Create an bucket

Create a new bucket that will be used to store the backups.

### Configure Medusa

Create an user for the backups and generate access keys for that user and save them in a file called `ceph-credentials` in the following format:

```
{
    "access_key_id": "accesskeyid",
    "secret_access_key": "secretkey"
}

```

Place this file on all Cassandra nodes running medusa under `/etc/medusa` and set the rights appropriately so that only users running Medusa can read/modify it.
Set the `key_file` value and `host` and `port` of your RGW in the `[storage]` section of `/etc/medusa/medusa.ini` to the credentials file:

```
[storage]
storage_provider = s3_rgw
bucket_name = my_bucket
key_file = /etc/medusa/ceph-credentials
host = rgw_host
port = rgw_port
secure = false
```

Medusa should now be able to access the bucket and perform all required operations.
