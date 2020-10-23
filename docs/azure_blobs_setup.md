Azure Blob Storage setup
========================

### Storage account

Create a new storage account or use an existing one which will be used to store the backups. Ideally, do not enable public access. Under the `Settings`, find `Access keys`. Note the `storageaccountname` and `Key`. Create a file called `medusa-azure-credentials` in the following format:

```json
{
    "storage_account": "YOUR_STORAGE_ACCOUNT_NAME",
    "key": "YOUR_KEY"
}
```
Place this file on all Cassandra nodes running medusa under `/etc/medusa/`and set the rigths appropriately so that onyl users running Medusa can read/modify it.

### Create a container

Create a new container in your storage account that will be used to store the backups and do not enable public access.

### Configuring Medusa

Set the `key_file` value in the `[storage]` section of `/etc/medusa/medusa.ini` to the credentials file and set the bucket name as shown below:

```
bucket_name = your_container_name
key_file = /etc/medusa/medusa-azure-credentials
```

Medusa should now be able to access the bucket and perform all required operations.

