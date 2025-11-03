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
Place this file on all Apache Cassandra™ nodes running Medusa under `/etc/medusa/`and set the rights appropriately so that only users running Medusa can read/modify it.


### Identity through roles

If Medusa runs in an environment with a managed identity set up, then there is no need to create the key file, nor to put the `key_file` field in the `medusa.ini` file.

You can check if the managed identity works by running:

```
az login --identity
az storage blob list  --account-name <account_name> --container-name <container_name> --auth-mode login --output table
```

  * The `--account-name` is the name of the Azure account where the storage is located.
  * The `--container-name` is the name of the container where the blobs are store. This is equivalent to the _bucket name_ in other storage providers.

To have Medusa use the managed identity, you need to set the `AZURE_STORAGE_ACCOUNT` environment variable before you run `medusa`:

```
export AZURE_STORAGE_ACCOUNT=<account_name>
```

In the `medusa.ini` file, set only the `bucket_name` to your container name. You should see log lines like these if everything is working as expected:

```
[2025-03-24 15:47:22,191] INFO: ManagedIdentityCredential will use IMDS
[2025-03-24 15:47:22,215] INFO: DefaultAzureCredential acquired a token from ManagedIdentityCredential
```

### Create a container

Create a new container in your storage account that will be used to store the backups and do not enable public access.

### Configuring Medusa

Set the `key_file` value in the `[storage]` section of `/etc/medusa/medusa.ini` to the credentials file and set the bucket name as shown below:

```
bucket_name = your_container_name
key_file = /etc/medusa/medusa-azure-credentials
```

Medusa should now be able to access the bucket and perform all required operations.

If you need to set a different host for Azure (for example the host for Azure Gov is `<storageAccount>.blob.core.usgovcloudapi.net`), please use the `host` parameter in the `[storage]` section of `/etc/medusa/medusa.ini`:

```
"host": "usgovcloudapi.net"
```
