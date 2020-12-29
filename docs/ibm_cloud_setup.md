IBM Cloud Object Storage setup
==============================

### Create a bucket

Create a new object storage bucket that will be used to store the backups using the following aws cli command:

```
aws --profile default --endpoint-url https://s3.us.cloud-object-storage.appdomain.cloud s3api create-bucket --bucket <bucket name> --create-bucket-configuration LocationConstraint=us-smart
```

The endpoint URLs and their corresponding location constraints can be found [here](https://github.com/thelastpickle/cassandra-medusa/blob/master/medusa/libcloud/storage/drivers/ibm.py#L27-L123). Medusa was tested against `smart` storage geo replicated buckets only.

### Create a service credential

Create a service credential for your object storage with `Writer` role, following the steps described on [this IBM Cloud documentation](https://cloud.ibm.com/docs/cloud-object-storage/iam?topic=cloud-object-storage-service-credentials).

Copy the JSON representation of your service credential to a temporary file. It should look like the following:


```
{
  "apikey": "vH3YokspokfDSFERZI-fFkCiUcc4aP9aagmCKz19QH4J9",
  "cos_hmac_keys": {
    "access_key_id": "0ac583530e3c431754534bfa379d43c6",
    "secret_access_key": "23dcaba061fcc067765c99d8e2823346c26cc2d8ef12c5f84c"
  },
  "endpoints": "https://control.cloud-object-storage.cloud.ibm.com/v2/endpoints",
  "iam_apikey_description": "Auto-generated for key 0fb58179-5z7h-9268-2936-9bfa379d43c6",
  "iam_apikey_name": "AccessKey",
  "iam_role_crn": "crn:v1:bluemix:public:iam::::serviceRole:Writer",
  "iam_serviceid_crn": "crn:v1:bluemix:public:iam-identity::a/5f44ddf16d936e47b52f7a2844d344c4::serviceid:ServiceId-037ef33d-e3b3-7374-91f5-5b8400f73cd2",
  "resource_instance_id": "crn:v1:bluemix:public:cloud-object-storage:global:a/5f44ddf59d926e92b52f7a78646d344c4:50f6904d-978e-450f-b117-e6ca42347ca3::"
}
```

### Configure Medusa

Copy the access key and the associated secret from the above file (under `cos_hmac_keys`) and save them in a file called `medusa-ibm-credentials` in the following format:

```
[default]
aws_access_key_id = <access_key_id>
aws_secret_access_key = <secret_access_key>
```

Place this file on all Cassandra nodes running medusa under `/etc/medusa` and set the rights appropriately so that only users running Medusa can read/modify it.
Set the `key_file` value in the `[storage]` section of `/etc/medusa/medusa.ini` to the credentials file:  

```
bucket_name = <bucket name>
key_file = /etc/medusa/medusa-ibm-credentials
region = us-smart
```

Note: adjust the region in the `medusa.ini` file to match your bucket's location constraint.

Medusa should now be able to access the bucket and perform all required operations.
