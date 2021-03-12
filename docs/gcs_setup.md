Google Cloud Storage setup
==========================

### Create a role for backups

In order to perform backups in GCS, Medusa will need to use a service account [with appropriate permissions](permissions-setup.md).

You will need the following variables to be set for this setup:

```
GCP_PROJECT=my-project                 # GCP project the bucket belongs to
LOCATION=my-location                   # for example: us-west1
BUCKET_URL=gs://my-bucket
SERVICE_ACCOUNT_NAME=my-sa-for-medusa  # Without "@my-project.iam.gserviceaccount.com"  
```

Using the [Google Cloud SDK](https://cloud.google.com/sdk/install), run the following command to create the `MedusaStorageRole`.  

```
gcloud iam roles create MedusaStorageRole \
        --project ${GCP_PROJECT} \
        --stage GA \
        --title MedusaStorageRole \
        --description "Custom role for Medusa for accessing GCS safely" \
        --permissions storage.buckets.get,storage.buckets.getIamPolicy,storage.objects.create,storage.objects.delete,storage.objects.get,storage.objects.getIamPolicy,storage.objects.list
```

### Create a GCS bucket

Create a bucket for each Apache Cassandra™ cluster, using the following command line:

```
gsutil mb -p ${GCP_PROJECT} -c regional -l ${LOCATION} ${BUCKET_URL}
```

### Create a service account and download its keys

Medusa will require a `credentials.json` file with the information and keys for a service account with the appropriate role in order to interact with the bucket.

Create the service account (if it doesn't exist yet):

```
gcloud --project ${GCP_PROJECT} iam service-accounts create ${SERVICE_ACCOUNT_NAME} --display-name ${SERVICE_ACCOUNT_NAME}
```

### Configure the service account with the role

Once the service account has been created, and considering [jq](https://stedolan.github.io/jq/) is installed, run the following command to add the `MedusaStorageRole` to it, for our backup bucket:

```
iamGetFile=$(mktemp) && \
gsutil iam get ${BUCKET_URL} | jq ".bindings += [{\"members\":[\"serviceAccount:${SERVICE_ACCOUNT_NAME}@${GCP_PROJECT}.iam.gserviceaccount.com\"],\"role\":\"projects/${GCP_PROJECT}/roles/MedusaStorageRole\"}]" > "${iamGetFile}" && \
gsutil iam set ${iamGetFile} ${BUCKET_URL} && \
rm -rf ${iamGetFile}
```

### Configure Medusa

Generate a json key file called `credentials.json`, for the service account:

```
gcloud --project ${GCP_PROJECT} iam service-accounts keys create credentials.json --iam-account=${SERVICE_ACCOUNT_NAME}@${GCP_PROJECT}.iam.gserviceaccount.com
```

Place this file on all Cassandra nodes running medusa under `/etc/medusa` and set the rights appropriately so that only users running Medusa can read/modify it.
Set the `key_file` value in the `[storage]` section of `/etc/medusa/medusa.ini` to the credentials file:  

```
[storage]
storage_provider = google_storage
bucket_name = my_gcs_bucket
key_file = /etc/medusa/credentials.json
```

Medusa should now be able to access the bucket and perform all required operations.
