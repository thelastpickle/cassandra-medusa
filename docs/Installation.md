Installation
------------
Choose and initialize the storage system:

* Local storage can be used in conjunction with NFS mounts to store backups off nodes. The backup directory must be accessible from all nodes in the cluster and mounted appropriately. If the backup folder is not shared, the nodes will only see their own backups.
* [Google Cloud Storage setup](/docs/gcs_setup.md)
* [AWS S3 setup](/docs/aws_s3_setup.md)
* [Ceph Object Gateway S3 API](/docs/ceph_s3_setup.md)
* [Azure Blob Storage setup](/docs/azure_blobs_setup.md)
* [IBM Cloud Object Storage setup](/docs/ibm_cloud_setup.md)
* [MinIO/S3 compatible Storage setup](/docs/minio_setup.md)

Install Medusa on each Cassandra node using one of the following methods.

## Using pip
### Online installation

* if the storage backend is a locally accessible shared storage, run `sudo pip3 install cassandra-medusa`
* if your backups are to be stored in AWS S3 or S3 compatible backends (IBM, OVHCloud, MinIO, ...), run `sudo pip3 install cassandra-medusa[S3]`
* if your backups are to be stored in Google Cloud Storage, run `sudo pip3 install cassandra-medusa[GCS]`
* if your backups are to be stored in Azure Blob Storage, run `sudo pip3 install cassandra-medusa[AZURE]`

Running the installation using `sudo` is necessary to have the `/usr/local/bin/medusa` script created properly.

### Offline installation

If your Cassandra servers do not have internet access:  

- on a machine with the same target os and python version, clone the cassandra-medusa repo and cd into the root directory
- run `mkdir pip_dependencies && pip download -r requirements.txt -d medusa_dependencies` to download the dependencies into a sub directory (do the same thing with either requirements-s3.txt or requirements-gcs.txt depending on your storage)
- run `cp requirements.txt medusa_dependencies/` (plus either requirements-s3.txt or requirements-gcs.txt)
- run `tar -zcf medusa_dependencies.tar.gz medusa_dependencies` to compress the dependencies
- Upload the archive to all Cassandra nodes and decompress it
- run `pip install -r medusa_dependencies/requirements.txt --no-index --find-links` to install the dependencies on the nodes (do the same thing with either requirements-s3.txt or requirements-gcs.txt depending on your storage)
- install Medusa using `python setup.py install` from the cassandra-medusa source directory

#### Example of Offline installation using pipenv on RHEL, centos 7

- install RPM pre-requisites `sudo yum install -y python3-pip python3-devel`
- install pipenv `sudo pip3 install pipenv`
- unpack your archive built using the procedure from previous section `tar zxvf my-archive-of-cassandra-medusa.tar.gz`
- create your python env in the directory previously created `cd cassandra-medusa-0.7.1 && pipenv --python 3`
- install python dependencies of medusa `pipenv run pip3 install -r requirements.txt --no-index --find-links medusa_dependencies/`
- prepare an installation directory with appropriate privileges `sudo mkdir /opt/cassandra-medusa ; sudo chmod go+rwX  /opt/cassandra-medusa`
- install medusa as non root user `pipenv run python3 setup.py install --prefix=. --root=/opt/cassandra-medusa`

## Debian packages
### Using apt-get
1/ Using the command line, run the following:

```
curl -1sLf \
  'https://dl.cloudsmith.io/public/thelastpickle/medusa/setup.deb.sh' \
  | sudo -E bash
```

In case of problem, read the full instructions on the [cloudsmith.io documentation](https://cloudsmith.io/~thelastpickle/repos/medusa/setup/#formats-deb)

*Note: since Medusa 0.9 we publish releases for Ubuntu `bionic` and `focal` only.*

2/ Install Medusa :

```
sudo apt-get update
sudo apt-get install cassandra-medusa
```

4/ (optional) Install the storage dependencies

* if your backups are to be stored in AWS S3 and all S3 compatible backends, run `sudo apt-get install awscli`
* if your backups are to be stored in Azure Blob Storage, run `sudo apt-get install azure-cli`
* if your backups are to be stored in Google Cloud Storage, [follow this quickstart guide from Google](https://cloud.google.com/sdk/docs/quickstart-debian-ubuntu).
