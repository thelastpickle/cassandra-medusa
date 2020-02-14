<!--
# Copyright 2019 Spotify AB. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
-->

[![Join the chat at https://gitter.im/thelastpickle/cassandra-medusa](https://badges.gitter.im/thelastpickle/cassandra-medusa.svg)](https://gitter.im/thelastpickle/cassandra-medusa?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

Medusa
======

Medusa is an Apache Cassandra backup system.

Features
--------
Medusa is a command line tool that offers the following features:

* Single node backup
* Single node restore
* Cluster wide in place restore (restoring on the same cluster that was used for the backup)
* Cluster wide remote restore (restoring on a different cluster than the one used for the backup)
* Backup purge
* Support for local storage, Google Cloud Storage (GCS) and AWS S3 through [Apache Libcloud](https://libcloud.apache.org/). Can be extended to support other storage providers supported by Apache Libcloud.
* Support for clusters using single tokens or vnodes
* Full or incremental backups


Setup
-----
Choose and initialize the storage system:

* Local storage can be used in conjunction with NFS mounts to store backups off nodes. The backup directory must be accessible from all nodes in the cluster and mounted appropriately. If the backup folder is not shared, the nodes will only see their own backups.
* [Google Cloud Storage setup](https://github.com/thelastpickle/cassandra-medusa/blob/master/docs/gcs_setup.md)
* [AWS S3 setup](https://github.com/thelastpickle/cassandra-medusa/blob/master/docs/aws_s3_setup.md)
* [Ceph Object Gateway S3 API](https://github.com/thelastpickle/cassandra-medusa/blob/master/docs/ceph_s3_setup.md)

Install Medusa on each Cassandra node:

### Online installation

* if the storage backend is a locally accessible shared storage, run `sudo pip3 install cassandra-medusa`
* if your backups are to be stored in AWS S3, run `sudo pip3 install cassandra-medusa[S3]`
* if your backups are to be stored in Google Cloud Storage, run `sudo pip3 install cassandra-medusa[GCS]`

Running the installation using `sudo` is necessary to have the `/usr/local/bin/medusa` script created properly.

### Offline installation

If your Cassandra servers do not have internet access:

- on a machine with the same target os and python version, clone the cassandra-medusa repo and cd into the root directory
- run `mkdir pip_dependencies && pip download -r requirements.txt -d medusa_dependencies` to download the dependencies into a sub directory
- run `cp requirements.txt medusa_dependencies/`
- run `tar -zcf medusa_dependencies.tar.gz medusa_dependencies` to compress the dependencies
- Upload the archive to all Cassandra nodes and decompress it
- run `pip install -r medusa_dependencies/requirements.txt --no-index --find-links` to install the dependencies on the nodes
- install Medusa using `python setup.py install` from the cassandra-medusa source directory

### Configure Medusa

Create the `/etc/medusa` directory if it doesn't exist, and create a file named `/etc/medusa/medusa.ini` with the content of [medusa-example.ini](https://github.com/thelastpickle/cassandra-medusa/blob/master/medusa-example.ini).
Modify it to match your requirements:

```
[cassandra]
;stop_cmd = /etc/init.d/cassandra stop
;start_cmd = /etc/init.d/cassandra start
;config_file = <path to cassandra.yaml. Defaults to /etc/cassandra/cassandra.yaml>
;cql_username = <username>
;cql_password = <password>
;nodetool_username =  <my nodetool username>
;nodetool_password =  <my nodetool password>
;nodetool_password_file_path = <path to nodetool password file>
;nodetool_host = <host name or IP to use for nodetool>
;nodetool_port = <port number to use for nodetool>

; Command ran to verify if Cassandra is running on a node. Defaults to "nodetool version"
;check_running = nodetool version

[storage]
storage_provider = <Storage system used for backups>
; storage_provider should be either of "local", "google_storage" or the s3_* values from
; https://github.com/apache/libcloud/blob/trunk/libcloud/storage/types.py

; Name of the bucket used for storing backups
bucket_name = cassandra_backups

; JSON key file for service account with access to GCS bucket or AWS credentials file (home-dir/.aws/credentials)
key_file = /etc/medusa/credentials

; Path of the local storage bucket (used only with 'local' storage provider)
;base_path = /path/to/backups

; Any prefix used for multitenancy in the same bucket
;prefix = clusterA

;fqdn = <enforce the name of the local node. Computed automatically if not provided.>

; Number of days before backups are purged. 0 means backups don't get purged by age (default)
max_backup_age = 0
; Number of backups to retain. Older backups will get purged beyond that number. 0 means backups don't get purged by count (default)
max_backup_count = 0
; Both thresholds can be defined for backup purge.

; Used to throttle S3 backups/restores:
transfer_max_bandwidth = 50MB/s

; Max number of downloads/uploads. Not used by the GCS backend.
concurrent_transfers = 1

; Size over which S3 uploads will be using the awscli with multi part uploads. Defaults to 100MB.
multi_part_upload_threshold = 104857600

; in the case awscli binaryis not located in the default python path  i.e venv supply the path to binary if the string 'dynamic' 
; is supplied medusa will attempt to the location awscli binary find by looping through directories 
;aws_cli_path=/path/to/awsclibinary/aws

[monitoring]
;monitoring_provider = <Provider used for sending metrics. Currently either of "ffwd" or "local">

[ssh]
;username = <SSH username to use for restoring clusters>
;key_file = <SSH key for use for restoring clusters. Expected in PEM unencrypted format.>
;port = <SSH port for use for restoring clusters. Default to port 22.

[checks]
;health_check = <Which ports to check when verifying a node restored properly. Options are 'cql' (default), 'thrift', 'all'.>
;query = <CQL query to run after a restore to verify it went OK>
;expected_rows = <Number of rows expected to be returned when the query runs. Not checked if not specified.>
;expected_result = <Coma separated string representation of values returned by the query. Checks only 1st row returned, and only if specified>

[logging]
; Controls file logging, disabled by default.
; enabled = 0
; file = medusa.log
; level = INFO

; Control the log output format
; format = [%(asctime)s] %(levelname)s: %(message)s

; Size over which log file will rotate
; maxBytes = 20000000

; How many log files to keep
; backupCount = 50

```


Usage
=====

```
$ medusa
Usage: medusa [OPTIONS] COMMAND [ARGS]...

Options:
  -v, --verbosity          Verbosity
  --without-log-timestamp  Do not show timestamp in logs
  --config-file TEXT       Specify config file
  --bucket-name TEXT       Bucket name
  --key-file TEXT          GCP credentials key file
  --prefix TEXT            Prefix for shared storage
  --fqdn TEXT              Act as another host
  --ssh-username TEXT
  --ssh-key-file TEXT
  --help                   Show this message and exit.

Commands:
  backup                          Backup Cassandra
  build-index                     Builds indices for all present backups
                                  and...
  download                        Download backup
  fetch-tokenmap                  Backup Cassandra
  get-last-complete-cluster-backup
                                  Pints the name of the latest complete
                                  cluster...
  list-backups                    List backups
  purge                           Delete obsolete backups
  report-last-backup              Find time since last backup and print it
                                  to...
  restore-cluster                 Restore Cassandra cluster
  restore-node                    Restore single Cassandra node
  status                          Show status of backups
  verify                          Verify the integrity of a backup
```


Performing a backup
-------------------

```
$ medusa backup --help
Usage: medusa backup [OPTIONS]

  Backup Cassandra

Options:
  --backup-name TEXT           Custom name for the backup
  --stagger INTEGER            Check for staggering initial backups for
                               duration seconds
  --mode [full|differential]
  --help                       Show this message and exit.
```

Once Medusa is setup, you can create a **full** backup with the following command:

```
$ medusa backup --backup-name=<name of the backup>
```

In order to perform an **differential** backup, add the `--mode=differential` argument to your command:

```
$ medusa backup --backup-name=<name of the backup> --mode=differential
```

To perform cluster wide backups, the command must run on all nodes in the cluster, using the same backup name.


Listing existing backups
------------------------

```
$ medusa list-backups --help
Usage: medusa list-backups [OPTIONS]

  List backups

Options:
  --show-all / --no-show-all  List all backups in the bucket
  --help                      Show this message and exit.
```

List all backups for the current node/cluster:

```
$ medusa list-backups
2019080507 (started: 2019-08-05 07:07:03, finished: 2019-08-05 08:01:04)
2019080607 (started: 2019-08-06 07:07:04, finished: 2019-08-06 07:59:08)
2019080707 (started: 2019-08-07 07:07:04, finished: 2019-08-07 07:59:55)
2019080807 (started: 2019-08-08 07:07:03, finished: 2019-08-08 07:59:22)
2019080907 (started: 2019-08-09 07:07:04, finished: 2019-08-09 08:00:14)
2019081007 (started: 2019-08-10 07:07:04, finished: 2019-08-10 08:02:41)
2019081107 (started: 2019-08-11 07:07:04, finished: 2019-08-11 08:03:48)
2019081207 (started: 2019-08-12 07:07:04, finished: 2019-08-12 07:59:59)
2019081307 (started: 2019-08-13 07:07:03, finished: Incomplete [179 of 180 nodes])
2019081407 (started: 2019-08-14 07:07:04, finished: 2019-08-14 07:56:44)
2019081507 (started: 2019-08-15 07:07:03, finished: 2019-08-15 07:50:24)
```

When listing backups from a cluster which is not the backed up one, please add the `--show-all` flag to bypass the filter and display all existing backups from storage.


Restoring a single node
-----------------------

```
$ medusa restore-node --help
Usage: medusa restore-node [OPTIONS]

  Restore single Cassandra node

Options:
  --temp-dir TEXT         Directory for temporary storage
  --backup-name TEXT      Backup name  [required]
  --in-place              Indicates if the restore happens on the node the
                          backup was done on.
  --keep-auth             Keep system_auth keyspace as found on the node
  --seeds TEXT            Nodes to wait for after downloading backup but
                          before starting C*
  --verify / --no-verify  Verify that the cluster is operational after the
                          restore completes,
  --keyspace TEXT         Restore tables from this keyspace
  --table TEXT            Restore only this table
  --use-sstableloader     Use the sstableloader to load the backup into the
                          cluster
  --help                  Show this message and exit.
```

In order to restore a backup on a single node, run the following command:

```
$ sudo medusa restore-node --backup-name=<name of the backup>
```

Medusa will need to run with `sudo` as it will:

* stop Cassandra
* wipe the existing files
* Download the files from backup storage locally and move them to Cassandra's storage directory
* Change the ownership of the files back to the one owning the Cassandra data directory
* start Cassandra

The `--use-sstableloader` flag will be useful for restoring data when the topology doesn't match between the backed up cluster and the restore one.
In this mode, Cassandra will not be stopped and downloaded SSTables will be loaded into Cassandra by the sstableloader. Data already present in the cluster will not be altered.

The `--fqdn` argument allows to force the node to act on behalf of another backup node. It can take several hostnames separated by commas in order to restore several nodes backup using the sstableloader.

The `--keyspace` option allows limiting the restore to all tables in the given keyspace. The `--table` option allows limiting the restore to just one table. The tables must be specified in the `keyspace.table` format. It is possible to repeat both of the options. Medusa will make an union of everything specified and restore all keyspaces and tables mentioned. The `--keyspace` option takes precedence, so using `--keyspace ks1` and then adding `--table ks1.t` will not limit the restore to just one table - everything from `ks1` will be restored.

Restoring a cluster
-------------------

```
$ medusa restore-cluster --help
Usage: medusa restore-cluster [OPTIONS]

  Restore Cassandra cluster

Options:
  --backup-name TEXT              Backup name  [required]
  --seed-target TEXT              seed of the target hosts
  --temp-dir TEXT                 Directory for temporary storage
  --host-list TEXT                List of nodes to restore with the associated
                                  target host
  --keep-auth / --overwrite-auth  Keep/overwrite system_auth as found on the
                                  nodes
  -y, --bypass-checks             Bypasses the security check for restoring a
                                  cluster
  --verify / --no-verify          Verify that the cluster is operational after
                                  the restore completes,
  --keyspace TEXT                 Restore tables from this keyspace
  --table TEXT                    Restore only this table
  --use-sstableloader             Use the sstableloader to load the backup
                                  into the cluster
  --help                          Show this message and exit.
```

## Topology matches between the backup and the restore cluster
In this section, we will describe procedures that apply to the following cases:

* Vnodes are not used (single token) + both the backup and the restore cluster have the exact same number of nodes and token assignments.
* Vnodes are used + both the backup and the restore cluster have the exact same number of nodes (regardless token assignments).

This method is by far the fastest as it replaces SSTables directly on disk.

### In place (same hardware)

In order to restore a backup for a full cluster, in the case where the restored cluster is the exact same as the backed up one:

```
$ medusa restore-cluster --backup-name=<name of the backup> --seed-target node1.domain.net
```

Medusa will need to run without `sudo` as it will connect through ssh to all nodes in the cluster in order to perform remote operations. It will, by default, use the current user to connect and rely on agent forwarding for authentication (you must ssh into the server using `-A` to enable agent forwarding).
The `--seed-target` node is used to connect to Cassandra and retrieve the current topology of the cluster. This allows Medusa to map each backup to the correct node in the current topology.

The following operations will take place:

* Stop Cassandra on all nodes
* Check that the current topology matches the backed up one (if not, will fallback to the next section, using the sstableloader)
* Run `restore-node` on each node in the cluster
* Start Cassandra on all nodes


### Remotely (different hardware)

In order to restore a backup of a full cluster but on different servers.
This can be used to restore a production cluster data to a staging cluster (with the same number of nodes), or recovering from an outage where previously used hardware cannot be re-used.

```
$ medusa restore-cluster --backup-name=<name of the backup> --host-list /etc/medusa/restore_mapping.txt
```

The `restore-mapping.txt` file will provide the mapping between the backed up cluster nodes and the restore cluster ones. It is expected in the following CSV format: `<Is it a seed node?>,<target node>,<source node>`

Sample file:

```
True,new_node1.foo.net,old_node1.foo.net
True,new_node2.foo.net,old_node2.foo.net
False,new_node3.foo.net,old_node3.foo.net
```

Medusa will need to run without `sudo` as it will connect through ssh to all nodes in the cluster in order to perform remote operations. It will, by default, use the current user to connect and rely on agent forwarding for authentication (you must ssh into the server using `-A` to enable agent forwarding).

* Stop Cassandra on all nodes
* Check that the current topology matches the backed up one (if not, will fallback to the next section, using the sstableloader)
* Run `restore-node` on each node in the cluster
* Start Cassandra on all nodes

By default, Medusa will overwrite the `system_auth` keyspace with the backed up one. If you want to retain the existing system_auth keyspace, you'll have to run `restore-cluster` with the `--keep-auth` flag:

```
$ medusa restore-cluster --backup-name=<name of the backup> --host-list /etc/medusa/restore_mapping.txt --keep-auth
```

## Topology does not match between the backup and the restore cluster
In this section, we will describe procedures that apply to the other cases:

* Vnodes are not used (single token) + the backup and restore cluster have a different token assignement or a different number of nodes.
* Vnodes are used + the backup and the restore cluster have a different number of nodes.


This case will be detected automatically by Medusa when checking the topologies, but it can be enforced by adding the `--use-sstableloader` flag to the `restore-cluster` command.
This technique allows to restore any backup on any cluster of any size, **at the expense of some overhead.**

* First, the sstableloader will have to parse all the backed up SSTables in order to send them to the appropriate nodes, which can take way more time on large volumes.
* Then, the amount of data loaded into the restore cluster will be multiplied by the replication factor of the keyspace, since we will be restoring the backups from all replicas without any merge (SSTables from different backups will contain copies of the same data).
**With RF=3, the cluster will contain approximately three times the data load from the backup.** The size will drop back to normal levels once compaction has caught up (a major compaction could be necessary).

Using this technique, Cassandra will not be stopped on the nodes and the following steps will happen:

* The data model will be updated as follows:
	* The schema will be downloaded from the backup and categorized by object types into individual queries
	* Missing keyspaces will be created
	* Existing Materialized Views from the backup schema will be dropped (other MVs remain untouched)
	* Existing tables from the backup schema will be dropped (other tables remain untouched)
	* Existing User Defined Types from the backup schema will be (re)created
	* Tables from the backup will be created
	* Secondary indexes will be created
	* Materialized Views will be created
* Backup nodes will be assigned to target nodes (one target node can be responsible from zero to several backup nodes)
* Run `restore-node` on each node in the target cluster, passing a list of backup nodes as `--fqdn` and the `--sstableloader` flag
	* for each specified node in `--fqdn`:
		* Download the files from backup storage locally
		* Invoke the locally installed sstableloader to load them using the local C* instance as contact point

If your cluster is configured with the default `auto_snapshot: true` then dropping the tables will trigger a snapshot that will persist their data on disk. Medusa will not clear this snapshot after restore.

The `--keyspace` and `--table` options of `restore-cluster` command work exactly the same way as they do for the `restore-node` command.


Verify an existing backup
-------------------------

```
$ medusa verify --help
Usage: medusa verify [OPTIONS]

  Verify the integrity of a backup

Options:
  --backup-name TEXT  Backup name  [required]
  --help              Show this message and exit.
```

Run a health check on a backup, which will verify that:

* All nodes have completed the backup
* All files in the manifest are present in storage
* All backed up files are present in the manifest
* All files have the right hash as stored in the manifest

```
$ medusa verify --backup-name=2019090503
Validating 2019090503 ...
- Completion: OK!
- Manifest validated: OK!!
```

In case some nodes in the cluster didn't complete the backups, you'll get the following output:

```
$ medusa verify --backup-name=2019081703
Validating 2019081703 ...
- Completion: Not complete!
  - [127.0.0.2] Backup missing
- Manifest validated: OK!!
```

Purge old backups
-----------------

```
$ medusa purge --help
Usage: medusa purge [OPTIONS]

  Delete obsolete backups

Options:
  --help  Show this message and exit.
```

In order to remove obsolete backups from storage, according to the configured `max_backup_age` and/or `max_backup_count`, run:

```
$ medusa purge
[2019-09-04 13:44:16] INFO: Starting purge
[2019-09-04 13:44:17] INFO: 25 backups are candidate to be purged
[2019-09-04 13:44:17] INFO: Purging backup 2019082513...
[2019-09-04 13:44:17] INFO: Purging backup 2019082514...
[2019-09-04 13:44:18] INFO: Purging backup 2019082515...
[2019-09-04 13:44:18] INFO: Purging backup 2019082516...
[2019-09-04 13:44:19] INFO: Purging backup 2019082517...
[2019-09-04 13:44:19] INFO: Purging backup 2019082518...
[2019-09-04 13:44:19] INFO: Purging backup 2019082519...
[2019-09-04 13:44:20] INFO: Purging backup 2019082520...
[2019-09-04 13:44:20] INFO: Purging backup 2019082521...
[2019-09-04 13:44:20] INFO: Purging backup 2019082522...
[2019-09-04 13:44:21] INFO: Purging backup 2019082523...
[2019-09-04 13:44:21] INFO: Purging backup 2019082600...
[2019-09-04 13:44:21] INFO: Purging backup 2019082601...
[2019-09-04 13:44:22] INFO: Purging backup 2019082602...
[2019-09-04 13:44:22] INFO: Purging backup 2019082603...
[2019-09-04 13:44:23] INFO: Purging backup 2019082604...
[2019-09-04 13:44:23] INFO: Purging backup 2019082605...
[2019-09-04 13:44:23] INFO: Purging backup 2019082606...
[2019-09-04 13:44:24] INFO: Purging backup 2019082607...
[2019-09-04 13:44:24] INFO: Purging backup 2019082608...
[2019-09-04 13:44:24] INFO: Purging backup 2019082609...
[2019-09-04 13:44:25] INFO: Purging backup 2019082610...
[2019-09-04 13:44:25] INFO: Purging backup 2019082611...
[2019-09-04 13:44:25] INFO: Purging backup 2019082612...
[2019-09-04 13:44:26] INFO: Purging backup 2019082613...
[2019-09-04 13:44:26] INFO: Cleaning up orphaned files...
[2019-09-04 13:45:59] INFO: Purged 652 objects with a total size of 3.74 MB

```

Since SSTables and meta files are stored in different places for differential backups, the purge is a two step process:

* Delete all backup directories
* Scan active backup files from manifests and compare with the list of SSTables in the `data` directory. All SSTables present in the `data` directory but absent from all manifests will get deleted in that step.


Check the status of a backup
----------------------------

```
$ medusa status --help
Usage: medusa status [OPTIONS]

  Show status of backups

Options:
  --backup-name TEXT  Backup name  [required]
  --help              Show this message and exit.
```

Outputs a summary of a specific backup status:

```
$ medusa status --backup-name=2019090503
2019090503
- Started: 2019-09-05 03:53:04, Finished: 2019-09-05 04:49:52
- 32 nodes completed, 0 nodes incomplete, 0 nodes missing
- 163256 files, 12.20 TB
```


Display informations on the latest backup
-----------------------------------------
```
$ medusa report-last-backup --help
Usage: medusa report-last-backup [OPTIONS]

  Find time since last backup and print it to stdout :return:

Options:
  --push-metrics  Also push the information via metrics
  --help          Show this message and exit.

```

This command will display several informations on the latest backup:

```
$ medusa report-last-backup
[2019-09-04 12:56:15] INFO: Latest node backup finished 18746 seconds ago
[2019-09-04 12:56:18] INFO: Latest complete backup:
[2019-09-04 12:56:18] INFO: - Name: 2019090407
[2019-09-04 12:56:18] INFO: - Finished: 18173 seconds ago
[2019-09-04 12:56:19] INFO: Latest backup:
[2019-09-04 12:56:19] INFO: - Name: 2019090407
[2019-09-04 12:56:19] INFO: - Finished: True
[2019-09-04 12:56:19] INFO: - Details - Node counts
[2019-09-04 12:56:19] INFO: - Complete backup: 180 nodes have completed the backup
[2019-09-04 12:58:47] INFO: - Total size: 94.69 TiB
[2019-09-04 12:58:55] INFO: - Total files: 5168096
```

When used with `--push-metrics`, Medusa will push completion metrics to the configured monitoring system.
