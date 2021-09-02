Usage
=====

```
$ medusa
Usage: medusa [OPTIONS] COMMAND [ARGS]...

Options:
  -v, --verbosity                 Verbosity  [x>=0]
  --without-log-timestamp         Do not show timestamp in logs
  --config-file TEXT              Specify config file
  --bucket-name TEXT              Bucket name
  --key-file TEXT                 GCP credentials key file
  --prefix TEXT                   Prefix for shared storage
  --fqdn TEXT                     Act as another host
  --backup-grace-period-in-days TEXT
                                  Duration for which backup files cannot be
                                  deleted from storage
  --ssh-username TEXT
  --ssh-key-file TEXT
  --version                       Show the version and exit.
  --help                          Show this message and exit.

Commands:
  backup (backup,backup-node)     Backup single Cassandra node
  backup-cluster                  Backup Cassandra cluster
  build-index                     Build indices for all present backups...
  delete-backup                   Delete the given backup on the...
  download                        Download backup
  fetch-tokenmap                  Get the token/node mapping for a...
  get-last-complete-cluster-backup
                                  Print the name of the latest complete...
  list-backups                    List backups
  purge                           Delete obsolete backups
  report-last-backup              Find time since last backup and print...
  restore-cluster                 Restore Cassandra cluster
  restore-node                    Restore single Cassandra node
  status                          Show status of backups.
  verify                          Verify the integrity of a backup
```

Performing Backups
------------------

See [Performing backups](Performing-backups.md).

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

See [Restoring a single node](Restoring-a-single-node.md).


Restoring a cluster
-------------------

See [Restoring a full cluster](Restoring-a-full-cluster.md).


Verify an existing backup
-------------------------

```
$ medusa verify --help
Usage: medusa verify [OPTIONS]

  Verify the integrity of a backup

Options:
  --backup-name TEXT   Backup name  [required]
  --enable-md5-checks  During backups and verify, use md5 calculations to
                       determine file integrity (in addition to size, which is
                       used by default)

  --help               Show this message and exit.

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

SSTable files are protected from deletion if they are still within the grace period, which defaults to 10 days.
In order to force purge files within the grace period, you can use the `--backup-grace-period-in-days` flag to reduce the value for the duration of a command (`purge` or `delete-backup`):

```
$ medusa --backup-grace-period-in-days=1 purge
```

Be careful when reducing the grace period to 0 as it can corrupt ongoing backups by deleting their sstables that were uploaded so far.

Delete a backup
---------------

```
$ medusa delete-backup --help
Usage: medusa delete-backup [OPTIONS]

  Delete the given backup on the current node (or on all nodes)

Options:
  --backup-name TEXT              Backup name  [required]
  -a, --all-nodes / -c, --current-node
                                  Delete backups on all nodes (Default is
                                  current node only)
  --help                          Show this message and exit.
```

By default backups will only be deleted on the local node. In order to globally remove a backup add the `-a` flag to the command.

```
$ medusa delete-backup --backup-name=backup1
[2021-07-08 09:49:48,994] INFO: Monitoring provider is noop
[2021-07-08 09:49:50,078] INFO: Deleting Backup backup1...
[2021-07-08 09:49:50,078] INFO: 1 backups are candidate to be purged
[2021-07-08 09:49:50,078] INFO: Purging backup 1 from node ip-172-31-28-101.us-west-2.compute.internal...
[2021-07-08 09:49:50,369] INFO: Cleaning up orphaned files for ip-172-31-28-101.us-west-2.compute.internal...
[2021-07-08 09:49:50,462] INFO: Purged 4 objects with a total size of 82.13 KB
[2021-07-08 09:49:50,462] INFO: 168 objects within 10 days grace period were not deleted
```

Since SSTables and meta files are stored in different places for differential backups, the deletion is a two step process:

* Delete all backup directories
* Scan active backup files from manifests and compare with the list of SSTables in the `data` directory. All SSTables present in the `data` directory but absent from all manifests will get deleted in that step.

SSTable files are protected from deletion if they are still within the grace period, which defaults to 10 days.
In order to force purge files within the grace period, you can use the `--backup-grace-period-in-days` flag to reduce the value for the duration of a command (`purge` or `delete-backup`):

```
$ medusa --backup-grace-period-in-days=1 delete-backup --backup-name=backup1
```


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
