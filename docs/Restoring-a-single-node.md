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

  --keyspace TEXT         Restore tables from this keyspace, use --keyspace ks1 [--keyspace ks2]
  --table TEXT            Restore only this table, use --table ks.t1 [--table ks.t2]
  --use-sstableloader     Use the sstableloader to load the backup into the
                          cluster

  --help                  Show this message and exit.
```

In order to restore a backup on a single node, run the following command:

```
$ sudo medusa restore-node --in-place --backup-name=<name of the backup>
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