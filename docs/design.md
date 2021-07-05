Design and Datastructures
=========================
Medusa is designed to have node-specific operations, but cluster-aware data structures. Backups and
restores happen locally on each node without any centralized coordination, but the backed up data
structures are designed to aid making decisions about the backup state of the entire cluster.

### Backup
Backups are performed by making an Apache Cassandra™ snapshot and copying it along with
the schema and topology to a storage system.

Backups can be performed in 2 different fashions:

- **Full backups**: the entire data set is transferred and stored every time. The GCS implementation of the storage layer will opportunistically transfer previously backed up files from the bucket itself to avoid unnecessary network transfers.
- **Differential backups**: only newly created SSTables will be transferred and the manifest file will link to previously backed up files that are still being used by Cassandra. **This feature is not related to nor using the incremental backup feature from Cassandra.**


#### Data structures
The backed up data is stored using the following structure when performing full backups:

```
root
   |
   -----<fqdn>
   |      |
   |      ------<backup-name>
   |                   |
   |                   ---------data
   |                   |         |
   |                   |         -------<keyspace>
   |                   |                    |
   |                   |                    ----------<table>
   |                   |                                 |
   |                   |                                 -------sstable1
   |                   |                                 |
   |                   |                                 -------sstable2
   |                   |
   |                   ---------meta
   |                             |
   |                             -------manifest.json
   |                             |
   |                             -------schema.cql
   |                             |
   |                             -------tokenmap.json
   |
   ----- index
```

When performing differential backups, the following structure will be used:

```
root
   |
   -----<fqdn>
   |      |
   |      ---------data
   |      |          |
   |      |          -------<keyspace>
   |      |                    |
   |      |                    ----------<table>
   |      |                                 |
   |      |                                 -------sstable1
   |      |                                 |
   |      |                                 -------sstable2
   |      |
   |      ------<backup-name>
   |                   |
   |                   ---------meta
   |                             |
   |                             -------manifest.json
   |                             |
   |                             -------schema.cql
   |                             |
   |                             -------tokenmap.json
   |                             |
   |                             -------differential
   |
   ----- index
```


- `<optional prefix>` allows several clusters to share the same bucket, but is not encouraged as
buckets are cheap anyway. The support for this prefix might be dropped in later development.
- `<fqdn>` is the FQDN of the backed up node.
- `<backup name>` is the name of the backup, which defaults to a timestamp rounded to hours. Data
  from different nodes with the same `<backup name>` is considered part of the same backup, and
  expected to have been created at close to the same time.
- `schema.cql` contains the CQL commands to recreate the schema. This is the very first file to be
  uploaded to the bucket, and thus the existence of this file indicates that a backup has begun.
- `tokenmap.json` contains the topology (token) configuration of the cluster as seen by the node
  at the time of backup.  The tokenmap includes the placement for designated rack and datacenter. 
- `manifest.json` will contain a list of all expected data files along with expected sizes and
  MD5 checksums. This can be used to easily validate the content of a backup in a bucket.
  The content of `manifest.json` is generated on the node as part of the upload process.
  This is the last file to be uploaded to the bucket, thus the existence of this file means that the
  backup is complete.

#### Optimizations
As Cassandra's SSTables are immutable, it is possible to optimize the backup operation by
recognizing duplicate files in previous backups and copying each SSTable exactly once.

For the same reason, it is possible to copy each SSTable exactly once and then refer to it from multiple manifests.
With this approach, Medusa also manages to save resources by storing each SSTable exactly once.

### Restore
Restoring is a bit more complicated and opinionated than backing up as it depends on whatever
tools you're using to manage the cluster's configuration and processes. Medusa provides both the necessary operations to build your own restore scripts for your environment, but it also offers one implementation of the entire restore process.

#### Restoring a single Cassandra node
- Before attempting to restore a Cassandra node, the restore script must compare the node's token
  configuration to the backed up data's topology, and make sure the token configurations match.
- If the topology matches, download all the backed up data to a temporary location.
- Stop the Cassandra process
- Delete any existing data
- Move the backed up data to Cassandra's data directory
- Start the Cassandra process
- Apply the schema from the backed up data
- Discover backed up SSTables

#### Restore a whole Cassandra cluster
- Pick a random node from the backed up data and download the topology.
- Configure the cluster to match the backed up topology. This step highly depends on the purpose
  of the restoring and which tools are used to configure Cassandra. It may involve allocating new
  nodes and configure them appropriately; or it may simply validate the configuration of an existing
  cluster and fail if it doesn't match.
- Run the [the previous section](#Restoring-a-single-Cassandra-node) on each individual node.
