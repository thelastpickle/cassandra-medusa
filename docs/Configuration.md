### Configure Medusa

Create the `/etc/medusa` directory if it doesn't exist, and create a file named `/etc/medusa/medusa.ini` with the content of [medusa-example.ini](../medusa-example.ini).
Modify it to match your requirements:

```
[cassandra]
;stop_cmd = /etc/init.d/cassandra stop
;start_cmd = /etc/init.d/cassandra start
;config_file = <path to cassandra.yaml. Defaults to /etc/cassandra/cassandra.yaml>
;cql_username = <username>
;cql_password = <password>
; When using the following setting there must be files in:
; - `<cql_k8s_secrets_path>/username` containing username
; - `<cql_k8s_secrets_path>/password` containing password
;cql_k8s_secrets_path = <path to kubernetes secrets folder>
;nodetool_username =  <my nodetool username>
;nodetool_password =  <my nodetool password>
;nodetool_password_file_path = <path to nodetool password file>
;nodetool_k8s_secrets_path = <path to nodetool kubernetes secrets folder>
;nodetool_host = <host name or IP to use for nodetool>
;nodetool_port = <port number to use for nodetool>
;certfile= <Client SSL: path to rootCa certificate>
;usercert= <Client SSL: path to user certificate>
;userkey= <Client SSL: path to user key>
;sstableloader_ts = <Client SSL: full path to truststore>
;sstableloader_tspw = <Client SSL: password of the truststore>
;sstableloader_ks = <Client SSL: full path to keystore>
;sstableloader_kspw = <Client SSL: password of the keystore>
;sstableloader_bin = <Location of the sstableloader binary if not in PATH>

; Enable this to add the '--ssl' parameter to nodetool. The nodetool-ssl.properties is expected to be in the normal location
;nodetool_ssl = true

; Command ran to verify if Cassandra is running on a node. Defaults to "nodetool version"
;check_running = nodetool version

; Disable/Enable ip address resolving.
; Disabling this can help when fqdn resolving gives different domain names for local and remote nodes
; which makes backup succeed but Medusa sees them as incomplete.
; Defaults to True.
resolve_ip_addresses = True

; When true, almost all commands executed by Medusa are prefixed with `sudo`.
; Does not affect the use_sudo_for_restore setting in the 'storage' section.
; See https://github.com/thelastpickle/cassandra-medusa/issues/318
; Defaults to True
;use_sudo = True

[storage]
storage_provider = <Storage system used for backups>
; storage_provider should be either of "local", "google_storage" or "s3"
region = <Region hosting the storage>

; Storage class to use when uploading objects.
; Use a value specific to chosen `storage_provider` that supports both reads and writes (eg S3's GLACIER and Azure's ARCHIVE won't work).
; If not specified, we default to the 'hottest' class (STANDARD, STANDARD, HOT for GCP, AWS, AZURE respectively).
; Supported values:
; AWS S3: STANDARD | REDUCED_REDUNDANCY | STANDARD_IA | ONEZONE_IA | INTELLIGENT_TIERING
;    GCP: STANDARD |        Unsupported | Unsupported | Unsupported
;  AZURE:      HOT |               COOL |        COLD
; https://aws.amazon.com/s3/storage-classes/
; https://cloud.google.com/storage/docs/storage-classes
; https://learn.microsoft.com/en-us/azure/storage/blobs/access-tiers-overview
; storage_class = <Storage Class Name used to store backups>

; Name of the bucket used for storing backups
bucket_name = cassandra_backups

; storage_provider should be "s3"
kms_id = <ARN of KMS key used for server-side bucket encryption>

; JSON key file for service account with access to GCS bucket or AWS credentials file (home-dir/.aws/credentials)
; optional if using GCS (see ./Docs/gcs_setup.md)
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

; GC grace period for backed up files. Prevents race conditions between purge and running backups
backup_grace_period_in_days = 10

; When not using sstableloader to restore data on a node, Medusa will copy snapshot files from a
; temporary location into the cassandra data directroy. Medusa will then attempt to change the
; ownership of the snapshot files so the cassandra user can access them.
; Depending on how users/file permissions are set up on the cassandra instance, the medusa user 
; may need elevated permissions to manipulate the files in the cassandra data directory.
;
; This option does NOT replace the `use_sudo` option under the 'cassandra' section!
; See: https://github.com/thelastpickle/cassandra-medusa/pull/399
;
; Defaults to True
;use_sudo_for_restore = True

;api_profile = <AWS profile to use>

;host = <Optional object storage host to connect to>
;port = <Optional object storage port to connect to>

; Configures the use of SSL to connect to the object storage system.
;secure = True

;aws_cli_path = <Location of the aws cli binary if not in PATH>

[monitoring]
;monitoring_provider = <Provider used for sending metrics. Currently either of "ffwd" or "local">

[ssh]
;username = <SSH username to use for restoring clusters>
;key_file = <SSH key for use for restoring clusters. Expected in PEM unencrypted format.>
;port = <SSH port for use for restoring clusters. Default to port 22.>
;cert_file = <Path of public key signed certificate file to use for authentication. The corresponding private key must also be provided via key_file parameter>
;keepalive_seconds = <seconds between ssh keepalive messages to the ssh server. Default to 60 seconds. Due to a limitation in parallel-ssh, if 'cert_file' is defined, then 'keepalive_seconds' will be ignored and no keep alive messages will be sent>
;use_pty = <Boolean: Allocates pseudo-terminal. Default to False. Useful if sudo settings require a tty>
; Enables the usage of a 'login' shell which, among other things, loads user's profile files.
;login_shell = False

[checks]
;health_check = <Which ports to check when verifying a node restored properly. Options are 'cql' (default), 'thrift', 'all'.>
;query = <CQL query to run after a restore to verify it went OK>
;expected_rows = <Number of rows expected to be returned when the query runs. Not checked if not specified.>
;expected_result = <Coma separated string representation of values returned by the query. Checks only 1st row returned, and only if specified>
;enable_md5_checks = <During backups and verify, use md5 calculations to determine file integrity (in addition to size, which is used by default)>

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

[grpc]
; Set to true when running in grpc server mode.
; Allows to propagate the exceptions instead of exiting the program.
;enabled = False
;port = <grpc port the server listens to. Defaults to port 50051.>

[kubernetes]
; The following settings are only intended to be configured if Medusa is running in containers, preferably in Kubernetes.
;enabled = False
;cassandra_url = <URL of the management API snapshot endpoint. For example: http://127.0.0.1:8080/api/v0/ops/node/snapshots>

; Enables the use of the management API to create snapshots. Falls back to using Jolokia if not enabled.
;use_mgmt_api = True
```

### Environment Variable Overrides

Some config settings can be overriden through environment variables prefixed with `MEDUSA_`:

| Setting                     | Env Variable                       |
|-----------------------------|------------------------------------|
| `cql_username`              | `MEDUSA_CQL_USERNAME`              |
| `cql_password`              | `MEDUSA_CQL_PASSWORD`              |
| `cql_k8s_secrets_path`      | `MEDUSA_CQL_K8S_SECRETS_PATH`      |
| `nodetool_username`         | `MEDUSA_NODETOOL_USERNAME`         |
| `nodetool_password`         | `MEDUSA_NODETOOL_PASSWORD`         |
| `nodetool_k8s_secrets_path` | `MEDUSA_NODETOOL_K8S_SECRETS_PATH` |
| `sstableloader_tspw`        | `MEDUSA_SSTABLELOADER_TSPW`        |
| `sstableloader_kspw`        | `MEDUSA_SSTABLELOADER_KSPW`        |
| `resolve_ip_addresses`      | `MEDUSA_RESOLVE_IP_ADDRESSES`      |

### Sourcing environment variables

If you are using environment variables to override some settings, you can source a file containing the environment variables before running Medusa commands. For example, if you have a file named `/etc/default/cassandra-medusa` containing exported environment variables.

These variables will also be available when running Medusa cluster commands, e.g. backup-cluster.
