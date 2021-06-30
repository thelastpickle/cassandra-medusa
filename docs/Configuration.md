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
;nodetool_username =  <my nodetool username>
;nodetool_password =  <my nodetool password>
;nodetool_password_file_path = <path to nodetool password file>
;nodetool_host = <host name or IP to use for nodetool>
;nodetool_port = <port number to use for nodetool>

; Command ran to verify if Cassandra is running on a node. Defaults to "nodetool version"
;check_running = nodetool version

; Disable/Enable ip address resolving.
; Disabling this can help when fqdn resolving gives different domain names for local and remote nodes
; which makes backup succeed but Medusa sees them as incomplete.
; Defaults to True.
resolve_ip_addresses = True

[storage]
storage_provider = <Storage system used for backups>
; storage_provider should be either of "local", "google_storage" or "s3"
region = <Region hosting the storage>

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

[monitoring]
;monitoring_provider = <Provider used for sending metrics. Currently either of "ffwd" or "local">

[ssh]
;username = <SSH username to use for restoring clusters>
;key_file = <SSH key for use for restoring clusters. Expected in PEM unencrypted format.>
;cert_file = <Path of public key signed certificate file to use for authentication. The corresponding private key must also be provided via key_file parameter>

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

```