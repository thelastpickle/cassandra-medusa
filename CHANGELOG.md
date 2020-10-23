## Change Log

### 0.8.0 (TBD)
- Add Azure blob storage support (@ilhanadiyaman)

### 0.7.2 (TBD)
- Fixed flakey tests (@adejanovski / @arodrime)
- Avoid double purge (through count + age) of the same backup (@arodrime)
- Remove nc (netcat) dependency, use python socket instead (@arodrime)
- Add multi-cassandra version integration tests (@adejanovski)
- Pin ssh2 version to 0.19.0 to avoid broken dependency (@arodrime)

### 0.7.1 (2020/07/30 11:12 +00:00)
- Add a timeout for nc checks - node_up? (@arodrime)
- Do not rely on nc command output, use returncode instead (@arodrime)

### 0.7.0 (2020/07/03 08:04 +00:00)
- Restore node should be done in place by default (@adejanovski)
- Use the service command as default to start Cassandra (@adejanovski)
- Add a setting to avoid ip addresses from being resolved to hostnames (@adejanovski)
- Do not delete cleanup and saved_caches folders (@arodrime)
- fixing empty folder issue (@arodrime)
- specify resotre specific keyspaces/tables usage (@arodrime)
- Keep system keyspaces when user specifies ks/tables (@arodrime)
- Add wait for node shutdown. Fix healtcheck config. Fixes #72 (@rzvoncek)
- Enforce tokens by modifying cassandra.yaml instead of env vars (@adejanovski)
- Use the local fqdn as seed target if it's not provided (@adejanovski)
- Simplify the checks for whether the seed_target or if the host_list is set. (@adejanovski)
- Overwrite auth by default (@adejanovski)
- Compute in-place based on source/target node list. (@adejanovski)
- Move question about keeping auth to a more appropriate location to avoid clashing with in place computation (@adejanovski)
- Fix --keep-auth flag issue (@adejanovski)
- Move digest computation to storage implementations. Fixes #123 (@rzvoncek)
- Resolve FQDN of the ip addresses provided in the host list file (@adejanovski)
- Fix restore infinite loop by using nc instead of remote nodetool invocation (@adejanovski)
- Fix debian packaging which broke due to gevent not building anymore. (@adejanovski)
- Make 1st differential backup upload all files, even if a full one exists. Fixes 108 (@rzvoncek)

### 0.6.0 (2020/05/15 13:27 +00:00)
- Add ini option to enable -ssl parameter for nodetool (@ANeumann82)
- Add S3 region selector for S3 rgw (@mclpfr)
- Disable checksum comparisons for the local storage provider. (@adejanovski)
- Enable SSL Authentication for Medusa to access Cassandra SSL Clusters (@SINDHUJA21)
- Local and s3 RGW backends now use streams for both uploads and downloads. (@nicholasamorim)
- fix fqdn not honored in config file (@chebelom)
- fix medusa download command not working due to missing parameter (@chebelom)
- medusa get_last_complete_cluster_backup explodes if no complete cluster backup exists (@chebelom)
- Add a delete-backup command (@arodrime)
- Allow awscli binary path to be specified (@bishoprunout)
- Add ssh port info to Readme.md and medusa-example.ini (@alvaropalmeirao)

### 0.5.1
- Improve S3 connection performance when IAM roles aren't used (@adejanovski)

### 0.5.0
- Add customized port to ssh section (@alvaropalmeirao)
- Fix the usage of prefix for multi tenant buckets (@adejanovski)
- Fix sstableloader calls for clusters without authentication (@adejanovski)
- Adding support for Env Credentials and IAM Role (@alvaropalmeirao)
- add aws regions by bumping libcloud (@arodrime)

### 0.4.1
- Publish debian packages (@arodrime/@adejanovski)
- Fixed the path to the file where S3 is configured for integration tests. (@pumpkiny9120)
- Use awscli for large files downloads (@adejanovski)
- Fixing issue #54 - permission denied during build (@arodrime)
- Pass temp_dir to restore-node command from restore-cluster command (@jfharden)
- Add offline installation informations (@adejanovski)
- Set nodetool parameters (@bhagenbourger)
- Instead of failing on nodetool clearsnapshot, just log a warning message as the backup is essentially finished. (@nicholasamorim)
- Add Gitter badge (@gitter-badger)
- Added support to Ceph Object Gateway, an API compatible with S3. Uses s3_rgw libcloud driver. (@nicholasamorim)
- Added logging section to configuration which provides control over logging-to-file (@nicholasamorim)
- Fix compatibility with python3.5

### 0.3.1
- Fix differential backups re-uploading files in S3 (@adejanovski)

### 0.3.0
- Use multi part uploads for S3 (@adejanovski)
- Replacing custom ssh management by parallel-ssh lib (@arodrime)
- Parse blob names with regexpes. Fixes #12 (@rzvoncek)
- requirements require minimum versions, not exact versions (@michaelsembwever)
- Added nose & flake8 libs to test libraries (@maciej-lasyk)
- Fix contact point address discovery when listen_address is missing from cassandra.yaml (@adejanovski)
- use correct terminology of 'IAM Policy' rather than 'IAM Strategy' (@michaelsembwever)
- Support backups of SIs (@rzvoncek)

### v0.2.1
- Adjust installation procedure in README (@adejanovski)
- Separate build and publish workflows. (@adejanovski)

### v0.2.0
- Fix release publishing step (@adejanovski)
- Bump pyyaml from 3.10 to 5.1 (@dependabot[bot])
- Publish releases to pypi (@adejanovski)
- Cleanup ci workflow (@adejanovski)
- Add Github actions workflow (@adejanovski)
- Change default Cassandra stop command (@adejanovski)
- Remove 'incremental' in tests and docs (@rzvoncek)
- Switch incremental to differential. (@adejanovski)
- Move to incremental by default (@adejanovski)
- Update paramiko dependency to v2.6.0 (@arodrime)
- Fix hostname being used instead of fqdn (@adejanovski)
- Initial commit (@adejanovski)
