## Change Log

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
