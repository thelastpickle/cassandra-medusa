# PITR-02: Record `snapshot_time` at backup time

- **Type:** Feature
- **Size:** S (2-5 ideal days)
- **Estimated Ideal Days:** 3 days (Est. Calendar Time: 6-7 days)
- **Parallel Track:** Track B - Storage
- **Blocked By / Dependencies:** PITR-03 (merge first or in the same release)
- **Blocks:** PITR-12, PITR-13

---

## Purpose
> WHY are we doing this work? What problem does it solve and what value does it deliver?

A PITR restore needs to know when the snapshot data was frozen on each node. Cassandra snapshots are not instant. The backup `started` time can be earlier than the real snapshot time. If Medusa used `started`, a restore could pick a base backup that already holds data written after the target time. Commitlog replay cannot undo that (design doc section 7, question 4).

This ticket records the real snapshot time at backup time. It stores the value as an index blob in object storage. PITR-03 reads it back.

## Requirements
> WHAT needs to be true for this work to be successful? (Bulleted list)

- At backup time, read the snapshot directory modification time (mtime) of the node. Do this before the snapshot is cleaned up.
- The value is an integer epoch time in seconds. This matches `started` and `finished`.
- Write the value to this index blob (design doc section 4.2):
  - `{prefix_path}index/backup_index/{backup_name}/snapshot_time_{fqdn}_{timestamp}.timestamp`
  - `{timestamp}` is the snapshot time. The blob content is `str(timestamp)`. This matches the `started` and `finished` blobs.
- If no snapshot directory is present, do NOT write a blob. `snapshot_time` is `None` / missing.
- Do NOT fall back to `started`, `finished` or the current time. Any fallback is a defect (design doc section 4.2, section 7 question 4).
- Do NOT write the blob from `build_indices` / `process_backup`. Old backups have no snapshot directory. They stay without `snapshot_time`.
- Write the blob only when `config.pitr.enabled` is true. With PITR off, no new blob is written and backup output is identical to today (G3, SC4). A backup without the blob is not PITR-capable (design doc section 3).
- A failure to read the mtime must not fail the backup. Log a warning and write no blob.
- Blob name constraint: the name must match the index blob pattern that PITR-03 adds to `medusa/storage/__init__.py`. See "Merge order" below.
- Required unit tests:
  - Snapshot time is the latest directory mtime when several snapshot dirs exist.
  - Snapshot time is `None` when no snapshot dirs exist.
  - The index function uploads the blob with the exact name and content.
  - No blob is uploaded when the value is `None`.
  - `pitr.enabled = false`: no blob uploaded.
  - `do_backup` reads the value before the snapshot is deleted.

**Merge order:** `Storage.get_fqdn_from_any_index_blob` asserts on blob names it does not know. Today the regex `INDEX_BLOB_WITH_TIMESTAMP_PATTERN` only knows `started|finished`. A `snapshot_time_...` blob in a real index breaks `list_node_backups` until PITR-03 extends the regex. Set Dependencies to `PITR-03 (merge first or same release)`. Update the README.

## Definition of Done (Acceptance Criteria)
> How will we know we did the work right?

- [ ] A backup of a node with snapshot dirs leaves one `snapshot_time_{fqdn}_{timestamp}.timestamp` blob in the backup index.
- [ ] A backup with no snapshot dirs leaves no such blob and does not fail.
- [ ] The value never comes from `started`.
- [ ] With pitr.enabled=false no snapshot_time blob is written.
- [ ] Unit test coverage implemented (>= 80% coverage for new logic).
- [ ] Existing unit tests pass with no change to their expectations.
- [ ] Documentation / comments updated.
- [ ] Linters (`flake8`) and CI pass cleanly.

## Proposed Implementation (Optional / Starting Point)
> Suggested technical approach as a non-prescriptive starting point.

This is a suggestion. You own the final design.

- [`medusa/cassandra_utils.py`](../../../medusa/cassandra_utils.py:522): `Cassandra.Snapshot` already has `find_dirs()` ([line 550](../../../medusa/cassandra_utils.py:550)). It returns `SnapshotPath` objects with a `.path`. Add a method or property on `Cassandra.Snapshot` that takes `path.stat().st_mtime` of those dirs and returns `int(...)`, or `None` for an empty list.
- Rule: use the **latest** mtime of all snapshot dirs. The base SSTables then hold no data after this value (design doc 4.5 "Why the latest mtime"). Put the rule in a docstring and in the unit test.
- [`medusa/backup_node.py`](../../../medusa/backup_node.py:225): in `do_backup`, read the value inside `with snapshot:` ([line 239](../../../medusa/backup_node.py:239)). `Snapshot.__exit__` deletes the snapshot, so the dirs are gone after the block. The `use_existing_snapshot` path ([line 231](../../../medusa/backup_node.py:231)) uses the same `Snapshot` class.
- [`medusa/index.py`](../../../medusa/index.py:112): add a function next to `add_backup_finish_to_index`. For example `add_snapshot_time_to_index(storage, node_backup, snapshot_time)`. Build the path the same way as the `started` blob ([line 101](../../../medusa/index.py:101)). Upload with `storage.storage_driver.upload_blob_from_string`. Return early on `None`.
- Call it from `do_backup` near `add_backup_finish_to_index(storage, node_backup)` ([line 257](../../../medusa/backup_node.py:257)). Passing the value as a parameter keeps this ticket independent of the `NodeBackup.snapshot_time` property (PITR-03).
- `clean_backup_from_index` ([line 128](../../../medusa/index.py:128)) deletes by `"_" + fqdn` in the blob name. It already covers the new blob. Add a test line that confirms it.
- Tests:
  - [`tests/cassandra_utils_test.py`](../../../tests/cassandra_utils_test.py): use `tmp_path`-style temp dirs and `os.utime` to set known mtimes.
  - [`tests/backup_node_test.py`](../../../tests/backup_node_test.py): mock the storage driver. Assert on `upload_blob_from_string` calls.
  - There is no `tests/index_test.py` yet. Create it for the index function.
