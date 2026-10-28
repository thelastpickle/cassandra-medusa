# PITR-03: Expose `snapshot_time` on `NodeBackup` / `ClusterBackup`

- **Type:** Feature
- **Size:** S (2-5 ideal days)
- **Estimated Ideal Days:** 3 days (Est. Calendar Time: 6-7 days)
- **Parallel Track:** Track B - Storage
- **Blocked By / Dependencies:** None (shares blob contract with PITR-02)
- **Blocks:** PITR-02, PITR-07, PITR-11

---

## Purpose
> WHY are we doing this work? What problem does it solve and what value does it deliver?

PITR restore, purge and the operator all need the snapshot time of a backup. They read it from `NodeBackup`, not from raw blobs. This ticket reads the `snapshot_time` index blob (written by PITR-02) and exposes it on `NodeBackup` and `ClusterBackup`.

`node_backup.snapshot_time is not None` is the storage-readable flag that a backup can be restored with PITR. The operator uses it without access to the control plane (design doc section 3).

## Requirements
> WHAT needs to be true for this work to be successful? (Bulleted list)

- Blob contract (shared with PITR-02): `{prefix_path}index/backup_index/{backup_name}/snapshot_time_{fqdn}_{timestamp}.timestamp`. `{timestamp}` is an integer epoch time in seconds.
- `NodeBackup.snapshot_time` returns the integer timestamp from that blob name. It returns `None` when the blob does not exist (pre-PITR backups).
- Do NOT fall back to `started`, `finished` or any other value. `None` is a valid and expected result.
- `Storage.list_node_backups()` must find the `snapshot_time` blob for each node backup and pass the value to `NodeBackup`.
- `Storage.get_fqdn_from_any_index_blob()` and `Storage.get_timestamp_from_blob_name()` must accept `snapshot_time_...` blob names. Today `INDEX_BLOB_WITH_TIMESTAMP_PATTERN` only knows `started|finished`. A new blob name that no pattern matches makes the code assert.
- Reading `snapshot_time` must not cost an extra storage call per backup. It must use the index blobs that `list_node_backups` already lists.
- `NodeBackup` objects built by `Storage.get_node_backup(fqdn=..., name=...)` have no preloaded blob. For them, `snapshot_time` must be resolved lazily with ONE `storage_driver.list_objects('{prefix}index/backup_index/{name}/snapshot_time_{fqdn}_')` call, parsed with `Storage.get_timestamp_from_blob_name`. Cache the result, including `None`, so the call happens at most once per object. `pitr_restore` (PITR-10) relies on this.
- `ClusterBackup.min_snapshot_time` and `ClusterBackup.max_snapshot_time` aggregate `node_backup.snapshot_time` over the nodes of the backup.
  - Ignore nodes whose value is `None`.
  - Return `None` when no node has a value.
- Existing behaviour must not change for backups with no `snapshot_time` blob: listing, sorting, `started`, `finished`, `ClusterBackup.to_json_dict`.
- Required unit tests in `tests/storage_test.py`:
  - `NodeBackup.snapshot_time` is read from the index blob.
  - `NodeBackup.snapshot_time` is `None` when the blob is absent. It is not equal to `started`.
  - `Storage.get_fqdn_from_any_index_blob` and `Storage.get_timestamp_from_blob_name` parse a `snapshot_time_...` blob name, with and without a prefix.
  - `list_node_backups` returns the value for a node with the blob and `None` for a node without it.
  - `ClusterBackup.min_snapshot_time` / `max_snapshot_time` with all nodes set, some `None`, and all `None`.
  - `get_node_backup(...).snapshot_time` returns the value from the index blob via one `list_objects` call, and a second access makes no call.
  - `get_node_backup(...).snapshot_time` is `None` when no blob matches, and is cached.

## Definition of Done (Acceptance Criteria)
> How will we know we did the work right?

- [ ] `list_node_backups` on an index with `snapshot_time` blobs returns `NodeBackup` objects with the right `snapshot_time`.
- [ ] `list_node_backups` on an index without these blobs returns `snapshot_time == None` and the same results as before.
- [ ] `ClusterBackup.min_snapshot_time` and `max_snapshot_time` return the right values in all three cases above.
- [ ] get_node_backup(...).snapshot_time works without list_node_backups.
- [ ] Unit test coverage implemented (>= 80% coverage for new logic).
- [ ] Existing storage tests pass with no change to their expectations.
- [ ] Documentation / comments updated.
- [ ] Linters (`flake8`) and CI pass cleanly.

## Proposed Implementation (Optional / Starting Point)
> Suggested technical approach as a non-prescriptive starting point.

This is a suggestion. You own the final design.

- [`medusa/storage/__init__.py`](../../../medusa/storage/__init__.py:46): extend `INDEX_BLOB_WITH_TIMESTAMP_PATTERN` to `(started|finished|snapshot_time)`. `get_fqdn_from_any_index_blob` ([line 321](../../../medusa/storage/__init__.py:321)) and `get_timestamp_from_blob_name` ([line 347](../../../medusa/storage/__init__.py:347)) use it. Group 3 must stay the timestamp.
- [`medusa/storage/__init__.py`](../../../medusa/storage/__init__.py:218): in `list_node_backups`, look up the blob like `started`: `self.lookup_blob(blobs_by_backup, backup_name, tokenmap_fqdn, 'snapshot_time')`. Parse it with `get_timestamp_from_blob_name`. Initialise the variables next to `started_blob, finished_blob` ([line 212](../../../medusa/storage/__init__.py:212)) so they are `None` when the node has no entry in `blobs_by_backup`.
- Check `lookup_blob` ([line 352](../../../medusa/storage/__init__.py:352)). It matches by substring (`chunk in blob.name`). Confirm that `'snapshot_time'` does not match another blob, and that no existing chunk (`started`, `finished`, `schema`, `manifest`, `tokenmap`, `differential`, `incremental`) matches the new blob.
- [`medusa/storage/node_backup.py`](../../../medusa/storage/node_backup.py:23): add constructor arguments such as `snapshot_time_timestamp=None` and `snapshot_time_blob=None`. Follow `started_timestamp` / `started_blob` ([lines 33-36](../../../medusa/storage/node_backup.py:33), [81-84](../../../medusa/storage/node_backup.py:81)). Add a `snapshot_time` property that returns the stored value. Do not read the schema or manifest blob for it.
- `discover_node_backups` ([line 121](../../../medusa/storage/__init__.py:121)) also gets the lazy lookup. It does not preload the index.
- Use a sentinel (`_UNSET = object()`) for the cache, since `None` is a valid result.
- [`medusa/storage/cluster_backup.py`](../../../medusa/storage/cluster_backup.py:19): add `min_snapshot_time` and `max_snapshot_time` properties. `started` ([line 38](../../../medusa/storage/cluster_backup.py:38)) shows the `operator.attrgetter` style. Filter out `None` before `min` / `max`. Use `expected_node_backups()` or `node_backups.values()`. Pick one and add a test for it.
- Tests: [`tests/storage_test.py`](../../../tests/storage_test.py). `test_parse_backup_index` ([line 254](../../../tests/storage_test.py:254)) and `test_get_timestamp_from_blob_name` ([line 349](../../../tests/storage_test.py:349)) show how to build index blobs. `tests/storage_test_with_prefix.py` covers the prefix case. Add a case there if the prefix path differs.
