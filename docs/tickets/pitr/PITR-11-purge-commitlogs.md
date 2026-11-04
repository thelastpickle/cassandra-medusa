# PITR-11: Add `purge_commitlogs` to `medusa purge`

- **Type:** Feature
- **Size:** S (2-5 ideal days)
- **Estimated Ideal Days:** 2 days (Est. Calendar Time: 4-6 days)
- **Parallel Track:** Track F - Purge
- **Blocked By / Dependencies:** PITR-01, PITR-03
- **Blocks:** PITR-13, PITR-14

---

## Purpose
> WHY are we doing this work?

Archived commitlog segments grow without limit. Old segments are useless once no kept backup needs them. `medusa purge` already removes old backups. It must also remove the old segments, or storage cost grows forever.

The purge must not delete a segment that a kept backup still needs. A wrong threshold would make a PITR restore fail or lose data. This ticket sets a safe threshold from the oldest kept snapshot.

Design doc reference: section 4.9. Success criterion: SC7.

## Requirements
> WHAT needs to be true for this work to be successful?

### Function

- Add `purge_commitlogs(storage, fqdn, oldest_kept_snapshot_ts, interval_seconds)` in [`medusa/purge.py`](../../../medusa/purge.py).
- List blobs with `storage.storage_driver.list_objects(...)` under `<prefix>/<fqdn>/commitlogs/`. Build the path from `storage.prefix_path` and `fqdn`, in the same way `get_file_paths_from_storage` builds the `data` path.
- Threshold: `purge_threshold = oldest_kept_snapshot_ts - interval_seconds - 30`.
  - `30` is a fixed buffer for clock skew. Add no config knob.
  - `interval_seconds` is `config.pitr.commitlog_archive_interval_seconds`.
- Delete a blob only when `blob.last_modified < purge_threshold` (strictly less).
  - A blob with `last_modified == purge_threshold` MUST stay.
- Use `blob.last_modified` only. Do not parse the segment file name for a time.
- Use `storage.storage_driver.get_object_datetime(blob).timestamp()` for `last_modified`.
- Log at INFO the number of deleted segments, and the `fqdn`.
- Scope: only one `fqdn`. Never list or delete under another node's prefix.
- Return the number of deleted segments (for tests and for the log).

### Retention anchor and call site

- In `main()`, inside the existing `with Storage(...) as storage:` block, call `purge_commitlogs` when `config.pitr.enabled` is true.
- Call it after backups are purged. The anchor must be computed from the backups that remain.
- `oldest_kept_snapshot_ts = min(nb.snapshot_time for nb in surviving_node_backups if nb.snapshot_time is not None)`.
  - `surviving_node_backups` are the `NodeBackup` objects for `config.storage.fqdn` that purge keeps.
  - Backups with `snapshot_time is None` (pre-PITR) are skipped as anchors.
  - Use `NodeBackup.snapshot_time`. Do not use `ClusterBackup.min_snapshot_time`.
- If no surviving backup has a `snapshot_time`, there is no anchor. Delete nothing. Log at INFO that no anchor exists.
- If `config.pitr.enabled` is false: do not call `purge_commitlogs`. Zero change to current purge behaviour.

### Anti-requirements

- MUST NOT delete a segment with `last_modified >= purge_threshold`.
- MUST NOT delete segments of other nodes.
- MUST NOT use `started` as a substitute for a missing `snapshot_time`.
- MUST NOT delete anything when there is no anchor.
- MUST NOT change the return tuple of `main()` or `purge_backups()` in a way that breaks existing callers and tests.
- MUST NOT fail the whole purge for a bad blob in a way that hides the error. Errors follow the existing `except Exception` path in `main()`.

### Required unit tests: `tests/purge_test.py`

Mock `storage` and the driver. Build blobs with `last_modified` values around the threshold. Use example numbers: `oldest_kept_snapshot_ts = 1000`, `interval_seconds = 60`, so `purge_threshold = 910`.

- `last_modified = 909`: deleted.
- `last_modified = 910` (equals threshold): kept.
- `last_modified = 911`: kept.
- Far older (for example `0`): deleted. Far newer: kept.
- `last_modified` as `datetime` is compared correctly.
- Empty blob list: nothing deleted, count is `0`.
- Threshold formula with a different `interval_seconds` (for example `0` and `300`).
- Only the given `fqdn` prefix is listed. Assert the path passed to `list_objects`.
- INFO log contains the deleted count (use `caplog` or a mock on `logging.info`).
- Anchor selection: backups with `snapshot_time = None` are skipped. The minimum of the remaining values is used.
- No backup has `snapshot_time`: `purge_commitlogs` is not called, or deletes nothing.
- `pitr.enabled = false`: `purge_commitlogs` is not called from `main()`.
- `pitr.enabled = true`: `purge_commitlogs` is called once with the anchor computed from surviving backups only (not from the purged ones).
- Existing purge tests still pass without change.

## Definition of Done (Acceptance Criteria)
> How will we know we did the work right?

- [ ] SC7 verified: segments with `last_modified < purge_threshold` are deleted. All others stay. Boundary values are tested.
- [ ] With `pitr.enabled = false`, `medusa purge` behaves as before.
- [ ] Purge on one node never touches another node's `commitlogs/` prefix.
- [ ] Log shows the deleted count at INFO.
- [ ] Unit test coverage implemented (>= 80% coverage for new logic) in `tests/purge_test.py`.
- [ ] Docstring on `purge_commitlogs` explains the threshold and the anchor rule.
- [ ] Linters (`flake8`) and CI pass cleanly.

## Proposed Implementation (Optional / Starting Point)
> Suggestion only. The engineer who owns the ticket owns the final decisions.

- In [`medusa/purge.py`](../../../medusa/purge.py), `main()` (lines 30-62) lists backups with `storage.list_node_backups(fqdn=config.storage.fqdn, backup_index_blobs=backup_index)` and calls `purge_backups(...)`. Compute `backups_to_purge` as it does today. Then derive the survivors as `[b for b in backups if b not in backups_to_purge]`, compute the anchor from them, and call `purge_commitlogs` after `purge_backups`.
- Build the listing prefix like `get_file_paths_from_storage` (line 172): `"{}{}/commitlogs".format(storage.prefix_path, fqdn)`. Check that `prefix_path` already ends with `/` and match the style used there. Add a trailing `/` if needed so one node's prefix cannot match another node's prefix.
- Delete with `storage.storage_driver.delete_object(blob)`, as `purge_backup` and `cleanup_obsolete_files` do.
- `is_older_than_gc_grace` (line 190) shows how the file already converts `datetime` to a timestamp with `datetime.timestamp(...)`. Check how `last_modified` is typed in `AbstractBlob` ([`medusa/storage/abstract_storage.py`](../../../medusa/storage/abstract_storage.py)); `get_object_datetime` there handles per-driver differences.
- `delete_backup()` (line 243) also calls `purge_backups`. The design doc names only `main()` as call site. Do not add the call to `delete_backup()` unless the team agrees.
- Existing test file to extend: [`tests/purge_test.py`](../../../tests/purge_test.py).

---

## Why this matters to you
A purge that deletes too much breaks restores, and nobody sees it until restore day. Test the boundary values with care. Ask the PITR-03 owner if `snapshot_time` behaves in an unexpected way.
