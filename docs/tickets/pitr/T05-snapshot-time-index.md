# T05: Persist `snapshot_time` index blob and read it in `list_node_backups()`

- **Type:** Feature
- **Size:** S (2–5 ideal days)
- **Estimated Ideal Days:** 2 days (Est. Calendar Time: 4 days)
- **Parallel Track:** B – Storage
- **Blocked By / Dependencies:** T02 (`snapshot_time` property on `NodeBackup`)
- **Blocks:** T06

---

## Purpose

`select_node_base_snapshot` requires an accurate per-node `snapshot_time` to correctly bound PITR base-backup selection. This ticket wires the snapshot timestamp captured in T02 into the index layer: it is written as a dedicated blob when a backup completes, and read back by `list_node_backups()` to populate `NodeBackup.snapshot_time`. Pre-PITR backups without the blob leave `NodeBackup.snapshot_time` as `None` (no fallback to `started`), preserving full backward compatibility for non-PITR operations while preventing unsafe PITR restore selections.

## Requirements

- Modify `medusa/index.py` to persist a `snapshot_time_{fqdn}_{timestamp}.timestamp` index blob containing the node's snapshot mtime (captured from `cassandra_utils.py` in T02) when a backup finishes.
- Modify `medusa/storage/__init__.py` in `list_node_backups()` to:
  - List and read the `snapshot_time_*` index blob for each node backup.
  - Populate `NodeBackup._snapshot_time` with the parsed value when found.
  - When the blob is absent (pre-PITR backup), leave `_snapshot_time` unset so `NodeBackup.snapshot_time` remains `None`.
- The blob format is a plain text file containing a single float (Unix epoch seconds).
- Must NOT change the structure of existing backup index blobs.
- Must NOT require a migration step for pre-PITR backups.

**Test scenarios:**
- Writing a backup with `snapshot_time` creates the `snapshot_time_*` blob in storage.
- `list_node_backups()` populates `NodeBackup._snapshot_time` from the blob when present.
- `list_node_backups()` leaves `_snapshot_time` unset when the blob is absent; `NodeBackup.snapshot_time` returns `None`.
- Two nodes with different snapshot times are each populated correctly.

## Definition of Done (Acceptance Criteria)

- [ ] `index.py` writes `snapshot_time_{fqdn}_{timestamp}.timestamp` blob on backup completion.
- [ ] `list_node_backups()` reads and populates `NodeBackup._snapshot_time` when blob present.
- [ ] When blob is absent, `NodeBackup.snapshot_time` remains `None`.
- [ ] Unit tests cover write path, read path, and absent blob behavior.
- [ ] No changes to existing index blob formats.
- [ ] All existing tests pass with no regressions.
- [ ] Linters pass cleanly.

## Proposed Implementation (Starting Point)

- In `index.py`, add a helper `write_snapshot_time(storage, fqdn, backup_name, snapshot_time_s)` that uploads a blob with content `str(snapshot_time_s)`.
- In `storage/__init__.py`, after loading other index blobs for a node backup, attempt to read the `snapshot_time_*` blob; on success parse with `float(content)`; on `NotFound` / `KeyError` do nothing.
- Blob path: `{prefix}/{fqdn}/index/snapshot_time_{backup_name}.timestamp`.
