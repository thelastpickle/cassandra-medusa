# T02: Add `snapshot_time` to `NodeBackup` / `ClusterBackup`

- **Type:** Feature
- **Size:** S (2–5 ideal days)
- **Estimated Ideal Days:** 3 days (Est. Calendar Time: 6 days)
- **Parallel Track:** B – Storage (independent)
- **Blocked By / Dependencies:** None
- **Blocks:** T05, T06

---

## Purpose

PITR base-snapshot selection requires knowing when a node completed its snapshot, not just when the backup job started. This ticket adds a `snapshot_time` property to `NodeBackup` (returning `None` when the snapshot timestamp / index blob is absent) and aggregation properties to `ClusterBackup`, so all PITR logic can rely on a consistent, per-node timestamp that accurately bounds the commitlog replay window.

## Requirements

- Add `snapshot_time` property to `medusa/storage/node_backup.py`:
  - Primary source: timestamp of the first snapshot directory (from mtime inspection in `cassandra_utils.py` / index blob).
  - Returns `None` when the snapshot timestamp / index blob is absent (pre-PITR backups and cases where no snapshot directories are present) — no fallback to `started`.
  - `snapshot_time` must always be ≤ actual snapshot time (safe direction for PITR selection).
- Modify `medusa/cassandra_utils.py` to inspect snapshot directory mtime **before** snapshot cleanup and record the node snapshot timestamp.
- Add `min_snapshot_time` and `max_snapshot_time` properties to `medusa/storage/cluster_backup.py`, aggregating `node_backup.snapshot_time` across all nodes (ignoring nodes where `snapshot_time is None` or returning `None` if absent).
- Must NOT change the serialized backup format or break existing `NodeBackup` deserialization.

**Test scenarios:**
- `NodeBackup.snapshot_time` returns the mtime-based value when the index blob is present.
- `NodeBackup.snapshot_time` returns `None` when the index blob is absent.
- `ClusterBackup.min_snapshot_time` is the minimum across all nodes.
- `ClusterBackup.max_snapshot_time` is the maximum across all nodes.

## Definition of Done (Acceptance Criteria)

- [ ] `NodeBackup.snapshot_time` property implemented (returns `None` when index blob / snapshot timestamp absent).
- [ ] `cassandra_utils.py` records snapshot directory mtime before cleanup.
- [ ] `ClusterBackup.min_snapshot_time` and `max_snapshot_time` implemented.
- [ ] Unit tests in `tests/storage_test.py` cover resolution paths (present and `None`).
- [ ] No changes to existing backup serialization format.
- [ ] All existing tests pass with no regressions.
- [ ] Linters pass cleanly.

## Proposed Implementation (Starting Point)

- In `node_backup.py`, add a `@property snapshot_time` that reads `_snapshot_time` if set (populated from the index blob loaded by `storage/__init__.py` in T05), else returns `None`.
- In `cassandra_utils.py`, capture the mtime of `snapshot_dir` before calling `cleanup_snapshot()` and store it on the backup metadata object.
- In `cluster_backup.py`, iterate `self.node_backups.values()` for the two aggregation properties.
