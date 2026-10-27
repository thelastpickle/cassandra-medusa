# T04: Implement pure PITR restore logic (`medusa/pitr_restore.py`)

- **Type:** Feature
- **Size:** S (2–5 ideal days)
- **Estimated Ideal Days:** 2 days (Est. Calendar Time: 4 days)
- **Parallel Track:** D – Restore Logic
- **Blocked By / Dependencies:** T01 (PitrConfig)
- **Blocks:** T06

---

## Purpose

`medusa/pitr_restore.py` is the pure decision layer for PITR restore: it selects the correct base snapshot, identifies which commitlog segments to download, and generates the `commitlog_archiving.properties` file content. It has no gRPC or SSH dependencies, making it independently testable and reusable across both the gRPC server and the initContainer entrypoint.

## Requirements

- Create `medusa/pitr_restore.py` with the following four functions:

### `select_node_base_snapshot(storage, fqdn: str, target_timestamp_s: float) -> NodeBackup`
- Returns the most recent `NodeBackup` for `fqdn` where `node_backup.snapshot_time ≤ target_timestamp_s`.
- Ignores/rejects backups with `snapshot_time is None` (pre-PITR backups lacking `snapshot_time` index blob).
- Raises `ValueError` when no valid PITR backup exists with `snapshot_time ≤ target_timestamp_s`.

### `find_next_backup(storage, fqdn: str, base_backup: NodeBackup) -> NodeBackup | None`
- Returns the earliest `NodeBackup` for `fqdn` whose `snapshot_time > base_backup.snapshot_time`.
- Returns `None` if no such backup exists.

### `find_commitlog_segments(storage_driver, prefix_path, fqdn, after_snapshot_time_s: float, safety_margin_s: float = 30.0, upper_bound_s: float | None = None) -> List[str]`
- Returns all storage paths for this node with `blob.last_modified ≥ (after_snapshot_time_s - safety_margin_s)`.
- When `upper_bound_s` is set, also filters `blob.last_modified ≤ upper_bound_s`.
- Results sorted ascending by filename.
- `None` upper bound means no upper bound applied.

### `generate_commitlog_archiving_properties(archive_command: str, restore_directories: str | None = None, target_timestamp_s: float | None = None) -> str`
- Always includes `archive_command`.
- When both `restore_directories` and `target_timestamp_s` are provided, appends `restore_directories` and `restore_point_in_time` formatted as `%Y:%m:%d %H:%M:%S` UTC.
- When those args are omitted, outputs archiving settings only.
- `target_timestamp_s` to UTC: `datetime.datetime.fromtimestamp(target_timestamp_s, tz=datetime.timezone.utc).strftime("%Y:%m:%d %H:%M:%S")`.

**Constraints:**
- No gRPC, SSH, or subprocess imports.
- No side effects (no file writes, no directory creation).
- Must be importable without a running gRPC server.

**Test scenarios (in `tests/pitr_restore_test.py`):**
- `select_node_base_snapshot`: correct backup selected when multiple exist with varying `snapshot_time`.
- `select_node_base_snapshot`: raises `ValueError` when no backup has `snapshot_time ≤ target`.
- `select_node_base_snapshot`: raises `ValueError` when `snapshot_time` is `None` / absent (ignores pre-PITR backups without `snapshot_time`).
- `find_commitlog_segments`: blob `last_modified` filtering with safety margin applied correctly.
- `find_commitlog_segments`: `upper_bound_s` excludes blobs modified after it.
- `find_commitlog_segments`: results sorted ascending by filename.
- `generate_commitlog_archiving_properties`: archive-only output when restore args omitted.
- `generate_commitlog_archiving_properties`: full restore output with correct UTC timestamp format.
- `generate_commitlog_archiving_properties`: epoch `0` formatted correctly.

## Definition of Done (Acceptance Criteria)

- [ ] `medusa/pitr_restore.py` created with all four functions.
- [ ] No gRPC/SSH/subprocess imports in the file.
- [ ] Unit tests in `tests/pitr_restore_test.py` cover all scenarios above (≥ 80% coverage).
- [ ] Linters pass cleanly.

## Proposed Implementation (Starting Point)

- `find_commitlog_segments` calls `storage.list_objects(f"{prefix_path}/{fqdn}/commitlogs/")` and filters by `blob.last_modified`.
- `generate_commitlog_archiving_properties` builds a list of `key=value` lines and joins with `\n`.
- Keep each function ≤ 30 lines; no class needed.
