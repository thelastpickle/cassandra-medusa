# PITR-07: Implement Pure PITR Restore Logic

- **Type:** Feature
- **Size:** S (2-5 ideal days)
- **Estimated Ideal Days:** 4 days (Est. Calendar Time: 8-10 days)
- **Parallel Track:** Track E - Restore
- **Blocked By / Dependencies:** PITR-03
- **Blocks:** PITR-08, PITR-10

---

## Purpose
> WHY are we doing this work? What problem does it solve and what value does it deliver?

Point-in-time recovery requires algorithms to select base snapshots, discover archived commitlog segments, download segments, and generate Cassandra archiving properties. Implementing these calculations in a standalone pure logic module (`medusa/pitr_restore.py`) separates decision logic from gRPC and SSH execution runtimes. This guarantees clean unit testability, deterministic behavior, and safe execution across both coordinator RPCs and node initContainers.

## Requirements
> WHAT needs to be true for this work to be successful? (Bulleted list)

- Create `medusa/pitr_restore.py` containing five pure functions with no gRPC or SSH runtime dependencies:
  1. `select_node_base_snapshot(storage, fqdn: str, target_timestamp_s: float) -> NodeBackup`:
     - List all node backups for the given `fqdn`.
     - Filter out any backup where `snapshot_time is None` (pre-PITR backups lacking commitlogs).
     - Filter out any backup where `snapshot_time > target_timestamp_s`.
     - Select the eligible backup with the latest `snapshot_time <= target_timestamp_s`.
     - Explicit anti-requirement: MUST NOT fall back to `started` timestamp when `snapshot_time` is missing or invalid.
     - Raise `ValueError` if no eligible base backup exists.
  2. `find_next_backup(storage, fqdn: str, base_backup: NodeBackup) -> NodeBackup | None`:
     - List all node backups for `fqdn`.
     - Return the backup with the smallest `snapshot_time > base_backup.snapshot_time`.
     - Return `None` if no subsequent backup exists.
     - Skip backups whose snapshot_time is None (they cannot be compared). Never raise TypeError.
  3. `find_commitlog_segments(storage: Storage, prefix_path: str, fqdn: str, after_snapshot_time_s: float, upper_bound_s: float, safety_margin_s: float = 30.0) -> List[AbstractBlob]`:
     - List all commitlog blobs for `fqdn` under `<prefix_path>/<fqdn>/commitlogs/`.
     - Filter blobs where `blob.last_modified >= (after_snapshot_time_s - safety_margin_s)` AND `blob.last_modified <= upper_bound_s`.
     - Require `upper_bound_s` as a mandatory parameter.
     - Sort segments ascending by filename (lexicographical order).
     - `blob.last_modified` can be a `datetime` or a number depending on the driver. Convert it with `storage.storage_driver.get_object_datetime(blob)` and `.timestamp()` before comparing to the float bounds.
     - `after_snapshot_time_s` is the lower bound. Callers pass `base_backup.started` (design doc 4.5 "Why the latest mtime").
  4. `download_commitlog_segments(storage: Storage, segments: List[AbstractBlob], destination_dir: Path | str) -> None`:
     - Download each segment blob into `destination_dir`.
     - Overwrite existing files with matching names.
  5. `generate_commitlog_archiving_properties(archive_command: str, restore_directories: str | None = None, target_timestamp_s: float | None = None) -> str`:
     - Return configuration text for `commitlog_archiving.properties`.
     - Always output `archive_command=<archive_command>`.
     - When `restore_directories` and `target_timestamp_s` are provided, output `restore_directories=<restore_directories>` and `restore_point_in_time=<formatted_timestamp>`.
     - Format `restore_point_in_time` in UTC using `%Y:%m:%d %H:%M:%S` format (`datetime.datetime.fromtimestamp(target_timestamp_s, tz=datetime.timezone.utc).strftime("%Y:%m:%d %H:%M:%S")`).
     - When `restore_directories` or `target_timestamp_s` is omitted, output archiving settings only.
- Implement comprehensive unit tests in `tests/pitr_restore_test.py`:
  - `select_node_base_snapshot` with eligible backups, backups after target timestamp, backups with `snapshot_time=None`, and empty lists.
  - `find_next_backup` when a next backup exists and when the base backup is the newest.
  - `find_next_backup` ignores a backup with `snapshot_time=None`.
  - `find_commitlog_segments` boundary conditions, safety margin inclusion, upper bound cutoff, and filename ordering.
  - `find_commitlog_segments` works with `last_modified` as a `datetime` and as a number.
  - `download_commitlog_segments` blob download and file overwrite.
  - `generate_commitlog_archiving_properties` with restore parameters (verifying exact `%Y:%m:%d %H:%M:%S` UTC format) and with archiving-only mode.

## Definition of Done (Acceptance Criteria)
> How will we know we did the work right?

Plain-English deliverables and verification checklist:
- [ ] `medusa/pitr_restore.py` implemented with all five core functions.
- [ ] UTC timestamp formatting matches Cassandra `%Y:%m:%d %H:%M:%S` parser expectations.
- [ ] Pre-PITR backups (`snapshot_time=None`) rejected without fallback to `started`.
- [ ] Unit test suite implemented in `tests/pitr_restore_test.py` with >= 80% coverage on new logic.
- [ ] Linters and CI pass cleanly.

## Proposed Implementation (Optional / Starting Point)
> Suggested technical approach as a non-prescriptive starting point.

- Implement pure logic in [`medusa/pitr_restore.py`](../../../medusa/pitr_restore.py).
- Reference storage model in [`medusa/storage/abstract_storage.py`](../../../medusa/storage/abstract_storage.py:1) and `NodeBackup` fields from PITR-03.
- Use `datetime.timezone.utc` for all datetime conversions.
- Add test coverage in [`tests/pitr_restore_test.py`](../../../tests/pitr_restore_test.py).
