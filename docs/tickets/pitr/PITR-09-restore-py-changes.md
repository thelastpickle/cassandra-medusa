# PITR-09: Update restore.py for Per-Node Backup Name and Mapping

- **Type:** Feature
- **Size:** S (2-5 ideal days)
- **Estimated Ideal Days:** 3 days (Est. Calendar Time: 6-8 days)
- **Parallel Track:** Track E - Restore
- **Blocked By / Dependencies:** None
- **Blocks:** PITR-10

---

## Purpose
> WHY are we doing this work? What problem does it solve and what value does it deliver?

In point-in-time recovery restores, nodes in a cluster may restore from different base backups resolved during pre-flight. `restore.py` must support receiving per-node `backup_name` and full parsed mapping structures from `RESTORE_MAPPING` while preserving strict backward compatibility for standard cluster restores that rely on the `BACKUP_NAME` environment variable.

## Requirements
> WHAT needs to be true for this work to be successful? (Bulleted list)

- Modify `apply_mapping_env()` in `medusa/service/grpc/restore.py`:
  - Return `tuple[bool, dict] | None` representing `(in_place, mapping)` instead of `bool | None`.
  - When `RESTORE_MAPPING` environment variable is present, parse it with `json.loads()` and return `(mapping["in_place"], mapping)`.
  - When `RESTORE_MAPPING` environment variable is absent, return `(True, {})`.
  - When `in_place` is `False` and `POD_NAME` is missing from `mapping["host_map"]`, return `None` (preserving existing skip behavior).
  - Preserve existing `POD_IP` environment variable injection for remote restores.
- Modify `restore_backup()` in `medusa/service/grpc/restore.py`:
  - Update function signature to accept optional parameters: `def restore_backup(in_place, config, backup_name=None, mapping=None):`.
  - If `backup_name` is `None` and `mapping` contains a `"pitr"` dictionary:
    - Resolve the local node's FQDN:
      - If `POD_NAME` is set in `os.environ` and present in `mapping.get("host_map", {})`, use `mapping["host_map"][os.environ["POD_NAME"]]["source"][0]`.
      - Else if `POD_IP` is set in `os.environ`, use `os.environ["POD_IP"]`.
      - Else fallback to `config.storage.fqdn`.
    - Retrieve assigned `backup_name` from `mapping["pitr"]["node_backups"][fqdn]`.
  - If `backup_name` is `None` and `mapping` does NOT contain a `"pitr"` dictionary:
    - Fallback to `os.environ["BACKUP_NAME"]` (guarantees backward compatibility for non-PITR restores).
  - Execute existing SSTable restore flow via `medusa.restore_node.restore_node()` with the resolved `backup_name`.
  - Move the FQDN rule into a module-level function in `restore.py`:
    `def resolve_node_fqdn(mapping: dict, config) -> str:` (same body as in design doc 4.8). `restore_backup()` calls it. PITR-10 imports it. Do not duplicate it.
  - If the resolved FQDN is not a key of `mapping["pitr"]["node_backups"]`, raise `ValueError(f"No PITR base backup for node {fqdn}")`. Do not fall back to `BACKUP_NAME`.
- Update `__main__` entrypoint in `medusa/service/grpc/restore.py`:
  - Call `mapping_result = apply_mapping_env()`.
  - If `mapping_result is not None`:
    - Unpack `in_place, mapping = mapping_result`.
    - Call `restore_backup(in_place, config, mapping=mapping)`.
- Update and extend unit tests in `tests/service/grpc/restore_test.py`:
  - Update existing test cases (`test_restore_inplace`, `test_restore_remote`, `test_restore_no_match`) for new return signature `(in_place, mapping)`.
  - Add test verifying `restore_backup` resolves `backup_name` from `mapping["pitr"]["node_backups"]` when `POD_NAME` is in `host_map`.
  - Add test verifying `restore_backup` resolves `backup_name` using `POD_IP` and `config.storage.fqdn` fallbacks.
  - Add test verifying `restore_backup` falls back to `os.environ["BACKUP_NAME"]` when no PITR block is present.
  - `restore_backup` raises `ValueError` when the FQDN is missing from `node_backups`; add test for `resolve_node_fqdn` (three cases).

## Definition of Done (Acceptance Criteria)
> How will we know we did the work right?

Plain-English deliverables and verification checklist:
- [ ] `apply_mapping_env()` returns `(in_place, mapping)` tuple or `None`.
- [ ] `restore_backup()` supports `backup_name` and `mapping` parameters.
- [ ] FQDN resolution handles `POD_NAME` / `host_map`, `POD_IP`, and `config.storage.fqdn`.
- [ ] Non-PITR backward compatibility with `BACKUP_NAME` env var verified.
- [ ] resolve_node_fqdn is defined once in restore.py.
- [ ] Unit test coverage in `tests/service/grpc/restore_test.py` >= 80% for new logic.
- [ ] Linters and CI pass cleanly.

## Proposed Implementation (Optional / Starting Point)
> Suggested technical approach as a non-prescriptive starting point.

- Update [`apply_mapping_env`](../../../medusa/service/grpc/restore.py:52) and [`restore_backup`](../../../medusa/service/grpc/restore.py:80) in `medusa/service/grpc/restore.py`.
- Update `__main__` caller in [`medusa/service/grpc/restore.py`](../../../medusa/service/grpc/restore.py:106).
- Update and add test cases in [`tests/service/grpc/restore_test.py`](../../../tests/service/grpc/restore_test.py:20).
