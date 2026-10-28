# PITR-08: Implement PreparePitrRestore RPC

- **Type:** Feature
- **Size:** S (2-5 ideal days)
- **Estimated Ideal Days:** 3 days (Est. Calendar Time: 6-8 days)
- **Parallel Track:** Track E - Restore
- **Blocked By / Dependencies:** PITR-05, PITR-07
- **Blocks:** PITR-12, PITR-13, PITR-14

---

## Purpose
> WHY are we doing this work? What problem does it solve and what value does it deliver?

Before executing a point-in-time recovery, Kubernetes restore operators need a coordinator RPC to validate cluster-wide restore feasibility. `PreparePitrRestore` resolves the required base snapshot for every node in the cluster and validates that all nodes have an eligible backup before any pod is restarted or wiped. This ensures fast failure and prevents destructive partial restores.

## Requirements
> WHAT needs to be true for this work to be successful? (Bulleted list)

- Add `PreparePitrRestore(request, context)` handler to `MedusaService` in `medusa/service/grpc/server.py`.
- Parse incoming `request.targetTimestamp`:
  - Support Unix epoch numeric string (e.g. `"1721000000.0"` or `"1721000000"`).
  - Support ISO-8601 strings (e.g. `"2024-07-15T12:30:00Z"`, `"2024-07-15T12:30:00+00:00"`).
  - Treat ISO-8601 strings without explicit timezone offset as UTC.
  - Convert parsed timestamp into a Unix epoch float in seconds (`target_timestamp_s`).
  - If timestamp parsing fails, log error and return response with `status = medusa_pb2.StatusType.FAILED`.
- Perform pre-flight per-node base snapshot resolution:
  - Node set: the keys of `node_backups` of the most recent complete cluster backup (`storage.list_cluster_backups()`, filter `is_complete()`, newest by `started`). If there is no complete cluster backup, return `FAILED`. Nodes seen only in older backups are ignored.
  - For each node FQDN, call `medusa.pitr_restore.select_node_base_snapshot(storage, fqdn, target_timestamp_s)`.
  - Assemble mapping of `{fqdn: backup_name}`.
  - If any node has no eligible base snapshot with `node_backup.snapshot_time <= target_timestamp_s` (or `select_node_base_snapshot` raises `ValueError`), immediately return `status = medusa_pb2.StatusType.FAILED` (Scenario SC5).
- Build success response:
  - Set `status = medusa_pb2.StatusType.SUCCESS`.
  - Populate `nodeBaseBackups` map with `{fqdn: backup_name}` for all nodes in the cluster.
  - Populate `targetTimestampS = target_timestamp_s` (echo of parsed timestamp as epoch float).
- On an unexpected exception: logging.exception, context.set_code(grpc.StatusCode.INTERNAL), context.set_details(...), and return a response with status FAILED (same style as PrepareRestore).
- Explicit operational constraints:
  - RPC MUST be idempotent and read-only against object storage.
  - RPC MUST NOT write any files to disk.
  - RPC MUST NOT list or transfer commitlog segment blobs.
- Unit tests in `tests/service/grpc/server_test.py`:
  - Test valid ISO-8601 UTC timestamp and epoch timestamp requests returning SUCCESS with complete `nodeBaseBackups`.
  - Test invalid timestamp formatting returning FAILED.
  - Test SC5 behavior: return FAILED when one or more nodes lack a base backup with `snapshot_time <= target_timestamp_s`.
  - Test rejection of pre-PITR backups (`snapshot_time is None`).
  - Test no complete cluster backup returns FAILED.
  - Test a node only in an older backup is ignored.
  - Test unexpected exception sets `INTERNAL` and FAILED.

## Definition of Done (Acceptance Criteria)
> How will we know we did the work right?

Plain-English deliverables and verification checklist:
- [ ] `PreparePitrRestore` RPC implemented on `MedusaService` in `medusa/service/grpc/server.py`.
- [ ] Timestamp parser handles ISO-8601 and epoch formats with UTC default.
- [ ] SC5 fast-failure verified when any cluster node lacks an eligible base backup.
- [ ] Idempotent, read-only behavior verified with zero disk writes.
- [ ] Unit test coverage in `tests/service/grpc/server_test.py` >= 80% for new logic.
- [ ] Linters and CI pass cleanly.

## Proposed Implementation (Optional / Starting Point)
> Suggested technical approach as a non-prescriptive starting point.

- Implement `PreparePitrRestore` in [`MedusaService`](../../../medusa/service/grpc/server.py:127) within `medusa/service/grpc/server.py`.
- Use `dateutil.parser.parse` or `datetime.fromisoformat` to parse ISO-8601 timestamps and convert to epoch float.
- Call `select_node_base_snapshot` from [`medusa/pitr_restore.py`](../../../medusa/pitr_restore.py) using `Storage(config=self.storage_config)`.
- Map response fields defined in [`medusa/service/grpc/medusa.proto`](../../../medusa/service/grpc/medusa.proto) (`PreparePitrRestoreResponse`).
- Add tests in [`tests/service/grpc/server_test.py`](../../../tests/service/grpc/server_test.py).
