# Design Doc Review: Point-in-Time Recovery (PITR) for Cassandra Medusa

> Reviewed: 2025-07-16
> Doc: `docs/superpowers/specs/pitr-design-doc.md`

---

## Summary

This is a high-quality, detailed design doc. The architecture is sound, the failure modes table is comprehensive, and the
integration with the existing `docker-entrypoint.sh` guard is well-thought-out. The most critical finding is a **contract
mismatch** between the proposed `restore.py` design and the existing code: the current `restore_backup()` hard-codes
`os.environ["BACKUP_NAME"]` as the backup source, but the PITR design requires per-node backup names from
`RESTORE_MAPPING["pitr"]["backup_name"]` — and the proposed pseudocode in §4.8 shows the lookup *before* calling
`restore_backup()` but never shows `restore_backup()` receiving it as a parameter. As written, `restore_backup()` would
ignore the PITR-assigned per-node backup and blindly use `BACKUP_NAME`. This will likely need a parameter addition or
`os.environ` mutation before `restore_backup()` is called. The doc is close to implementation-ready but requires a few
clarifications before work begins.

---

## Phase 1 — Structural Findings

### Gaps

- **G1 — No specified behaviour when no segments exist for a node.** A node with no commitlog segments in the window
  `[snapshot_time, target_timestamp]` is a valid scenario (e.g. a read-only node, or a node that had no writes since
  the base snapshot). The doc does not state what `apply_pitr_restore()` should do in this case: skip writing
  `commitlog_archiving.properties` entirely, write it with an empty `restore_directories`, or write it and let Cassandra
  start normally without replay. Leaving this unspecified risks inconsistent node behaviour in a valid production case.

- ~~**G2 — No specification for cross-DC PITR.**~~ **Resolved:** Medusa is a per-DC backup/restore tool by design and
  does not reason about multi-DC coordination. PITR follows the same pattern: each DC is handled independently.
  Operators in multi-DC deployments run `PreparePitrRestore` once per DC. No additional specification is needed.

- ~~**G3 — No staging directory capacity pre-flight check.** `commitlog_staging_dir` defaults to `/medusa-commitlog-staging`
  on an `emptyDir` volume.~~ **Resolved:** `commitlog_staging_dir` defaults to `/var/lib/cassandra/medusa-commitlog-staging`
  located directly on the persistent Cassandra data volume. Because the volume is wiped prior to restore, it has ample
  capacity for both the base snapshot and the commitlog replay segments. `restore.py` systematically empties the staging
  folder on startup, preventing orphaned segments and avoiding `emptyDir` exhaustion issues.

- **G4 — No Cassandra version/configuration compatibility check at restore time.** The design does not specify whether
  commitlog segments from one Cassandra version are replayable on a different version, or whether segment size changes
  between backup and restore are safe. No pre-restore validation is proposed.

- **G5 — Success Criterion SC4 is not falsifiable as stated.** "Existing test suite passes unchanged when PITR config
  is absent" is not a testable criterion for the feature itself — it is a regression guard. The doc should clarify how
  SC4 is run in CI (e.g. run without a `[pitr]` section and assert no new failures).

### Inconsistencies

- **I1 — OQ5 resolution references the wrong section.** Line 617: "Documented as an explicit assumption in **§4.10**"
  — §4.10 is "Proto Changes". Purge scope is documented in **§4.9**. The section reference is off by one.

- **I2 — OQ8 resolution references the wrong section.** Line 623: "~~§4.11 proto still names
  `RestoreClusterToTimestamp`~~" — §4.11 is "Object Storage Layout", not Proto. Proto definitions are in §4.10. The
  resolved note says "§4.11 and §6.5 now consistently use `PreparePitrRestore`" but §4.11 contains no proto at all.

- **I3 — `pending_count` semantics are contradictory under directory-read failure.** §6.4 failure table states that a
  runtime directory read error during a poll produces `pending_count=0, running=true`. §6.4 also states that spool
  exhaustion causes `pending_count` to rise monotonically on upload failures. If the spool cannot be read, reporting
  `pending_count=0` is misleading — it is indistinguishable from "empty spool." The doc should specify a distinct
  status (e.g. `status=FAILED` or an error field) when the spool directory itself is unreadable, rather than silently
  returning 0.

### Inaccuracies

- ~~**A1 — `restore_backup()` ignores per-node backup name from `RESTORE_MAPPING["pitr"]`.**~~ **Resolved:** `restore_backup()` gains a `backup_name=None` parameter; when `None` it falls back to `os.environ["BACKUP_NAME"]`, preserving backward compatibility. `__main__` extracts `mapping.get("pitr", {}).get("backup_name", None)` from `RESTORE_MAPPING` and passes it explicitly. `RESTORE_MAPPING` is also parsed before calling `restore_backup()` (not after), eliminating the redundant `json.loads` call. §4.8 pseudocode updated accordingly.

- **A2 — `grpcio` is in an optional dependency group, not a main dependency.** OQ2 resolution (line 611) states "all
  storage backend packages are main (non-optional) dependencies in `pyproject.toml`." This is true for `boto3`,
  `azure-storage-blob`, and `gcloud-aio-storage` (all in `[tool.poetry.dependencies]`). However, `grpcio` and
  `grpcio-tools` are in `[tool.poetry.group.grpc.dependencies]` and `[tool.poetry.group.grpc-runtime.dependencies]`
  (lines 87–95 of `pyproject.toml`) — these are *optional dependency groups*, not the default install. The OQ resolution
  is correct for storage backends but should not be generalised to gRPC deps without noting the group distinction.
  (This does not affect PITR correctness since the sidecar always installs the grpc group, but the claim in the doc is
  imprecise.)

---

## Phase 2 — Assumption & Dependency Verification

### Verified Assumptions

- `AbstractBlob` has a `last_modified` field — ✅ confirmed at [`medusa/storage/abstract_storage.py:39`](../../../medusa/storage/abstract_storage.py)
- `AbstractBlob` has a `size` field — ✅ confirmed at [`medusa/storage/abstract_storage.py:39`](../../../medusa/storage/abstract_storage.py)
- `RESTORE_MAPPING` is read from `os.environ` via `json.loads()` in `apply_mapping_env()` — ✅ confirmed at [`medusa/service/grpc/restore.py:55-57`](../../../medusa/service/grpc/restore.py)
- `RESTORE_KEY` is passed as `sys.argv[2]` to `restore.py` by `docker-entrypoint.sh` — ✅ confirmed at [`k8s/docker-entrypoint.sh:44`](../../../k8s/docker-entrypoint.sh)
- `docker-entrypoint.sh` writes `.last-restore` at line 45 *after* `restore.py` exits — ✅ confirmed at [`k8s/docker-entrypoint.sh:45`](../../../k8s/docker-entrypoint.sh). (Minor note: the doc says "written by the operator"; it is actually written by the shell script, not the operator.)
- `StatusType` enum exists in `medusa.proto` with values `IN_PROGRESS=0, SUCCESS=1, FAILED=2, UNKNOWN=3` — ✅ confirmed at [`medusa/service/grpc/medusa.proto:21-26`](../../../medusa/service/grpc/medusa.proto)
- `MedusaConfig` is a `collections.namedtuple` and existing config sections are structured as namedtuples — ✅ confirmed at [`medusa/config.py:63-94`](../../../medusa/config.py); adding `pitr` follows the same `CONFIG_SECTIONS` pattern.
- `tenacity` is available for storage retry — ✅ declared at `pyproject.toml:62`
- `enable_md5_checks` lives in `ChecksConfig` — ✅ confirmed at [`medusa/config.py:55`](../../../medusa/config.py)
- `KubernetesConfig` is a parallel namedtuple to the proposed `PitrConfig` — ✅ confirmed at [`medusa/config.py:81-84`](../../../medusa/config.py)

### Unverified or Challenged Assumptions

- **`NodeBackup.snapshot_time` property** — ⚠️ does not yet exist. [`medusa/storage/node_backup.py`](../../../medusa/storage/node_backup.py) has no `snapshot_time` property. The doc correctly lists this as a new addition, but the fallback logic ("falls back to `started`") depends on the `started` property already existing — which it does (`self._started` at line 83). The new property and its index blob (`snapshot_time_{fqdn}_{timestamp}.timestamp`) must be added as specified before any other PITR code can work.

- **`ClusterBackup.min_snapshot_time` and `max_snapshot_time`** — ⚠️ do not yet exist. [`medusa/storage/cluster_backup.py`](../../../medusa/storage/cluster_backup.py) has no such properties. These are required by the purge threshold calculation (§4.9). The doc correctly flags them as new additions.

- **`medusa/index.py` persists `snapshot_time` index blobs** — ⚠️ does not yet exist. Current [`medusa/index.py`](../../../medusa/index.py) only writes `started_*` and `finished_*` timestamp blobs (lines 101–115). No `snapshot_time_*` blob is written. The design correctly identifies this as a new addition; the implementation must add this before `NodeBackup.snapshot_time` can resolve from storage.

- **`medusa/pitr_restore.py` exists** — ⚠️ not yet created. Confirmed absent. Correctly flagged as "Create" in the file map.

- **`medusa/service/grpc/commitlog_archiver.py` exists** — ⚠️ not yet created. Correctly flagged as "Create".

- **`purge_commitlogs()` exists in `medusa/purge.py`** — ⚠️ function does not yet exist. Confirmed absent from the file.

- ~~**`RESTORE_MAPPING` contains per-node backup name via `pitr` block`**~~ — **Resolved:** `RESTORE_MAPPING` is now parsed once, before `restore_backup()` is called, and the result is reused for both the `backup_name` extraction and the `apply_pitr_restore()` call. No second `json.loads` is needed.

- ~~**`restore_backup()` uses `backup_name` from `RESTORE_MAPPING["pitr"]["backup_name"]`**~~ — **Resolved:** see A1 above.

### Dependency Check

- `boto3` — ✅ declared at `1.40.41` in `pyproject.toml` (main dependencies)
- `azure-storage-blob` — ✅ declared at `12.17.0` in `pyproject.toml` (main dependencies)
- `gcloud-aio-storage` — ✅ declared at `9.6.4` in `pyproject.toml` (main dependencies)
- `tenacity` — ✅ declared at `9.1.2` in `pyproject.toml` (main dependencies)
- `grpcio` — ✅ declared at `1.81.0` in `pyproject.toml` but in the **`grpc`/`grpc-runtime` optional groups**, not main dependencies. The doc's OQ2 claim that all dependencies are "main (non-optional)" is imprecise for gRPC itself.
- `grpcio-tools` — ✅ declared at `1.81.0` in `[tool.poetry.group.grpc.dependencies]` (optional group)
- `AbstractStorage` abstraction — ✅ exists at [`medusa/storage/abstract_storage.py`](../../../medusa/storage/abstract_storage.py)

### Unverifiable (external systems)

- **Cassandra `commitlog_archiving.properties` parsing format `yyyy:MM:dd HH:mm:ss`** — requires manual verification against the Cassandra version deployed. The format is widely documented for Cassandra 3.x/4.x but is not verifiable from this codebase.
- **Cassandra `archive_command` finalization guarantee** (segment immutable after `archive_command` is called) — documented Cassandra behaviour, not verifiable from Medusa code.
- **Object storage `last_modified` timezone consistency** — S3/GCS/Azure may return `last_modified` in different timezone representations; all three are timezone-aware in practice, but cross-backend consistency should be validated manually.
- **`emptyDir` volume persistence across initContainer → main container boundary** — a Kubernetes infrastructure assumption; not verifiable from this codebase.

---

## Recommended Actions

1. ~~**(Blocker) Resolve the `restore_backup()` / per-node backup name interface gap (A1).** Either add a `backup_name` parameter to `restore_backup()` or explicitly mutate `os.environ["BACKUP_NAME"]` before calling it. Update §4.8 pseudocode to show the chosen approach explicitly.~~ **Resolved:** `backup_name=None` parameter added; §4.8 pseudocode updated.

2. **(Medium) Specify `apply_pitr_restore()` behaviour when no segments exist for a node (G1).** Document whether
   the function skips writing `commitlog_archiving.properties` (Cassandra starts normally from base SSTables),
   writes an empty `restore_directories`, or treats the case as an error. The chosen behaviour should be reflected
   in the unit tests.

3. **(High) Fix section references in OQ5 and OQ8 (I1, I2).** OQ5 should reference §4.9; OQ8 should reference §4.10.

4. **(High) Clarify `pending_count` semantics under directory-read failure (I3).** Specify a distinct error indicator
   when the spool directory is unreadable, so operators can distinguish "empty spool" from "unreadable spool."

5. **(Medium) Address per-node backup-name extraction from `RESTORE_MAPPING` (challenged assumption).** §4.8 pseudocode
   shows `mapping.get("pitr", {}).get("backup_name", ...)` before `restore_backup()` but the mapping is not returned
   by `apply_mapping_env()`. Either return the raw mapping from `apply_mapping_env()` or call
   `json.loads(os.environ["RESTORE_MAPPING"])` again explicitly in `__main__`.

6. ~~**(Medium) Clarify cross-DC PITR scope (G2).** Resolved — Medusa's per-DC model covers this.~~

7. **(Low) Add a staging directory capacity pre-flight check (G3).** Before downloading segments, estimate total
   download size (blob sizes available from `AbstractBlob.size`) and abort with a clear error if insufficient space.

8. **(Low) Clarify `grpcio` optional group vs main dependency in OQ2 resolution (A2).** Update OQ2 to note that
   storage backends are main deps but `grpcio` is an optional group installed in the gRPC image — the correctness
   argument still holds but the claim as written is imprecise.
