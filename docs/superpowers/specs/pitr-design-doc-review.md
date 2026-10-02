# Design Doc Review: Point-in-Time Recovery (PITR) for Cassandra Medusa

> Reviewed: 2026-10-02
> Doc: `docs/superpowers/specs/pitr-design-doc.md`

## Summary

The doc is thorough and well-structured. Most open questions have been resolved, the alternative analysis is solid, and the failure-mode table is complete. Two **blockers** require author action before implementation begins: (1) the `restore.py` extension assumes a "restore marker file" guard mechanism that does not exist in the current codebase — the entire idempotency claim for `apply_pitr_restore()` rests on this non-existent guard; (2) the `MedusaRestoreMapping` Go struct in the k8ssandra-operator has no `pitr` field, so the operator cannot marshal the PITR block into `RESTORE_MAPPING` without a code change to the operator that is not mentioned in the design. Several secondary gaps and inaccuracies also warrant revision.

---

## Phase 1 — Structural Findings

### Gaps

- **Operator-side changes not scoped.** §4.7 and §4.8 describe the operator assembling a `RESTORE_MAPPING` JSON with a `pitr` block, but no file map entry (§4.2 / §4.9) lists any change to the k8ssandra-operator. The operator's `MedusaRestoreMapping` struct and its `setRestoreMappingInRestoreContainer` function need to be extended; this work is completely absent from scope.

- **No definition of the staging directory path.** §4.8 says segments are downloaded to "a local staging directory", but the exact path is never specified (e.g. `/medusa-commitlog-staging`). `generate_commitlog_archiving_properties()` needs to know this path to write `restore_directories`; without a defined default, implementers must guess.

- **`commitlog_archiving.properties` write path contradicts itself.** §4.8 body says `apply_pitr_restore()` writes the file to `Path(cassandra_config.config_file).parent`, but the same section later says "the operator passes the properties content as a field in the `pitr` block of `RESTORE_MAPPING` and `apply_pitr_restore()` materialises it" — and the resolved OQ3 footnote (§7) says Medusa does *not* write the file at all (the k8ssandra-operator injects it via config-builder). The doc presents three different answers to the same question. See Inconsistencies below.

- **No backward-compatibility plan for the proto change.** Two new RPCs and five new message types are added to `medusa.proto`. There is no mention of proto versioning, generated-file regeneration workflow, or how existing k8ssandra-operator versions that do not have `PreparePitrRestore` in their client interface behave.

- **`server_test.py` test description is wrong.** §6.5 says `PreparePitrRestore` "writes correct metadata to disk" — but the design explicitly states no data is written to disk by this RPC. This is a copy-paste error that will mislead the test implementer.

- **No acceptance condition for `GetCommitLogArchiveStatus`.** SC1–SC7 in §2 cover archiver, restore, config, and purge, but there is no success criterion for the `GetCommitLogArchiveStatus` RPC (running=false with no archiver, correct `pending_count` under load, etc.).

### Inconsistencies

- **`apply_pitr_restore()` properties-file authorship is contradicted three times.** §4.8 first paragraph says `apply_pitr_restore()` writes the file to `Path(cassandra_config.config_file).parent`; the same section's guard paragraph says the function "writes the properties file"; but OQ3 resolution in §7 says "Medusa does not write `commitlog_archiving.properties`" and the operator injects it via cass-operator's config-builder. The doc cannot be implemented consistently from these three statements.

- **`PreparePitrRestore` return value vs. operator assembly.** §4.7 says the RPC "returns the base backup name and a per-node PITR metadata map in the response" and the operator assembles the full JSON. §4.8 says the operator "passes the properties content as a field in the `pitr` block of `RESTORE_MAPPING`" (implying the *generated properties file content* is embedded in the JSON). These are two different contracts: one passes raw segment lists and the other passes a pre-rendered properties string. The proto definition (§4.11) only has `segments` in `NodePitrMetadata` — no `commitlog_archiving_properties` field — which is consistent with the first description but not the second.

- **`drain_spool` step 2 says "removes hardlink on successful upload OR confirmed-present skip", but step 3 says "on upload failure: leaves the hardlink in place".** This is consistent internally, but the description of step 1 says size-match = skip *and* remove. It is not clear whether an MD5-verified skip also removes the hardlink immediately (it should, but is not stated).

### Inaccuracies

- **`Server.serve()` line reference is wrong.** §4.4 references `Server.serve()` at line 66 of `server.py`. The actual `serve` method starts at line 66, but that is the correct line — however, the method signature and body show no archiver lifecycle code at all today. Referencing it as if it already contains archiver integration misleads readers into thinking less work is needed.

- **`server_test.py` test description says `PreparePitrRestore` "writes correct metadata to disk"** (§6.5). The entire design premise is that this RPC writes nothing to disk (it returns inline data). This is a factual inaccuracy in the test spec.

- **§4.10 purge formula uses `pitr.interval_seconds`** but §4.3 names the config key `commitlog_archive_interval_seconds`. The variable name in the formula must match the config key exactly, or implementations will use the wrong value.

---

## Phase 2 — Assumption & Dependency Verification

### Verified Assumptions

- `AbstractBlob.last_modified` exists — ✅ confirmed at [`abstract_storage.py:39`](../../../medusa/storage/abstract_storage.py:39): `AbstractBlob = collections.namedtuple('AbstractBlob', ['name', 'size', 'hash', 'last_modified', 'storage_class'])`.
- `list_blobs()` is available on all storage backends — ✅ confirmed at [`abstract_storage.py:81`](../../../medusa/storage/abstract_storage.py:81); all concrete drivers implement `_list_blobs`.
- `ClusterBackup.started` returns the minimum `started` across node backups — ✅ confirmed at [`cluster_backup.py:39`](../../../medusa/storage/cluster_backup.py:39).
- `restore_node_locally()` calls `clean_path(cassandra.commit_logs_path, ...)` — ✅ confirmed at [`restore_node.py:100`](../../../medusa/restore_node.py:100); commitlog directory is wiped before data placement.
- `boto3`, `gcloud-aio-storage`, `azure-storage-blob` are non-optional main dependencies — ✅ confirmed at [`pyproject.toml:73–81`](../../../pyproject.toml:73).
- `PrepareRestore` writes to disk (not inline) — ✅ confirmed at [`server.py:340–342`](../../../medusa/service/grpc/server.py:340); it writes to `RESTORE_MAPPING_LOCATION = "/var/lib/cassandra/.restore_mapping"` on disk.
- `MedusaConfig` is a namedtuple with no `pitr` field — ✅ confirmed at [`config.py:63–69`](../../../medusa/config.py:63). `pitr` is absent; it must be added.
- `CONFIG_SECTIONS` dict does not include `'pitr'` — ✅ confirmed at [`config.py:86–95`](../../../medusa/config.py:86). The key must be added for `load_config` to parse the section.

### Unverified or Challenged Assumptions

- **Restore marker file guard** — ⚠️ The doc (§4.8) states that `apply_pitr_restore()` "checks for the restore marker file (the same file the existing restore guard uses to detect a completed restore)". No such marker file mechanism exists anywhere in the current `restore.py`, `restore_node.py`, or any related module. There is no "restore guard" file in the codebase today. The idempotency guarantee for `apply_pitr_restore()` is entirely unfounded in existing code; the mechanism must be designed from scratch, not inherited.

- **`RESTORE_MAPPING` is pure inline JSON with a `pitr` block** — ⚠️ The k8ssandra-operator's `MedusaRestoreMapping` Go struct at [`medusarestorejob_types.go:77–84`](../../../k8ssandra-operator/apis/medusa/v1alpha1/medusarestorejob_types.go:77) has only `InPlace` and `HostMap`. There is no `pitr` field. `setRestoreMappingInRestoreContainer` marshals that struct to JSON — the resulting `RESTORE_MAPPING` env var cannot contain a `pitr` block without adding the field to the struct. The operator-side change is a prerequisite for the `restore.py` PITR code to ever find `mapping["pitr"]`.

- **`restore.py` reads the full RESTORE_MAPPING and exposes the `pitr` block** — ⚠️ Current `apply_mapping_env()` at [`restore.py:52–77`](../../../medusa/service/grpc/restore.py:52) parses `RESTORE_MAPPING` but only acts on `in_place` and `host_map` keys. The `pitr` block is silently ignored. The doc assumes the block is available to `apply_pitr_restore()` via the same call, but the function must be modified to return or expose it.

- **`purge.main()` has access to `pitr` config and `fqdn`** — ⚠️ `purge.main()` at [`purge.py:30`](../../../medusa/purge.py:30) uses `config.storage.fqdn` and has access to the storage context, so `purge_commitlogs()` can be hooked in. However, `config.pitr` does not yet exist (see verified assumption above), so the call will raise `AttributeError` until `MedusaConfig` and `CONFIG_SECTIONS` are updated.

- **`commitlog_archiving.properties` location** — ⚠️ The doc says the file is written to `Path(cassandra_config.config_file).parent`. The default value of `config_file` is `/etc/cassandra/cassandra.yaml` (verified at [`cassandra_utils.py:236`](../../../medusa/cassandra_utils.py:236)), so the default target is `/etc/cassandra/`. In Kubernetes with a read-only root filesystem and a `server-config` `emptyDir`, write permission at that path is only guaranteed if the initContainer explicitly mounts that volume there. The OQ3 resolution (§7) contradicts this by saying Medusa does not write the file at all. This conflict must be resolved before implementation.

### Dependency Check

- `AbstractBlob` — ✅ declared in [`medusa/storage/abstract_storage.py:39`](../../../medusa/storage/abstract_storage.py:39)
- `tenacity` (retry in storage driver) — ✅ declared at `9.1.2` in [`pyproject.toml:62`](../../../pyproject.toml:62)
- `grpcio` — ✅ declared at `1.81.0` in `[tool.poetry.group.grpc.dependencies]` at [`pyproject.toml:89`](../../../pyproject.toml:89)
- `protobuf` — ✅ declared at `6.33.6` in [`pyproject.toml:88`](../../../pyproject.toml:88)
- `grpcio-tools` (for proto regeneration) — ✅ declared at `1.81.0` in [`pyproject.toml:91`](../../../pyproject.toml:91)
- `aiofiles` — ✅ declared at `23.2.1` in [`pyproject.toml:77`](../../../pyproject.toml:77)
- `MedusaRestoreMapping` (operator struct, needs `pitr` field) — ❌ struct exists at [`k8ssandra-operator/apis/medusa/v1alpha1/medusarestorejob_types.go:77`](../../../k8ssandra-operator/apis/medusa/v1alpha1/medusarestorejob_types.go:77) but has no `pitr` field; operator changes are not in scope in the doc's file map

### Unverifiable (external systems)

- **Cassandra `archive_command` hardlink semantics** — Cassandra fires `archive_command` after final fsync and before the segment is eligible for deletion. Verified by doc's own analysis; cannot be verified from this codebase.
- **`restore_point_in_time` in `commitlog_archiving.properties`** — Cassandra replay filters by mutation timestamp, not wall clock. Described in §4.8; cannot be verified from this codebase.
- **`emptyDir` volume cleared on pod restart in Kubernetes** — Standard Kubernetes behaviour; cannot be verified from this codebase.

---

## Recommended Actions

1. ~~**(Blocker)**~~ ✅ **Resolved.** §4.8 now documents the guard using the existing `/var/lib/cassandra/.last-restore` mechanism from [`k8s/docker-entrypoint.sh`](../../../k8s/docker-entrypoint.sh): `apply_pitr_restore()` reads the file at entry and compares it to `$RESTORE_KEY`; if they match it returns immediately. The shell writes `.last-restore` after `restore.py` exits, covering both SSTable and PITR steps atomically. No new marker file or constant is introduced.

2. **(Blocker)** Add the k8ssandra-operator changes to scope. At minimum: extend `MedusaRestoreMapping` with a `pitr` field, update `setRestoreMappingInRestoreContainer` to marshal it, and add the operator files to §4.2 / §4.9's file map. The `restore.py` PITR logic cannot be exercised without this.

3. **(Blocker — inconsistency)** Resolve the three-way contradiction about who writes `commitlog_archiving.properties` (§4.8 body says `apply_pitr_restore()` writes it; OQ3 in §7 says the operator's config-builder writes it). Pick one authoritative answer and remove the other two.

4. **(High)** Clarify the staging directory path. Choose a concrete default (e.g. `/medusa-commitlog-staging`) and document it in §4.3 (config) and §4.8 (restore flow). `generate_commitlog_archiving_properties()` depends on this value.

5. **(High)** Fix the purge formula field name. §4.10 uses `pitr.interval_seconds` but §4.3 names it `commitlog_archive_interval_seconds`. Align both sections to the same key name.

6. **(Medium)** Fix the `server_test.py` test description in §6.5 — `PreparePitrRestore` must not say "writes correct metadata to disk". The test should assert the RPC returns the correct `backupName` and per-node metadata map in the response.

7. **(Medium)** Add a success criterion (SC8) for `GetCommitLogArchiveStatus` — e.g. "returns `running=false` when no archiver is configured; returns correct `pending_count` after N hardlinks are created in the spool".

8. **(Low)** Add a brief note on proto backward compatibility — how the existing `medusa.proto` consumers (k8ssandra-operator's generated Go client) should handle the new RPCs (they are additive and safe, but re-generation of the Go client stubs should be called out).
