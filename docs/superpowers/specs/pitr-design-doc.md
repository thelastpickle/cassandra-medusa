# Point-in-Time Recovery (PITR) for Cassandra Medusa

- **Author(s):** Alexander Dejanovski
- **Status:** Draft
- **Reviewers:** 

---

## 1. Context and Scope

### Background

Cassandra Medusa is a backup and restore tool for Apache Cassandra clusters. It currently supports snapshot-based backups (full and differential) stored in object storage (S3, GCS, Azure, local). Restores can only target an exact backup snapshot, leaving all data written after that snapshot unrecoverable if the cluster is lost or corrupted.

Cassandra itself offers a native mechanism for finer-grained recovery: every mutation is first written to a commitlog segment before being applied. Once a segment is sealed (i.e., the active write position moves to a new segment), it is immutable and can be replayed deterministically. Cassandra exposes a `commitlog_archiving.properties` hook with two commands: `archive_command` fires when a segment is finalized (after the final sync; Cassandra will not delete the segment until the command returns successfully) and can be used to preserve the segment; `restore_command` fires during startup replay to fetch each required segment. Together they allow an external process to archive sealed segments and feed them back during a targeted restore, replaying mutations up to a user-specified timestamp.

### Problem Statement

Operators need to restore a Cassandra cluster to an arbitrary point in time — not just to the moment a snapshot was taken. Typical use cases are:

- **Logical data corruption** (e.g. bad application code writes garbage data at T+5m; the snapshot was taken at T; the operator wants to restore to T+4m).
- **Accidental bulk deletes or truncations** where the exact timestamp of the operation is known.
- **Compliance requirements** mandating a specific recovery point objective (RPO) finer than the snapshot cadence.

### Why Now

The gRPC server mode introduced in Medusa provides a long-lived process context — a prerequisite for running a continuous archiving background thread. With the server model in place, PITR can be added without architectural changes to the core backup/restore pipeline.

### Scope

**In scope:**
- Continuous archiving of sealed Cassandra commitlog segments to object storage. Cassandra's `archive_command` (configured by the operator) creates a hardlink of each finalized segment into a local spool directory; Medusa's `CommitLogArchiver` thread monitors that spool, uploads segments to object storage, and removes the hardlink on completion.
- A new `PreparePitrRestore` gRPC RPC that validates pre-flight conditions and resolves the base snapshot name per-node (ensuring every node has `node_backup.snapshot_time ≤ target_timestamp`), without fetching segment lists centrally.
- Per-node SSTable restore in [`restore.py`](../../../medusa/service/grpc/restore.py): restores the base snapshot SSTables for each node.
- Per-node PITR restore and archiving configuration in [`pitr_restore.py`](../../../medusa/service/grpc/pitr_restore.py): queries object storage for commitlog segments archived since the node's base snapshot, downloads them to local staging (`/var/lib/cassandra/medusa_commitlog_staging`), and writes `commitlog_archiving.properties` (with restore parameters during a PITR restore; archiving-only on normal startup).
- A new `GetCommitLogArchiveStatus` gRPC RPC for operator health checks.
- Extension of `medusa purge` to delete commitlog segments older than the oldest surviving backup.
- A new `[pitr]` configuration section in `medusa.ini`.

**Out of scope (explicit non-goals):**
- `k8ssandra-operator` implementation changes (CRD modifications, controller orchestration, reconciliation loops, and Go client updates are specified and tracked in a dedicated operator design doc; this document defines Medusa's contracts, gRPC APIs, `RESTORE_MAPPING` structure, and runtime behaviour).
- Cross-topology PITR (restoring to a cluster with a different node count).
- DSE-specific commitlog archiving.
- Custom `restore_command` scripts (Medusa always uses the staging-directory pattern).
- Commitlog encryption (segments are subject to the same object-storage encryption-at-rest as backup snapshots; no Medusa-side encryption is added).
- New CLI commands (the existing `medusa-server` gRPC command is the entry point; no new top-level commands are added).

---

## 2. Goals and Non-Goals

### Goals

- **G1:** Enable full-cluster PITR restore to any timestamp covered by archived commitlog segments since the base snapshot.
- **G2:** The archiving daemon runs as a background thread inside the existing gRPC server — no new process, CronJob, or sidecar required.
- **G3:** `pitr.enabled = false` by default; zero behaviour change when PITR is disabled.
- **G4:** All storage operations use the existing `AbstractStorage` abstraction; no backend-specific code is added.
- **G5:** A failed segment upload never crashes the gRPC server.
- **G6:** Segment finalization is guaranteed by Cassandra's `archive_command` hook before Medusa touches any segment — Medusa never reads from the live commitlog directory.

### Non-Goals

- **NG1:** Cross-topology restore (restoring to a cluster with a different node count — commitlog streams are per-node and cannot be safely split or merged).
- **NG2:** Providing a CLI path for PITR; operator tooling is expected to use the gRPC API.
- **NG3:** Providing segment-level granularity within a single Cassandra write batch.

### Success Criteria

| # | Criterion | How verified |
|---|---|---|
| SC1 | A PITR restore to timestamp T produces a cluster state where all rows written before T are present and all rows written after T are absent | Integration test: row count assertion at a known target timestamp |
| SC2 | `CommitLogArchiver` uploads all segments present in the spool before sleeping until the next tick — a slow batch delays the next tick rather than being interrupted by it | Unit test: `_run_once` with mock storage returning N files; assert all N are uploaded and removed before the sleep call is made |
| SC3 | A failed segment upload does not crash the gRPC server; the segment remains in the spool and is retried on the next tick | Unit test: `_run_once` raises on upload; server continues; segment still present in spool |
| SC4 | `pitr.enabled = false` (default) produces zero behaviour change to existing backup, restore, and purge commands | Run the full existing unit + integration test suite with no `[pitr]` section in `medusa.ini`; assert zero new test failures compared to baseline |
| SC5 | `PreparePitrRestore` returns `FAILED` immediately when any node lacks an eligible base backup with `node_backup.snapshot_time ≤ target_timestamp`, aborting before any pod restart or data wipe | Unit test: `select_node_base_snapshot` raises `ValueError`; RPC maps to FAILED response |
| SC6 | `CommitLogArchiver.__init__` raises `RuntimeError` with a clear message when spool dir and commitlog dir are on different devices (or directories are inaccessible), failing fast and crashing Medusa at startup | Unit test: `os.stat` mocked to return differing `st_dev` or missing dir |
| SC7 | `purge_commitlogs` deletes all segments whose `blob.last_modified < purge_threshold` and retains all others | Unit test: mock blob list with boundary values |
| SC8 | `GetCommitLogArchiveStatus` returns `running=false` when no archiver is configured; returns correct `pending_count` after N hardlinks are created in the spool | Unit test: (a) RPC invoked with no `CommitLogArchiver` running — assert `running=false`, `pending_count=0`; (b) N dummy segment files written to spool dir before RPC call — assert `pending_count=N` |

---

## 3. Overview (Executive Summary)

PITR is implemented as two cooperating additions to the existing gRPC server:

1. **`CommitLogArchiver` (background thread):** Started on server startup when `pitr.enabled = true`. It polls the local commitlog spool directory (`commitlog_spool_dir`) every `commitlog_archive_interval_seconds` (default: 60s). The spool is populated by Cassandra's `archive_command` (configured by the operator), which creates a hardlink of each finalized segment into that directory immediately after the segment is synced and sealed — guaranteeing Medusa never sees a partially-written file. For each file found, the archiver uploads it to `<prefix>/<fqdn>/commitlogs/` in object storage and removes the hardlink on success.

2. **`PreparePitrRestore` RPC (Pre-flight & Per-Node Base Selection):** Given a `target_timestamp`, it: queries node backups in object storage for all nodes in the cluster and selects the most recent base backup for each node where `node_backup.snapshot_time ≤ target_timestamp`. If any node in the cluster lacks an eligible base backup before the target timestamp, the RPC returns `FAILED` immediately, preventing destructive data wipes on a cluster that cannot be fully restored. If all nodes are eligible, it returns a map of `{fqdn: backup_name}` and the parsed `target_timestamp_s`. It does not query or list commitlog segments centrally. The operator assembles a single `RESTORE_MAPPING` JSON — containing `target_timestamp_s` and the full `node_backups` map — and sets it identically on every pod's initContainer. Each pod resolves its own `backup_name` at runtime by looking up its `fqdn` in `pitr.node_backups`.

3. **Per-node `pitr_restore.py`:** After SSTables are restored by `restore.py`, [`pitr_restore.py`](../../../medusa/service/grpc/pitr_restore.py) is executed. It cleans the local staging directory (`/var/lib/cassandra/medusa_commitlog_staging`), checks whether an active PITR restore is taking place, queries object storage live for commitlog segments archived since `node_backup.snapshot_time`, downloads them to the staging directory, and writes `commitlog_archiving.properties` (with `restore_directories` pointing to staging and `restore_point_in_time` set to target; on normal start or skipped restore, it writes `archive_command` only). Cassandra is started by the operator and replays segments up to `restore_point_in_time` automatically on startup; Cassandra's own replay logic skips mutations already persisted in the base SSTables. No SSH is used at any point.

The design is purely additive. Existing backup, restore, and purge commands are unchanged when PITR is disabled.

---

## 4. Proposed Architecture & Detailed Design

### 4.1 System Architecture

```mermaid
graph TD
    subgraph gRPC Sidecar per pod
        S[Server.serve] -->|pitr.enabled=true| A[CommitLogArchiver thread]
        A -->|polls every interval| CL[commitlog_spool_dir]
        A -->|upload sealed segments| OS[Object Storage]
        SVC[MedusaService] -->|GetCommitLogArchiveStatus| A
        SVC -->|PreparePitrRestore| RPC_HANDLER[server.py: PreparePitrRestore handler]
    end

    subgraph PreparePitrRestore - coordinator only
        RPC_HANDLER -->|calls| PITR_MODULE[medusa/pitr_restore.py]
        PITR_MODULE -->|select_node_base_snapshot per node| OS
        PITR_MODULE -->|returns per-node backup names| RM[RESTORE_MAPPING inline JSON]
    end

    subgraph initContainer per pod - at restart
        RM -->|read PITR block| RC[restore.py: restore base SSTables]
        RC -->|invoked next by docker-entrypoint| PR[pitr_restore.py: PITR & archiving setup]
        PR -->|query & download segments live| DL[download segments from storage]
        DL -->|returns properties content| PROPS[commitlog_archiving.properties]
        PROPS -->|Cassandra reads on startup| CS[Cassandra replays to target_timestamp]
    end

    subgraph Object Storage Layout
        OS --> |prefix/fqdn/backups/| BK[Backup snapshots]
        OS --> |prefix/fqdn/commitlogs/| CLS[CommitLog segments]
    end
```

### 4.2 Complete File Map

| File | Change | Responsibility |
|---|---|---|
| [`medusa/config.py`](../../../medusa/config.py) | Modify | `PitrConfig` namedtuple; `[pitr]` section defaults; `MedusaConfig.pitr` field |
| [`medusa/service/grpc/commitlog_archiver.py`](../../../medusa/service/grpc/commitlog_archiver.py) | Create | `CommitLogArchiver` background thread; `drain_spool()` helper |
| [`medusa/service/grpc/server.py`](../../../medusa/service/grpc/server.py) | Modify | Archiver lifecycle; `GetCommitLogArchiveStatus` + `PreparePitrRestore` RPC handlers |
| [`medusa/service/grpc/restore.py`](../../../medusa/service/grpc/restore.py) | Modify | Update `restore_backup(..., backup_name=...)` signature to support per-node base backup override |
| [`medusa/service/grpc/pitr_restore.py`](../../../medusa/service/grpc/pitr_restore.py) | Create | Per-node PITR & archiving restore entrypoint: clean staging, check guard, download segments, manage `commitlog_archiving.properties` |
| [`medusa/service/grpc/medusa.proto`](../../../medusa/service/grpc/medusa.proto) | Modify | Add `GetCommitLogArchiveStatus` + `PreparePitrRestore` RPCs and message types (purely additive — no existing RPCs removed) |
| [`medusa/pitr_restore.py`](../../../medusa/pitr_restore.py) | Create | Pure restore logic: `select_node_base_snapshot`, `find_next_backup`, `find_commitlog_segments`, `generate_commitlog_archiving_properties` |
| [`k8s/docker-entrypoint.sh`](../../../k8s/docker-entrypoint.sh) | Modify | Always invoke `pitr_restore.py` after SSTable restore step |
| [`medusa/storage/node_backup.py`](../../../medusa/storage/node_backup.py) | Modify | Add `snapshot_time` property (timestamp of first snapshot dir, falling back to `started`) |
| [`medusa/storage/cluster_backup.py`](../../../medusa/storage/cluster_backup.py) | Modify | Add `min_snapshot_time` and `max_snapshot_time` properties aggregating `node_backup.snapshot_time` across nodes |
| [`medusa/cassandra_utils.py`](../../../medusa/cassandra_utils.py) | Modify | Inspect snapshot directory mtime before snapshot cleanup to record node snapshot timestamp (falls back to `started` if no snapshot directories are present) |
| [`medusa/index.py`](../../../medusa/index.py) | Modify | Persist `snapshot_time_{fqdn}_{timestamp}.timestamp` index blob |
| [`medusa/storage/__init__.py`](../../../medusa/storage/__init__.py) | Modify | Read `snapshot_time` index blob in `list_node_backups()` to populate `NodeBackup` |
| [`medusa/purge.py`](../../../medusa/purge.py) | Modify | `purge_commitlogs()`; PITR hook in `main()` |
| [`medusa-example.ini`](../../../medusa-example.ini) | Modify | Document `[pitr]` config section |

### 4.3 Configuration

A new `[pitr]` section is added to [`medusa/config.py`](../../../medusa/config.py) as a `PitrConfig` namedtuple, parallel to the existing `KubernetesConfig`:

```python
PitrConfig = collections.namedtuple(
    'PitrConfig',
    ['enabled', 'commitlog_archive_interval_seconds', 'commitlog_spool_dir', 'commitlog_staging_dir',
     'commitlog_restore_grace_period_seconds']
)
```

Defaults in `_build_default_config()`:

```ini
[pitr]
enabled = false
commitlog_archive_interval_seconds = 60
commitlog_spool_dir = /var/lib/cassandra/commitlog_spool_dir
commitlog_staging_dir = /var/lib/cassandra/medusa_commitlog_staging
commitlog_restore_grace_period_seconds = 3600
```

`commitlog_spool_dir` must be on the same filesystem (volume) as Cassandra's commitlog directory (`/var/lib/cassandra/commitlog`) — hardlinks cannot cross filesystem boundaries. Defaulting to `/var/lib/cassandra/commitlog_spool_dir` ensures it resides on the persistent Cassandra data mount. In Kubernetes, this is shared between the Cassandra container and the Medusa sidecar.

`commitlog_staging_dir` specifies the local staging directory (default: `/var/lib/cassandra/medusa_commitlog_staging`) where `pitr_restore.py` downloads commitlog segments from object storage during a PITR restore. Residing directly on the Cassandra persistent data volume, it leverages the volume's high capacity. `pitr_restore.py` systematically empties and resets this staging directory on every initContainer startup to ensure no orphaned segments persist across restarts.

`commitlog_restore_grace_period_seconds` (default: `3600`, i.e. 1 hour) sets the upper-bound grace period applied when selecting commit log segments during a restore. At restore time, `pitr_restore.py` calls `find_next_backup(storage, fqdn, base_backup)` to locate the earliest backup whose `snapshot_time > base_backup.snapshot_time`. When found, the upper bound for segment fetch is `next_backup.snapshot_time + commitlog_restore_grace_period_seconds`. This handles async upload lag: segments belonging to the restore window may still be in flight when the next backup snapshot starts, so a conservative window beyond the next snapshot time ensures they are captured. When there is no subsequent backup, `upper_bound_s` falls back to `time.time()` (the current wall clock) — avoiding an unbounded scan while still capturing any segments that have been uploaded since the base snapshot.

`archive_command` is not a config field — it is derived at runtime from `commitlog_spool_dir`:

```python
archive_command = f"/bin/ln %path {config.pitr.commitlog_spool_dir}/%name"
```

This ensures the generated `commitlog_archiving.properties` is always consistent with the configured spool directory without requiring a separate, potentially conflicting config value.

`[checks].enable_md5_checks` is reused from the existing `[checks]` section for upload deduplication.

### 4.4 CommitLogArchiver

`CommitLogArchiver` is a `threading.Thread` subclass (daemon thread) started from [`Server.serve()`](../../../medusa/service/grpc/server.py:66) when PITR is enabled, and stopped in `Server.shutdown()`. Starting it in `serve()` (rather than `__init__`) avoids thread leaks in unit test suites where `Server` is instantiated without being run.

**Startup validation:**

`CommitLogArchiver.__init__` checks that `commitlog_spool_dir` and the Cassandra commitlog directory reside on the same filesystem by comparing `os.stat(commitlog_spool_dir).st_dev` with `os.stat(cassandra_commitlog_dir).st_dev`. If they differ (or if either directory does not exist / is inaccessible), it raises `RuntimeError` immediately with a message of the form:

```
commitlog_spool_dir '<path>' is on a different device than the Cassandra commitlog
directory '<path>'. Hardlinks cannot cross filesystem boundaries. Mount both paths
on the same volume.
```

Because `CommitLogArchiver` is initialized in [`Server.serve()`](../../../medusa/service/grpc/server.py:66) when `pitr.enabled = true`, this exception is intentionally unhandled at startup: it **fails fast and crashes Medusa immediately** (causing the sidecar container/process to exit with a non-zero code). This guarantees that filesystem misconfigurations cannot go unnoticed or run in a silently degraded state where Cassandra's `archive_command` would later fail with `EXDEV`.

**Spool drain (`drain_spool`):**

Medusa never reads from Cassandra's live commitlog directory. Instead, the operator configures Cassandra with:

```
archive_command=ln %path <commitlog_spool_dir>/%name
```

Cassandra executes this command after the segment's final fsync and before it is eligible for deletion — the hardlink is therefore always a complete, immutable snapshot of the finalized file. Because hardlinks share the same inode, creation is atomic and requires no data copy; they do require the spool directory and the commitlog directory to be on the same filesystem (same volume).

`drain_spool` lists all files in `commitlog_spool_dir`, and for each:
1. Checks `<prefix>/<fqdn>/commitlogs/<filename>` in storage.
   - If absent → upload.
   - If present and size matches → skip (already uploaded); or MD5 check if `[checks].enable_md5_checks=true`.
   - If present but size mismatches → re-upload (partial upload from a prior crash).
2. On successful upload or confirmed-present skip: removes the hardlink from the spool.
3. On upload failure: logs the error, leaves the hardlink in place for retry next tick.

**Upload loop (`_run_once`):**

`_run_once` is invoked from a single daemon thread only — there are no concurrent invocations. The internal lock guards only the status counters read by `status()`.

1. Call `drain_spool()`, which handles the full per-file cycle: check storage, upload or skip, remove hardlink on success, log on failure.
2. Update internal status counters under a lock.
3. Any exception from `drain_spool()` is caught and logged; the thread continues.

**Lifecycle:**
- `start()` — begins the polling loop (inherited from `Thread`).
- `stop()` — sets a `threading.Event`; joins with `timeout=interval+5s`.
- `status() → dict` — returns `running`, `last_upload_time`, `pending_count`, `interval_seconds`, `status` (`StatusType`: `IN_PROGRESS` while the archiver thread is alive, `FAILED` if stopped unexpectedly), and `last_error` (non-empty string when the last poll failed to read the spool directory; empty string otherwise).

### 4.5 pitr_restore.py — Pure Restore Logic

[`medusa/pitr_restore.py`](../../../medusa/pitr_restore.py) contains only decision logic with no gRPC or SSH dependencies, making it independently testable.

| Function | Signature | Description |
|---|---|---|
| `select_node_base_snapshot` | `(storage, fqdn: str, target_timestamp_s: float) -> NodeBackup` | Most recent node backup for `fqdn` with `snapshot_time ≤ target_timestamp_s`; raises `ValueError` if none. `node_backup.snapshot_time` falls back to `started` when the `snapshot_time` index blob is absent (pre-PITR backups). Because `started ≤ actual snapshot_time`, this fallback makes `snapshot_time` earlier than the true value, so the `≤ target_timestamp_s` check may conservatively reject a candidate that would have been eligible under the real timestamp. This is a safe direction: a backup is only selected when Medusa can confirm the node completed its snapshot before the target time. |
| `find_next_backup` | `(storage, fqdn: str, base_backup: NodeBackup) -> NodeBackup \| None` | Returns the earliest node backup for `fqdn` whose `snapshot_time > base_backup.snapshot_time`, or `None` if no such backup exists. Used by `pitr_restore.py` to compute `upper_bound_s` for segment selection. |
| `find_commitlog_segments` | `(storage_driver, prefix_path, fqdn, after_snapshot_time_s: float, safety_margin_s: float = 30.0, upper_bound_s: float \| None = None) -> List[str]` | All storage paths for this node with `blob.last_modified ≥ (after_snapshot_time_s - safety_margin_s)` and, when `upper_bound_s` is set, `blob.last_modified ≤ upper_bound_s`; sorted ascending by filename. When `upper_bound_s` is `None`, no upper bound is applied — callers must always pass a computed value; `None` is reserved for cases where no bound can be determined. |
| `generate_commitlog_archiving_properties` | `(archive_command: str, restore_directories: str \| None = None, target_timestamp_s: float \| None = None) -> str` | `commitlog_archiving.properties` file content. Always includes `archive_command`. When `restore_directories` and `target_timestamp_s` are provided, also appends `restore_directories` and `restore_point_in_time` (formatted in `%Y:%m:%d %H:%M:%S` UTC). When omitted, outputs archiving settings only. |

**Timestamp Parsing and Formatting:**
- Cassandra's `CommitLogArchiver` parses `restore_point_in_time` using the format equivalent to the Python strftime pattern `%Y:%m:%d %H:%M:%S`. Cassandra interprets this timestamp in the UTC timezone.
- `generate_commitlog_archiving_properties` converts `target_timestamp_s` (Unix epoch seconds) to UTC:
  `datetime.datetime.fromtimestamp(target_timestamp_s, tz=datetime.timezone.utc).strftime("%Y:%m:%d %H:%M:%S")`.
- `PreparePitrRestore` parses incoming `targetTimestamp` from the gRPC request (accepting either ISO-8601 strings with timezone or Unix epoch numeric string) and converts it into a Unix epoch float (`target_timestamp_s`) in UTC. If ISO-8601 has no timezone specified, it is treated as UTC.

Segment filenames are used only for ordering and as storage object keys — their embedded ID is not used for time filtering.

**Fallback to `started` for segment collection:** During restore, `pitr_restore.py` passes `node_backup.snapshot_time` as `after_snapshot_time_s` to `find_commitlog_segments`. When the `snapshot_time` index blob is absent (pre-PITR backups), `node_backup.snapshot_time` falls back to `started`. Because `started ≤ actual snapshot_time`, the effective lower bound shifts earlier, causing `find_commitlog_segments` to fetch a superset of the segments it would fetch with the real snapshot time. This is safe: Cassandra's own replay logic discards any mutation already persisted in the base SSTables, so downloading extra segments beyond what strictly needed has no correctness impact — only a minor increase in download volume.

### 4.6 Kubernetes Restore Model — Why SSH Orchestration Does Not Apply

Understanding the existing Kubernetes restore flow is essential before designing the PITR restore path.

In Kubernetes, Medusa runs as **two containers in every Cassandra pod**:

- **Sidecar container** (`MEDUSA_MODE=GRPC`): runs the gRPC server (including `CommitLogArchiver`). It has direct access to the local filesystem and the local Cassandra commitlog directory.
- **initContainer** (`MEDUSA_MODE=RESTORE`): runs [`medusa/service/grpc/restore.py`](../../../medusa/service/grpc/restore.py) once at pod startup. It calls [`restore_node.restore_node()`](../../../medusa/restore_node.py) which downloads SSTables, places data, and returns — **without starting Cassandra**. The StatefulSet or Cassandra operator manages pod lifecycle and starts Cassandra.

In the Kubernetes path, the `MedusaRestoreJob` controller calls `GetHostMap` (an operator-internal Go function in [`k8ssandra-operator/pkg/medusa/hostmap.go`](../../../k8ssandra-operator/pkg/medusa/hostmap.go) that lists the backup's nodes and derives a source→target mapping locally in the operator), stores the result in `MedusaRestoreJob.Status.RestoreMapping`, marshals it to inline JSON, and injects it as the `RESTORE_MAPPING` env var on the `medusa-restore` initContainer in the `CassandraDatacenter` `PodTemplateSpec`. A rolling restart propagates this to every pod; the initContainer reads its own entry from the env var at startup. No file is written to disk.

There is **no SSH orchestration in the Kubernetes path**. Each node restores itself locally. The `Orchestration`/SSH layer in `medusa/orchestration.py` is the non-Kubernetes path and must not be used here.

### 4.7 PreparePitrRestore — Coordinator RPC

The PITR coordinator follows the same pattern as `MedusaRestoreJob` in the k8ssandra-operator: the gRPC server performs pre-flight storage validation and returns the result to the caller; the operator assembles the full `RESTORE_MAPPING` JSON and injects it directly into the initContainer env var. No file is written to disk by the RPC.

**Why per-node base backup resolution:** Because node snapshots complete at slightly different times across the cluster, a single global backup selection can be inaccurate. Evaluating `select_node_base_snapshot` per node ensures each node restores from a base snapshot whose snapshot time is strictly prior to `target_timestamp`. Furthermore, evaluating this pre-flight across all nodes guarantees that if even a single node lacks a valid base snapshot, the restore fails fast before any pods are restarted or data is wiped.

**Metadata delivery:** `PreparePitrRestore` is called against any live Medusa gRPC sidecar (the operator chooses which pod to target — no election needed). The RPC iterates over the cluster topology, resolves the base backup for each node, and returns a mapping of `{fqdn: backup_name}` and `target_timestamp_s`. No commitlog segments are listed or returned by the RPC, keeping the coordinator call fast and scalable.

The operator then:
1. Calls `GetHostMap` (an operator-internal Go function in [`k8ssandra-operator/pkg/medusa/hostmap.go`](../../../k8ssandra-operator/pkg/medusa/hostmap.go) — not a Medusa gRPC RPC) to compute `in_place` / `host_map` from the Kubernetes StatefulSet topology and the backup node list.
2. Assembles a single `RESTORE_MAPPING` JSON containing a `pitr` block with `target_timestamp_s` and a `node_backups` map of `{fqdn: backup_name}` for every node in the cluster.
3. Sets the same `RESTORE_MAPPING=<inline-JSON>` on **every** pod's initContainer before restarting them.

Each pod's `restore.py` reads `RESTORE_MAPPING` via `json.loads()` (the same inline-JSON parsing path used today; the `pitr` block is a new optional key handled by the modified `restore_backup()` described in §4.8). This RPC is idempotent and safe to retry: all operations are read-only against object storage.

**RPC Sequence:**

```mermaid
sequenceDiagram
    participant Operator
    participant RPC as PreparePitrRestore (gRPC server)
    participant Storage
    
    Operator->>RPC: PreparePitrRestoreRequest(target_timestamp)
    RPC->>Storage: list_cluster_backups() / list_node_backups()
    loop For each node in cluster topology
        RPC->>RPC: select_node_base_snapshot(storage, fqdn, target_timestamp_s)
        alt Any node has no eligible base snapshot
            RPC-->>Operator: FAILED (no eligible snapshot for node X)
        end
    end
    RPC-->>Operator: SUCCESS - nodeBaseBackups {fqdn -> backup_name} + targetTimestampS
    Note over Operator: Operator assembles RESTORE_MAPPING JSON and sets it on each pod initContainer
```

The `RESTORE_MAPPING` JSON is identical on every pod's initContainer. Each pod resolves its own entry using its local `fqdn`:
```json
{
  "in_place": true,
  "host_map": {
    "node1.fqdn": {"source": ["node1.fqdn"], "seed": false},
    "node2.fqdn": {"source": ["node2.fqdn"], "seed": false},
    "node3.fqdn": {"source": ["node3.fqdn"], "seed": false}
  },
  "pitr": {
    "target_timestamp_s": 1721000000.0,
    "node_backups": {
      "node1.fqdn": "backup-20240715-node1",
      "node2.fqdn": "backup-20240715-node2",
      "node3.fqdn": "backup-20240715-node3"
    }
  }
}
```

The `pitr` block contains the full cluster mapping. Each initContainer reads its own `fqdn` (from the pod's hostname or the Medusa config) and looks up `pitr.node_backups[fqdn]` to obtain its assigned `backup_name`.

### 4.8 Per-Node PITR Restore & Archiving Configuration

After `PreparePitrRestore` succeeds, the operator restarts the pods. In Kubernetes (`MEDUSA_MODE=RESTORE`), [`k8s/docker-entrypoint.sh`](../../../k8s/docker-entrypoint.sh) orchestrates two distinct sequential steps during initContainer startup:

```
docker-entrypoint.sh (MEDUSA_MODE=RESTORE)
  |-- 1. SSTable restore step (restore.py)
  |     |-- compare /var/lib/cassandra/.last-restore vs $RESTORE_KEY
  |     |     if equal: skip restore_backup()
  |     |     if different/absent: run restore.py, write $RESTORE_KEY > .last-restore on success
  |
  +-- 2. PITR & Archiving properties step (pitr_restore.py) [ALWAYS EXECUTED]
        |-- if pitr is disabled in config: exit immediately
        |-- clean staging directory (/var/lib/cassandra/medusa_commitlog_staging)
        |-- check if active PITR restore:
        |     mapping has "pitr" AND .last-restore != $RESTORE_KEY (or file absent)
        |     |
        |     +-- IF ACTIVE PITR RESTORE:
        |     |     |-- find_commitlog_segments(storage, prefix, fqdn, after_snapshot_time_s, upper_bound_s)
        |     |     |-- download segments to local staging dir
        |     |     |-- write commitlog_archiving.properties with archive_command AND restore settings
        |     |
        |     +-- ELSE (normal pod start or skipped restore):
        |           |-- write commitlog_archiving.properties with archive_command ONLY
```

**`RESTORE_MAPPING` format:** `RESTORE_MAPPING` is always inline JSON — the same `json.loads()` path used today. The `pitr` block is an optional top-level key in that JSON, present only for PITR restores. A regular (non-PITR) restore has no `pitr` key and the existing SSTable restore path is completely unchanged.

**Changes to `apply_mapping_env()` in `restore.py`:** `apply_mapping_env()` is modified to also return the full parsed `mapping` dict alongside `in_place`, so the caller (`__main__`) can pass the `pitr` block downstream. The return type changes from `bool | None` to `tuple[bool, dict] | None` — `(in_place, mapping)`. Existing callers that only consume `in_place` are updated accordingly. The `host_map` lookup logic is unchanged.

**`restore_backup()` signature change in `restore.py`:** `restore_backup(in_place, config)` gains a new `backup_name=None` parameter and a `mapping=None` parameter carrying the full parsed `RESTORE_MAPPING`. When `backup_name` is `None` and `mapping` contains a `pitr` block, `restore_backup()` reads the local pod's `fqdn` (from `POD_IP` or `POD_NAME`) and looks up `mapping["pitr"]["node_backups"][fqdn]` to obtain its assigned `backup_name`. When `backup_name` is `None` and there is no `pitr` block, it falls back to `os.environ["BACKUP_NAME"]` — preserving full backward compatibility for non-PITR restores.

`restore_backup()` calls `restore_node_locally()`, which wipes the commitlog directory via `clean_path(cassandra.commit_logs_path, ...)`. Because `pitr_restore.py` runs **after** `restore.py` completes, segment downloading and properties generation happen on a clean slate.

**`commitlog_archiving.properties` ownership & lifecycle:** [`medusa/service/grpc/pitr_restore.py`](../../../medusa/service/grpc/pitr_restore.py) is always invoked by `docker-entrypoint.sh` to generate and maintain `commitlog_archiving.properties` in `Path(cassandra_config.config_file).parent` (the directory containing `cassandra.yaml`).
- When `pitr.enabled` is `true`, archiving configuration is always maintained in `commitlog_archiving.properties` so Cassandra can continuously archive commitlogs once running.
- If an active PITR restore is running (`"pitr"` in mapping and `.last-restore` not yet matching `RESTORE_KEY`), `pitr_restore.py` includes the restore parameters (`restore_directories` pointing to `config.pitr.commitlog_staging_dir`, default `/var/lib/cassandra/medusa_commitlog_staging`, and `restore_point_in_time` in `%Y:%m:%d %H:%M:%S` UTC).
- If no restore is needed (e.g. normal pod start where `.last-restore` matches `RESTORE_KEY`, or non-PITR restore), the restore parameters (`restore_directories`, `restore_point_in_time`) are omitted from `commitlog_archiving.properties`, ensuring Cassandra starts without replaying mutations while keeping archiving enabled.
- The config directory is a shared volume mounted across initContainers and the Cassandra container, so Medusa can write there directly.

Sample generated `commitlog_archiving.properties` during PITR restore:
```properties
archive_command=/bin/ln %path /var/lib/cassandra/commitlog_spool_dir/%name
restore_directories=/var/lib/cassandra/medusa_commitlog_staging
restore_point_in_time=2024:07:15 12:30:00
```

Sample generated `commitlog_archiving.properties` during normal startup / skip path:
```properties
archive_command=/bin/ln %path /var/lib/cassandra/commitlog_spool_dir/%name
```

Cassandra startup detects `commitlog_archiving.properties` and replays segments up to `restore_point_in_time` automatically. No Medusa code starts or stops Cassandra.

**Entrypoint Flow in `docker-entrypoint.sh`:**

```bash
restore() {
    echo "Running Medusa in restore mode"
    last_restore_file=/var/lib/cassandra/.last-restore

    # 1. SSTable snapshot restore
    if [ -n "$BACKUP_NAME" ] && [ -n "$RESTORE_KEY" ]; then
        if [ -f "$last_restore_file" ]; then
            restore_key=$(cat "$last_restore_file")
        else
            restore_key=""
        fi

        if [ "$restore_key" == "$RESTORE_KEY" ]; then
            echo "Skipping SSTable restore operation"
        else
            echo "Restoring backup $BACKUP_NAME"
            poetry run python -m medusa.service.grpc.restore "/etc/medusa/medusa.ini" "$RESTORE_KEY"
            echo "$RESTORE_KEY" > "$last_restore_file"
        fi
    fi

    # 2. Always run pitr_restore to configure commitlog_archiving.properties and stage segments if needed
    echo "Configuring PITR and commitlog archiving settings"
    poetry run python -m medusa.service.grpc.pitr_restore "/etc/medusa/medusa.ini" "$RESTORE_KEY"
}
```

**`pitr_restore.py` execution logic:**

```python
def main():
    config_file_path = sys.argv[1]
    restore_key = sys.argv[2] if len(sys.argv) > 2 else os.environ.get("RESTORE_KEY", "")
    config = create_config(config_file_path)

    if not config.pitr.enabled:
        logging.info("PITR is disabled; skipping PITR restore and commitlog archiving configuration")
        return

    # Always ensure staging directory is clean
    clean_staging_dir(config.pitr.commitlog_staging_dir)

    mapping = json.loads(os.environ.get("RESTORE_MAPPING", "{}"))
    last_restore_file = Path("/var/lib/cassandra/.last-restore")
    last_restore_val = last_restore_file.read_text().strip() if last_restore_file.exists() else ""

    is_pitr_restore = "pitr" in mapping and (not restore_key or last_restore_val != restore_key)

    properties_path = Path(config.cassandra.config_file).parent / "commitlog_archiving.properties"

    if is_pitr_restore:
        logging.info("Active PITR restore detected: downloading commitlog segments")
        pitr_meta = mapping["pitr"]
        target_timestamp_s = pitr_meta["target_timestamp_s"]
        fqdn = os.environ.get("POD_IP", os.environ.get("POD_NAME"))
        backup_name = pitr_meta["node_backups"][fqdn]

        with Storage(config=config.storage) as storage:
            # Load the base NodeBackup by name to retrieve its snapshot_time as the segment
            # lower bound. The backup_name is authoritative (resolved by PreparePitrRestore);
            # reading snapshot_time from storage avoids duplicating it in RESTORE_MAPPING.
            base_backup = storage.get_node_backup(fqdn, backup_name)
            after_snapshot_time_s = base_backup.snapshot_time

            # Compute the upper bound for segment selection from the next available backup.
            # Falls back to the current wall clock when no subsequent backup exists, so the
            # scan is always bounded and captures any segments uploaded since the base snapshot.
            next_backup = find_next_backup(storage, fqdn, base_backup)
            if next_backup is not None:
                upper_bound_s = next_backup.snapshot_time + config.pitr.commitlog_restore_grace_period_seconds
            else:
                upper_bound_s = time.time()

            segments = find_commitlog_segments(
                storage,
                config.storage.prefix,
                fqdn,
                after_snapshot_time_s,
                upper_bound_s=upper_bound_s,
            )
            download_commitlog_segments(storage, segments, config.pitr.commitlog_staging_dir)

        props = generate_commitlog_archiving_properties(
            archive_command=f"/bin/ln %path {config.pitr.commitlog_spool_dir}/%name",
            restore_directories=config.pitr.commitlog_staging_dir,
            target_timestamp_s=target_timestamp_s,
        )
    else:
        logging.info("Ensuring archiving-only commitlog_archiving.properties without restore parameters")
        props = generate_commitlog_archiving_properties(
            archive_command=f"/bin/ln %path {config.pitr.commitlog_spool_dir}/%name"
        )

    properties_path.write_text(props)
```

**Semantics by scenario:**

| Scenario | `.last-restore` vs `RESTORE_KEY` | Result |
|---|---|---|
| First restore attempt | File absent → no match | Full SSTable restore run, segments downloaded, restore properties written |
| Pod crash mid-download | File absent (not yet written by shell) | Full re-execution on restart — correct |
| Pod restart after successful PITR restore | Match (shell wrote key after SSTable success) | SSTables skipped; `pitr_restore.py` stages 0 segments and writes archiving-only properties (no replay) |
| New restore job | No match (operator sets a new `RESTORE_KEY`) | Full execution — correct |

**Cleanup and Safety Guarantees:**
1. `pitr_restore.py` systematically empties and cleans the staging directory (`/var/lib/cassandra/medusa_commitlog_staging`) on every run.
2. `pitr_restore.py` maintains `commitlog_archiving.properties`: when restore is skipped (`.last-restore` matches `$RESTORE_KEY`), restore settings (`restore_directories`, `restore_point_in_time`) are excluded from `commitlog_archiving.properties` while archiving settings remain active.
3. If a pod crashes mid-download, `.last-restore` has not been written yet; upon restart `pitr_restore.py` purges the staging directory and re-executes cleanly and idempotently.

**Point-in-time semantics:** `restore_point_in_time` is a mutation-timestamp cutoff, not a wall-clock receive-time cutoff. Cassandra filters replayed mutations by the timestamp embedded in each mutation, which is the client-supplied CQL `USING TIMESTAMP` value or the coordinator's clock at the time the write was processed — not the time the segment was archived or the time Cassandra received the request. A write with a backdated CQL timestamp before the cutoff will be replayed; a write with a future-dated CQL timestamp after the cutoff will be suppressed even if it was acknowledged before the target time. Operators should be aware of this when the target application uses custom CQL timestamps.

`target_timestamp` in the request accepts either a Unix epoch float string or ISO-8601 (interpreted as UTC if timezone offset is not explicitly provided).

### 4.9 Purge Integration

[`purge_commitlogs(storage, fqdn, oldest_kept_snapshot_ts, interval_seconds)`](../../../medusa/purge.py) is called from `main()` inside the existing `with Storage(...) as storage:` block when `pitr.enabled = true`. It deletes all segments under `<prefix>/<fqdn>/commitlogs/` whose `blob.last_modified` is strictly less than the purge threshold.

**Retention anchor:** `oldest_kept_snapshot_ts` is the minimum `min_snapshot_time` timestamp across all backups that purge decides to retain. Using `min_snapshot_time` (with fallback to `started`) ensures every segment written from the moment the oldest retained snapshot was taken is preserved — guaranteeing a complete replay chain.

**Safety margin:** A segment uploaded just before the snapshot was recorded may have a `last_modified` slightly earlier than `oldest_kept_snapshot_ts` due to upload lag (up to one archiver poll interval) and clock skew between the node and the storage backend. To avoid under-retention, the effective purge threshold is:

```
purge_threshold = oldest_kept_snapshot_ts - interval_seconds - 30
```

`interval_seconds` comes from `pitr.commitlog_archive_interval_seconds` in the Medusa config (the archiver poll interval). The fixed 30-second buffer covers realistic clock skew. No additional configuration knob is required.

Segment timestamps are **not** extracted from filenames. `blob.last_modified` — already available on every [`AbstractBlob`](../../../medusa/storage/abstract_storage.py) returned by `list_objects()` — is the sole timestamp used for purge decisions.

**Purge scope assumption:** `purge_commitlogs` operates on a single `fqdn`. In Kubernetes, each pod's Medusa sidecar calls `medusa purge` independently and purges only the segments it archived — those stored under `<prefix>/<fqdn>/commitlogs/`. This is correct and consistent with the per-pod sidecar model: no cross-node purge coordination is needed.

### 4.10 Proto Changes

Two new RPCs are added to the `Medusa` service in [`medusa.proto`](../../../medusa/service/grpc/medusa.proto):

```protobuf
rpc GetCommitLogArchiveStatus(GetCommitLogArchiveStatusRequest)
    returns (GetCommitLogArchiveStatusResponse);

rpc PreparePitrRestore(PreparePitrRestoreRequest)
    returns (PreparePitrRestoreResponse);
```

Message definitions:

```protobuf
message GetCommitLogArchiveStatusRequest {
}

message GetCommitLogArchiveStatusResponse {
  bool       running         = 1;
  int64      lastUploadTime  = 2;  // Unix epoch seconds; 0 if never uploaded
  int32      pendingCount    = 3;  // segments in spool not yet uploaded
  int32      intervalSeconds = 4;
  StatusType status          = 5;
  string     lastError       = 6;  // non-empty when the last poll could not read the spool directory; empty otherwise
}

message PreparePitrRestoreRequest {
  string targetTimestamp = 1;  // Unix epoch float string or ISO-8601 (UTC)
}

message PreparePitrRestoreResponse {
  StatusType          status          = 1;
  // Per-node base backup mapping keyed by node FQDN: { "node1.fqdn": "backup-name", ... }
  // The operator places this map in pitr.node_backups inside the single RESTORE_MAPPING JSON
  // that is set identically on every pod's initContainer.
  map<string, string> nodeBaseBackups = 2;
  double              targetTimestampS = 3;  // echo of the parsed target timestamp (Unix epoch float)
}
```

`medusa_pb2.py` and `medusa_pb2_grpc.py` must be regenerated with `grpc_tools.protoc` after the proto change.

**Backward compatibility:** The two new RPCs are additive only — no existing messages or field numbers are changed. Existing consumers of `medusa.proto` are unaffected at the wire level; older clients that have not regenerated their stubs will simply not expose the new RPCs. The k8ssandra-operator's generated Go client falls into this category: when upgrading Medusa, the operator repo must regenerate its Go stubs from the updated proto using `protoc` with the `grpc-go` plugin so the operator can call `GetCommitLogArchiveStatus` and `PreparePitrRestore`.

### 4.11 Object Storage Layout

```
<prefix>/<fqdn>/commitlogs/CommitLog-6-1234567890123.log
<prefix>/<fqdn>/commitlogs/CommitLog-6-1234567890456.log
```

- Same bucket and `<prefix>` as backups — no new bucket or credential required.
- Segment filenames are preserved verbatim from Cassandra.
- Filenames are used only as storage object keys and for ordering — segment filename IDs are not parsed for time filtering. Segment selection at restore time uses `blob.last_modified` bounds (via `find_commitlog_segments`); `restore_point_in_time` in `commitlog_archiving.properties` is the sole mutation-level cutoff applied by Cassandra during replay. At purge time, retention is based on `blob.last_modified`, not on the filename (see §4.9).

---

## 5. Alternative Solutions & Trade-offs

### Option A — CommitLog archiver as a separate sidecar/process (Rejected)

**Pros:**
- Isolation — archiver failures cannot affect the gRPC server.
- Independent scaling.

**Cons:**
- Requires a new Docker image, Kubernetes Deployment, and service account.
- Requires shared filesystem or API access to Cassandra's commitlog directory from a separate pod — complex and fragile in Kubernetes environments.
- Significantly higher operational overhead for what is ultimately a single-purpose polling loop.

**Why rejected:** The gRPC server already has access to the local filesystem, storage credentials, and lifecycle management. A background thread within the server achieves the same archiving goal with zero additional deployment complexity.

### Option B — `archive_command` uploads directly to object storage (Rejected)

Instead of hardlinking to a spool, the `archive_command` could invoke a small script that uploads the segment directly to object storage.

**Pros:**
- No spool directory or shared volume needed — no filesystem constraint.
- Minimal latency between finalization and remote availability.

**Cons:**
- The script must embed or discover storage credentials, handle retries, and report failures — duplicating logic already in Medusa.
- Failures in the `archive_command` are opaque to Medusa: there is no way to monitor or surface upload errors through `GetCommitLogArchiveStatus`.
- Cassandra holds the segment open until `archive_command` returns successfully; a slow or failing upload blocks segment rotation and can stall writes.

**Why rejected:** Keeping all storage operations inside the Medusa gRPC server preserves credential management, retry policy, and observability in one place. The hardlink-to-spool approach gives Cassandra a fast, reliable `archive_command` (an `ln` syscall) while Medusa handles the upload asynchronously with its existing infrastructure.

### Option C — `archive_command` with a local polling loop on the live commitlog directory (Rejected)

Medusa polls the live Cassandra commitlog directory directly, using mtime and filename ordering to detect finalized segments.

**Pros:**
- No `archive_command` configuration required on the Cassandra side.

**Cons:**
- Cassandra's segment allocation model (reserve segment, memory-mapped writes) makes it impossible to reliably distinguish an active segment from a finalized one by mtime or filename alone without a finalization handshake.
- A segment can be deleted by Cassandra between Medusa's scan and upload, causing data loss with no way to detect or recover it.
- Polling the live directory creates a race with Cassandra's own segment management.

**Why rejected:** The `archive_command` hook is Cassandra's supported finalization signal. Without it, there is no correct way to determine when a segment is safe to upload.

### Option D — Store a segment manifest / index file (Rejected)

Maintain a JSON manifest in storage mapping each segment to metadata (time range, checksum, etc.).

**Pros:**
- Enables richer segment metadata and explicit coverage checkpoints.

**Cons:**
- Adds concurrency complexity: manifest updates must be atomic across concurrent archivers.
- A manifest reconstructed from filenames alone has the same uncertainty as using filenames directly; a reliable manifest requires the finalization handshake already provided by the spool approach.
- Complicates purge: two objects to delete per segment.

**Why rejected:** The spool approach already provides a finalization guarantee. Filenames serve as sufficient ordering keys and storage object identifiers; no manifest is needed.

---

## 6. Cross-Cutting Concerns & Operational Aspects

### 6.1 Scalability & Performance

- **Upload frequency:** Default 60s drain interval. One storage API call per segment in the spool per tick. At 100 MB/s aggregate write rate per node and a 256 MB segment size, a busy node seals approximately 23 segments/minute; at steady state the spool drains continuously. RPO is bounded by Cassandra's segment rotation cadence, not the polling interval — segments appear in the spool immediately after finalization.
- **Storage overhead:** Commitlog segments are typically 32–256 MB. A cluster writing 1 GB/s across all nodes accumulates approximately 86 TB/day of raw commitlog data. Operators should size object storage and set appropriate purge windows accordingly.
- **Thread overhead:** One daemon thread per gRPC server pod. No thread pool fan-out within the archiver.
- **Restore latency:** PITR restore is bounded by base-snapshot restore time (existing bottleneck) plus commitlog download and Cassandra replay time. The replay step is performed by Cassandra itself in parallel with normal startup; it is not a Medusa bottleneck.

### 6.2 Security & Privacy

- All storage access uses the existing `AbstractStorage` credential chain (IAM role, service account key, etc.) — no new credentials.
- Commitlog segments contain raw mutation data. They are subject to the same object-storage ACL and encryption-at-rest controls as backup snapshots.
- Segment download in the initContainer uses the existing storage driver directly — no SSH, no new network credentials.
- No new network ports or firewall rules are required.

### 6.3 Observability & Monitoring

- **`GetCommitLogArchiveStatus` RPC:** Returns `running`, `last_upload_time`, `pending_count`, `interval_seconds`, and `status` (`StatusType`: `IN_PROGRESS` while the archiver is running, `FAILED` if it stopped unexpectedly). Operators should alert on `pending_count > N` for sustained periods (indicating upload failures), `running=false`, or `status=FAILED` (archiver stopped unexpectedly).
- **Logging:** The archiver logs at `INFO` on each successful upload batch and at `ERROR`/`WARNING` on upload failures or directory access errors.
- **Purge logging:** `purge_commitlogs()` logs the count of deleted segments at `INFO`.

### 6.4 Failure Modes & Resilience

| Scenario | Behaviour |
|---|---|
| Spool & commitlog dirs on different devices (or directory missing at startup) | `CommitLogArchiver` raises `RuntimeError` at startup; Medusa fails fast and crashes immediately so operators/Kubernetes detect misconfigurations. |
| Upload failure (network, quota) | Logged at ERROR; retried next tick. Server continues. |
| Runtime directory read error during poll | Logged per tick; `last_error` in `GetCommitLogArchiveStatus` is set to the exception message so the error is distinguishable from a genuinely empty spool (`pending_count=0` with `last_error=""` means empty; `pending_count=0` with `last_error` non-empty means unreadable). `pending_count` is left at its last known value rather than reset to 0. No crash. |
| Storage backend unreachable | Existing tenacity retry in storage driver applies before error propagates to archiver. |
| Partial upload (size mismatch) | Detected on next tick; segment is re-uploaded. |
| No base snapshot before target | `PreparePitrRestore` returns `FAILED` immediately. No data movement started. |
| Cassandra fails to start after replay | Surfaced by the pod readiness probe / Cassandra operator. Medusa does not start Cassandra in the Kubernetes path. See §6.6 for remediation steps. |
| gRPC server pod restart | Archiver restarts cleanly; size/hash check prevents duplicate uploads of already-archived segments. |
| initContainer crashes mid-download | Pod restarts; `restore.py` is idempotent — it re-downloads segments and re-writes properties. Staging dir may have partial files; the download step overwrites them. |
| Spool exhaustion / disk full | Only failed-upload segments accumulate — each segment is removed from the spool immediately after a successful upload. Disk exhaustion can only occur if upload failures persist long enough for Cassandra to archive faster than Medusa can drain. Medusa logs upload errors at ERROR per tick; `pending_count` in `GetCommitLogArchiveStatus` rises monotonically. Resolves automatically once the underlying storage issue clears. |

### 6.5 Testing & Rollout Plan

**Unit tests (new):**
- [`tests/service/grpc/commitlog_archiver_test.py`](../../../tests/service/grpc/commitlog_archiver_test.py): `drain_spool()` edge cases (empty spool directory, already-uploaded segments, missing storage objects); `CommitLogArchiver._run_once` with mocked storage driver (upload, skip, re-upload, failure-does-not-raise); `CommitLogArchiver.__init__` raises `RuntimeError` to crash Medusa when spool dir and commitlog dir are on different devices or inaccessible (mock `os.stat` to return differing `st_dev` values or raise `FileNotFoundError`).
- [`tests/pitr_restore_test.py`](../../../tests/pitr_restore_test.py): `select_node_base_snapshot` (correct selection per node based on `snapshot_time`, error when no candidate), `find_commitlog_segments` (blob `last_modified` filtering with safety margin and ascending sort), `generate_commitlog_archiving_properties` (verifying `%Y:%m:%d %H:%M:%S` UTC formatting from epoch timestamps).
- [`tests/storage_test.py`](../../../tests/storage_test.py): `NodeBackup.snapshot_time` resolution (from index blob, snapshot folder timestamp, and fallback to `started`); `ClusterBackup.min_snapshot_time` and `ClusterBackup.max_snapshot_time` aggregation.
- [`tests/config_test.py`](../../../tests/config_test.py): `PitrConfig` defaults and custom values via `load_config`.
- [`tests/purge_test.py`](../../../tests/purge_test.py): `purge_commitlogs` deletes segments below threshold; skips segments above threshold.
- [`tests/service/grpc/server_test.py`](../../../tests/service/grpc/server_test.py): `GetCommitLogArchiveStatus` with no archiver returns `running=false`; `PreparePitrRestore` returns `nodeBaseBackups` mapping and fails fast if any node lacks an eligible base backup.
- [`tests/service/grpc/pitr_restore_test.py`](../../../tests/service/grpc/pitr_restore_test.py): `pitr_restore.py` queries storage, downloads correct segments, and writes valid `commitlog_archiving.properties` for both active restore and skip/normal paths.

**Integration tests :**
1. Start cluster; run workload; take snapshot.
2. Run additional writes; allow archiver to upload segments.
3. Record target timestamp mid-workload.
4. Call `PreparePitrRestore`.
5. Assert row counts match expected state at target timestamp.

**Rollout:**
1. Merge with `pitr.enabled = false` default — no behaviour change in production.
2. Enable in a non-production environment; verify archiver uploads via `GetCommitLogArchiveStatus`.
3. Test a PITR restore on a non-production cluster.
4. Enable in production with an initial purge window large enough to retain all desired recovery points.

### 6.6 Operator Remediation Runbook — PITR Replay Failure

When Cassandra fails to start after commitlog replay (pod readiness probe fails, Cassandra logs show replay errors), two remediation paths are available.

**Path (a) — Rollback to base snapshot (use when segments are corrupted, missing, or unrecoverable)**

1. Inspect Cassandra logs on the failing pod to confirm the replay error (e.g. `CommitLogReplayer` exception, missing segment file, checksum mismatch).
2. Trigger a standard (non-PITR) restore of the base snapshot with a fresh `RESTORE_KEY` (or update `RESTORE_MAPPING` without the `pitr` block).
3. The rolling restart proceeds. `restore.py` runs `restore_backup()` for the base snapshot, followed by `pitr_restore.py` writing archiving-only `commitlog_archiving.properties` (or no properties if PITR is disabled).
4. The staging segment directory (`/var/lib/cassandra/medusa_commitlog_staging`) is systematically cleared by `pitr_restore.py` on pod startup — no manual cleanup needed.
5. The cluster is operational at the base snapshot point in time. Plan a subsequent PITR attempt once the segment gap is resolved (re-archive or accept the data loss window).

**Path (b) — Retry with an adjusted target timestamp (use when the target fell in a gap where archiving was not yet active or segments were not yet uploaded)**

1. Inspect `GetCommitLogArchiveStatus` on each pod and compare `last_upload_time` against the original `target_timestamp` to identify nodes with no segments in that window.
2. Choose an earlier `target_timestamp` that falls within the verified archiving coverage window.
3. Call `PreparePitrRestore` with the adjusted timestamp. The RPC returns updated per-node base backup names.
4. Assemble a new `RESTORE_MAPPING` JSON (via `GetHostMap` + the new `PreparePitrRestore` response) and update the `CassandraDatacenter` spec accordingly.
5. The rolling restart proceeds as normal; `pitr_restore.py` is idempotent and re-downloads segments to the staging directory.

**Decision guide:**

| Symptom | Recommended path |
|---|---|
| Cassandra logs show missing or unreadable segment file | (a) Rollback — the segment cannot be recovered by retrying |
| Cassandra logs show `restore_point_in_time` parsing error | (b) Retry — ensure `restore_point_in_time` in `commitlog_archiving.properties` matches `%Y:%m:%d %H:%M:%S` UTC |
| `GetCommitLogArchiveStatus` shows `last_upload_time` after `target_timestamp` on all nodes | (b) Retry with earlier timestamp |
| Segment gap confirmed (archiver was not running at snapshot time) | (a) Rollback, then enable archiver before the next snapshot |

---

## 7. Open Questions

1. **Commitlog segments and TTL data:** Replaying a commitlog does not resurrect cells whose TTL has expired — Cassandra evaluates liveness against the current clock at query time, not at replay time. The risk goes the other way: a cell that was live at the target timestamp may have since expired and will appear dead when the restored cluster is queried. This is a known limitation, not a Medusa-specific issue.

2. **Time resolution for restores:** Is there a finer resolution than SECONDS which would be supported for PITR in Cassandra, and how does the `precision` field affect the replay.

3. **Are timestamp stored in UTC in commit logs?** The commitlog-archiving.properties file documentation states that the restore time is expected in "GMT". Questions are whether or not timestamps are always GMT/UTC in commit log files, and if not, is the conversion done correctly by Cassandra at replay time.

4. **Do we need a node_backup.snapshot_time to node_backup.started fallback?** A backup that doesn't have a snapshot was created before commit log backups were enabled, and is not suitable for PITR.

