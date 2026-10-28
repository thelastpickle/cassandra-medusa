# PITR-14: Docs, Operator Contract Notes, Runbook, and Rollout Guide

- **Type:** Task
- **Size:** S (2–5 ideal days)
- **Estimated Ideal Days:** 2 days (Est. Calendar Time: 4–5 days)
- **Parallel Track:** Track H — Docs
- **Blocked By / Dependencies:** PITR-06, PITR-08, PITR-10, PITR-11
- **Blocks:** None

---

## Purpose

> WHY are we doing this work? What problem does it solve and what value does it deliver?

PITR introduces new operator-visible configuration, a new gRPC operator workflow, and a new failure-recovery process. Without documentation, operators cannot safely enable the feature, configure `archive_command`, or recover from a replay failure. This ticket produces all written artefacts an operator needs: updated `medusa-example.ini`, a new PITR user guide, and a runbook covering the rollback-vs-retry decision.

This is also the completion gate for the go/no-go checklist defined in the [README](./README.md#gono-go-checklist-for-enabling-pitr-in-production).

## Requirements

> WHAT needs to be true for this work to be successful?

### `medusa-example.ini`

- If PITR-01 has not already documented the `[pitr]` section in [`medusa-example.ini`](../../../medusa-example.ini), add it now with all fields, defaults, and inline comments. The required content is:

  ```ini
  [pitr]
  # Set to true to enable continuous commitlog archiving and PITR restore support.
  # Default: false (zero behaviour change to existing commands when disabled).
  # enabled = false

  # Interval in seconds between archiver drain cycles.
  # The archiver uploads all segments present in commitlog_spool_dir each cycle.
  # commitlog_archive_interval_seconds = 60

  # Local spool directory for hardlinks created by Cassandra's archive_command.
  # MUST be on the same filesystem (same volume) as Cassandra's commitlog directory.
  # In Kubernetes this is the shared persistent data volume (/var/lib/cassandra).
  # commitlog_spool_dir = /var/lib/cassandra/commitlog_spool_dir

  # Staging directory for commitlog segments downloaded during a PITR restore.
  # Must also reside on the Cassandra persistent data volume.
  # commitlog_staging_dir = /var/lib/cassandra/medusa_commitlog_staging

  # Grace period in seconds added to the next backup's snapshot_time when computing
  # the upper bound for segment selection. Covers async upload lag.
  # When no subsequent backup exists, the upper bound falls back to the current clock.
  # commitlog_restore_grace_period_seconds = 3600
  ```

- Do NOT change any existing section in `medusa-example.ini`.

### New documentation page: `docs/Point-in-Time-Recovery.md`

Create `docs/Point-in-Time-Recovery.md` covering the following topics:

1. **Overview** — What PITR is, when to use it, and the two components (archiver thread + restore entrypoint).
2. **Prerequisites** — What the operator must configure on the Cassandra side before enabling PITR in Medusa.
3. **Cassandra `archive_command` setup** — The exact `commitlog_archiving.properties` entry the operator must place in the Cassandra config directory before enabling `pitr.enabled = true`. Medusa generates this file on every pod startup when PITR is enabled, but the operator must ensure Cassandra's own `archive_command` points to `commitlog_spool_dir`. Include the example:
   ```
   archive_command=/bin/ln %path /var/lib/cassandra/commitlog_spool_dir/%name
   ```
   Explain that the spool directory and the Cassandra commitlog directory must be on the same filesystem (same volume, required for hardlinks). If they are not, Medusa crashes at startup with a clear `RuntimeError`.
4. **Operator contract — `RESTORE_MAPPING` and the `pitr` block** — Document the JSON structure the operator assembles after calling `PreparePitrRestore`. Include the full example JSON from design doc §4.7. Explain:
   - `PreparePitrRestore` is the Medusa gRPC RPC the operator calls. It returns `{fqdn: backup_name}` per node and `target_timestamp_s`. It does not start any restore.
   - `GetHostMap` is an operator-side Go function in the k8ssandra-operator (not a Medusa gRPC RPC). It computes `in_place` / `host_map` from the Kubernetes StatefulSet topology.
   - The operator assembles the final `RESTORE_MAPPING` JSON by combining the `host_map` from `GetHostMap` with the `pitr` block from `PreparePitrRestore`.
   - The same `RESTORE_MAPPING` JSON is set on every pod's initContainer.
   - Each pod reads its own entry using its `fqdn`. The `pitr.node_backups[fqdn]` key selects the correct base backup per node.
   - If the mapping has a `pitr` block but `pitr.enabled = false`, the initContainer fails (D5).
5. **`snapshot_time != None` as a PITR-capable flag** — Explain that `node_backup.snapshot_time is not None` is the storage-readable signal that a backup is PITR-capable. The operator can read this from object storage metadata to gate PITR restores without needing access to the control-plane PITR config. Backups taken while `pitr.enabled = false` carry no `snapshot_time` and cannot be PITR bases.
6. **Point-in-time semantics** — Document that `restore_point_in_time` is a mutation-timestamp cutoff (the CQL write timestamp), not a wall-clock receive-time cutoff. Include the caveat: a write with a backdated CQL `USING TIMESTAMP` before the cutoff will be replayed; a write with a future-dated CQL timestamp after the cutoff will be suppressed. Note the TTL caveat: replaying a commitlog does not resurrect cells whose TTL has expired. A cell that was live at the target timestamp may have since expired and will appear dead when the restored cluster is queried. This is a known limitation, not a Medusa-specific issue (design doc §7 open question 1).
7. **Rollout steps** (from design doc §6.5):
   1. Merge with `pitr.enabled = false` default. Verify no behaviour change in production.
   2. Enable in a non-production environment. Confirm archiver uploads with `GetCommitLogArchiveStatus`.
   3. Test a PITR restore on a non-production cluster.
   4. Enable in production with a purge window large enough to retain all desired recovery points.
8. **Capacity sizing guidance** — At 1 GB/s aggregate write rate across all nodes, approximately 86 TB/day of raw commitlog data accumulates in object storage. Operators must size object storage and set appropriate purge windows. See design doc §6.1 for details.
9. **Open questions** — List the three open questions from design doc §7 (TTL caveat, time resolution, UTC/GMT in commit logs) so operators know what is unresolved, and list the outcome of [PITR-15](./PITR-15-spike-timestamp-semantics.md) (open questions 2 and 3).
10. **Go/no-go checklist** — Embed the go/no-go checklist from the [README](./README.md#gono-go-checklist-for-enabling-pitr-in-production) at the end of the rollout steps section.
11. **Monitoring** - `GetCommitLogArchiveStatus` fields; alert on `pending_count` high for a sustained period, `running=false`, `status=FAILED` (design doc 6.3).
12. **`target_timestamp` format** - Unix epoch or ISO-8601; UTC when no offset (design doc 4.5).
13. **Object storage layout** - `<prefix>/<fqdn>/commitlogs/` (design doc 4.11).
14. **Volume requirement** - spool and staging dirs on the Cassandra data volume, shared by the Cassandra container and the Medusa sidecar. Spool exhaustion behaviour (design doc 6.4).
15. **Go client regeneration** - the k8ssandra-operator must regenerate its Go stubs from the new proto to call the two RPCs (design doc 4.10).

### Runbook: PITR Replay Failure (design doc §6.6)

Add a section to `docs/Point-in-Time-Recovery.md` titled `## Runbook: PITR Replay Failure`. It must cover both remediation paths from design doc §6.6:

- **Path (a) — Rollback to base snapshot** — When to use it (corrupted, missing, or unrecoverable segments). Step-by-step instructions.
- **Path (b) — Retry with adjusted target timestamp** — When to use it (segment gap, archiver not yet active at that timestamp). Step-by-step instructions.
- **Decision guide table** — The four-row symptom/action table from design doc §6.6 (`CommitLogReplayer` exception → path (a); `restore_point_in_time` parsing error → path (b); etc.).

### Link from existing docs

- Add a one-line reference to `docs/Restoring-a-full-cluster.md` pointing to `docs/Point-in-Time-Recovery.md` for PITR restores. Do not modify any other content in that file.

### Anti-requirements

- Do NOT duplicate content already written by PITR-01 in `medusa-example.ini`. Check first; only add what is missing.
- Do NOT document k8ssandra-operator internals beyond what is needed to understand the operator contract (RESTORE_MAPPING assembly and GetHostMap role).
- Do NOT document implementation details of internal Python modules or gRPC message schemas, except the three RPC field names the operator needs (`nodeBaseBackups`, `targetTimestampS`, `targetTimestamp`). The audience is operators, not developers.
- Do NOT invent requirements that are not in the design doc.

## Definition of Done (Acceptance Criteria)

> How will we know we did the work right?

- [ ] `medusa-example.ini` contains the `[pitr]` section with all fields, defaults, and inline comments.
- [ ] `docs/Point-in-Time-Recovery.md` exists and covers all fifteen topics listed above.
- [ ] The runbook section is present and contains both remediation paths and the decision guide table.
- [ ] `docs/Restoring-a-full-cluster.md` contains a one-line reference to the PITR guide.
- [ ] All links within the new doc resolve correctly (relative paths to design doc, `medusa-example.ini`, etc.).
- [ ] The go/no-go checklist from README is embedded in the rollout section.
- [ ] Linters (markdownlint or equivalent) pass on all modified files.
- [ ] CI passes cleanly.

## Proposed Implementation (Optional / Starting Point)

> Suggested technical approach. The engineer who owns this ticket owns the final decisions.

**Check for existing `[pitr]` in `medusa-example.ini` before writing:**

```bash
grep -n '\[pitr\]' medusa-example.ini
```

If the section is already present (added by PITR-01), skip that step. Otherwise, append the section at the end of the file.

**New doc file location:**

`docs/Point-in-Time-Recovery.md` — consistent with the naming pattern of existing docs (e.g. `docs/Restoring-a-full-cluster.md`, `docs/Performing-backups.md`).

**Source material:**

All content in this ticket traces directly to the design doc:
- §4.3 (config fields and defaults)
- §4.7 (`RESTORE_MAPPING` JSON, `PreparePitrRestore` RPC, `GetHostMap` operator role)
- §4.8 (point-in-time semantics, `commitlog_archiving.properties` samples)
- §6.1 (capacity sizing)
- §6.5 (rollout steps, go/no-go checklist)
- §6.6 (runbook, decision table)
- §7 (open questions)

Do not add content beyond what those sections specify.
