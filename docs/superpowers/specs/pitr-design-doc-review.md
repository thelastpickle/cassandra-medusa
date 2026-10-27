# Design Doc Review: Point-in-Time Recovery (PITR) for Cassandra Medusa

> Reviewed: 2025-07-14
> Doc: `docs/superpowers/specs/pitr-design-doc.md`

## Summary

The doc is thorough and well-structured. However, it contains **7 internal inconsistencies**, two of which are blockers: `restore.py` is simultaneously described as "existing code, unchanged" and as having a new `backup_name` parameter with new `pitr`-block lookup logic; and `next_backup.min_snapshot_time` is used in both the prose and the code snippet despite `find_next_backup` returning a `NodeBackup` (which has `snapshot_time`, not `min_snapshot_time`). These two alone would produce a runtime `AttributeError` and a functional breakage if implemented verbatim. The doc should be revised to resolve all inconsistencies before implementation begins.

---

## Inconsistencies

### ~~I1 — `restore.py` is simultaneously "unchanged" and "modified"~~ ✅ Resolved

**Fix applied:** §4.7 previously said *"existing code, unchanged"*; reworded to *"the same inline-JSON parsing path used today; the `pitr` block is a new optional key handled by the modified `restore_backup()` described in §4.8"*. The contradiction is eliminated.

---

### ~~I2 — `next_backup.min_snapshot_time` used on a `NodeBackup`~~ ✅ Resolved

**Fix applied:** Both occurrences replaced with `next_backup.snapshot_time` — the correct `NodeBackup` attribute. `min_snapshot_time` remains correct only where it is used on `ClusterBackup` (§4.2 file map, §4.9 purge, §6.5 tests).

---

### ~~I3 — `pitr` block ownership split between `restore.py` and `pitr_restore.py`~~ ✅ Dismissed

Both modules legitimately read `pitr.node_backups[fqdn]` for different purposes:
- `restore.py` reads it to select the correct base SSTable backup to restore.
- `pitr_restore.py` reads it to know which backup's `snapshot_time` to use as the commitlog segment lower bound.

These are independent lookups of the same data, not a single-owner conflict. No doc change required.

---

### ~~I4 — `apply_mapping_env()` is unmodified but must support new `pitr` lookup~~ ✅ Resolved

**Fix applied:** Removed the claim that `apply_mapping_env()` is unmodified. §4.8 now explicitly describes both required changes:
- `apply_mapping_env()` is modified to return `(in_place, mapping)` instead of `bool | None`, making the full parsed `RESTORE_MAPPING` available to the caller.
- `restore_backup()` gains a `mapping=None` parameter; when a `pitr` block is present it resolves `backup_name` from `mapping["pitr"]["node_backups"][fqdn]`, otherwise falls back to `os.environ["BACKUP_NAME"]`.

---

### I5 — `pitr_restore.py` path ambiguity between §4.1 diagram and §4.5

- §4.1 architecture diagram: `RPC_HANDLER -->|calls| PITR_MODULE[medusa/pitr_restore.py]`
- §4.5 heading: *"`medusa/pitr_restore.py` — Pure Restore Logic"*
- §4.2 file map: `medusa/service/grpc/pitr_restore.py` — Create — *Per-node PITR & archiving restore entrypoint*; `medusa/pitr_restore.py` — Create — *Pure restore logic*

The architecture diagram shows `server.py`'s `PreparePitrRestore` handler calling `medusa/pitr_restore.py` (the pure-logic module). This is correct per the design intent, but §4.5's heading reads *"pitr_restore.py — Pure Restore Logic"* while §1 scope says the entrypoint is `medusa/service/grpc/pitr_restore.py`. The diagram labels are ambiguous and may cause confusion about which file the RPC handler delegates to.

---

### I6 — `upper_bound_s = None` semantics never exercised by the design's own code

- §4.5 `find_commitlog_segments` signature: *"When `upper_bound_s` is `None`, no upper bound is applied — callers must always pass a computed value; `None` is reserved for cases where no bound can be determined."*
- §4.8 code: `else: upper_bound_s = time.time()` (when no next backup exists)

The function contract documents `None` as the value for "no bound can be determined", but the only caller in the design never passes `None` — it passes `time.time()` in the no-next-backup branch. Either the `None` path should be removed from the contract, or the caller should be updated to pass `None` and the function doc should explain what "no upper bound" means in practice.

---

### I7 — `CommitLogArchiver.__init__` raises `RuntimeError` but the crash path is not fully traced

- §4.4: *"`CommitLogArchiver.__init__` checks… raises `RuntimeError` immediately…"*
- §4.4 (immediately after): *"Because `CommitLogArchiver` is **initialized** in `Server.serve()`…, this exception is intentionally unhandled at startup: it **fails fast and crashes Medusa immediately**."*

The doc states `__init__` raises but does not show how a `RuntimeError` from the constructor propagates out of `Server.serve()` to crash the process. The section implies the exception is unhandled in `serve()`, but §4.4 earlier says "Starting it in `serve()` (rather than `__init__`) avoids thread leaks" — conflating where the object is *constructed* with where the exception *propagates*. The failure path needs a single clear statement.

---

## Recommended Actions

All inconsistencies have been resolved. Summary:

1. ~~**(Blocker)** Resolve the `restore.py` modified-vs-unchanged contradiction (I1, I4)~~ — **✅ Resolved** (see I1 and I4 above).
2. ~~**(Blocker)** Fix `next_backup.min_snapshot_time` → `next_backup.snapshot_time` in §4.3 prose and code snippet (I2)~~ — **✅ Resolved** (see I2 above).
3. ~~Clarify `pitr` block ownership (I3): state once, in one section, which module reads `RESTORE_MAPPING["pitr"]["node_backups"][fqdn]`~~ — **✅ Dismissed** (see I3 above; both modules legitimately own independent lookups).
4. ~~Reconcile the `upper_bound_s = None` contract with the actual calling code (I6)~~ — **✅ Fixed** (`None` semantics sentence removed from §4.5; callers always pass a concrete float).
5. ~~Relabel the §4.1 architecture diagram node from `medusa/pitr_restore.py` to the fully-qualified path `medusa/service/grpc/pitr_restore.py` where the RPC handler is concerned, to eliminate path ambiguity (I5)~~ — **✅ Fixed** (both `pitr_restore.py` nodes in the §4.1 diagram now carry fully-qualified paths: the coordinator node uses `medusa/pitr_restore.py`, the initContainer node uses `medusa/service/grpc/pitr_restore.py`).
6. ~~Add one sentence to §4.4 explicitly stating that `RuntimeError` from `CommitLogArchiver.__init__` is allowed to propagate out of `Server.serve()` uncaught, and confirm this is the intended crash-fast path (I7)~~ — **✅ Fixed** (sentence added to §4.4).
