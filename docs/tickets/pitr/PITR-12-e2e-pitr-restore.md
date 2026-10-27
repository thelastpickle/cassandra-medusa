# PITR-12: E2E Integration Test — PITR Restore

- **Type:** Task
- **Size:** S (2–5 ideal days)
- **Estimated Ideal Days:** 5 days (Est. Calendar Time: 10–12 days)
- **Parallel Track:** Track G — Quality
- **Blocked By / Dependencies:** PITR-02, PITR-06, PITR-08, PITR-10, PITR-15
- **Blocks:** None

---

## Purpose

> WHY are we doing this work? What problem does it solve and what value does it deliver?

This ticket delivers the primary verification of PITR correctness (Success Criterion SC1). It proves that a full PITR restore returns the cluster to the exact state that existed at a known target timestamp — all rows written before that timestamp are present, and all rows written after it are absent. Without this test, the end-to-end restore path is unverified by automation, leaving operators to discover regressions in production.

The test extends the existing Behave integration framework in [`tests/integration/features/`](../../../tests/integration/features/) with a new scenario for PITR. All five steps from design doc §6.5 are covered in sequence.

## Requirements

> WHAT needs to be true for this work to be successful?

- Add a new scenario to [`tests/integration/features/integration_tests.feature`](../../../tests/integration/features/integration_tests.feature) tagged `@pitr` (or a new feature file `pitr_tests.feature` in the same directory, following the existing pattern).
- The scenario must follow these five steps from design doc §6.5:
  1. Start the cluster. Run an initial workload. Take a full snapshot (use the existing `perform a backup` step).
  2. Run additional writes after the snapshot. Allow the `CommitLogArchiver` to upload segments (the test must wait until `GetCommitLogArchiveStatus` confirms `pending_count = 0` and `last_upload_time` is set).
  3. Record a target timestamp mid-workload (after the snapshot, before the final writes). Record the expected row count at that timestamp.
  4. Call `PreparePitrRestore` with the recorded `target_timestamp`. Assert the RPC returns a successful `nodeBaseBackups` map.
  5. Simulate the Kubernetes restore (no SSH `restore-cluster`): build `RESTORE_MAPPING` from the RPC response, stop Cassandra, run `medusa.service.grpc.restore` then `medusa.service.grpc.pitr_restore` with a new `RESTORE_KEY`, start Cassandra, then assert the row count (SC1).
- The scenario must run against the `local` storage provider at minimum (the `@local` tag pattern used in existing scenarios).
- The test must not leave a running archiver or staging files after the scenario ends. Clean up in the `environment.py` teardown or in a final test step.
- Step implementations go in [`tests/integration/features/steps/integration_steps.py`](../../../tests/integration/features/steps/integration_steps.py), following the existing step-definition pattern.
- All new step definitions must have unit-level coverage for their helper functions (e.g. timestamp recording, row counting at a point in time). Add or extend tests in [`tests/restore_cluster_test.py`](../../../tests/restore_cluster_test.py) or a new `tests/pitr_integration_test.py` as appropriate.
- The `CommitLogArchiver` must be started with `pitr.enabled = true` and a valid `commitlog_spool_dir` pointing to a temp directory used as the spool in the test environment.
- The Cassandra `archive_command` must be configured in the test ccm cluster to hardlink sealed segments into the spool directory.
- Force segment sealing before the wait: run `nodetool drain` (or restart the node) after the post-target writes, so the active segment is archived. `pending_count = 0` alone does not prove the target-window writes are in storage.
- Leave at least 2 seconds of idle time before and after the target timestamp. `restore_point_in_time` has one-second resolution.
- Use `/bin/ln %path <spool>/%name` as the ccm `archive_command` (design doc 4.3).
- Anti-requirement: Do NOT modify any existing scenario or step definition in a way that changes current test behaviour.
- Anti-requirement: Do NOT add a `pitr` section to the default `medusa.ini` used by non-PITR scenarios.

## Definition of Done (Acceptance Criteria)

> How will we know we did the work right?

- [ ] A new Behave scenario tagged `@pitr` runs all five integration steps from design doc §6.5 end-to-end.
- [ ] Row count at the target timestamp matches the expected value after restore (SC1 verified by automation).
- [ ] The scenario passes with `local` storage in CI (add to the `@local` examples block or a dedicated `@pitr_local` tag).
- [ ] `GetCommitLogArchiveStatus` is polled in the test until `pending_count = 0` before the restore is triggered — no hard sleeps.
- [ ] Teardown clears the spool directory and staging directory. No residual state between scenarios.
- [ ] New helper functions have ≥ 80% unit test coverage.
- [ ] Existing integration scenarios are not affected. All pre-existing `@local` tests still pass.
- [ ] CI configuration includes the new `@pitr` tag in the integration test run.
- [ ] Linters and CI pass cleanly.

## Proposed Implementation (Optional / Starting Point)

> Suggested technical approach. The engineer who owns this ticket owns the final decisions.

**Existing framework to extend:**

- Scenarios: [`tests/integration/features/integration_tests.feature`](../../../tests/integration/features/integration_tests.feature) — add a new `Scenario Outline` for PITR or create `pitr_tests.feature`.
- Steps: [`tests/integration/features/steps/integration_steps.py`](../../../tests/integration/features/steps/integration_steps.py) — add step definitions for `I record a target timestamp`, `I call PreparePitrRestore with the recorded timestamp`, `I wait for the archiver to drain the spool`, `the cluster state matches the row count at the target timestamp`.
- Environment hooks: [`tests/integration/features/environment.py`](../../../tests/integration/features/environment.py) — add teardown logic for spool and staging directories in `after_scenario` when the scenario tag is `@pitr`.

**Cassandra `archive_command` in ccm:**

Configure the ccm node's `commitlog_archiving.properties` with:
```
archive_command=/bin/ln %path <spool_dir>/%name
```
This hardlinks each sealed segment into `<spool_dir>` — matching the design doc §3 contract that Medusa never reads the live commitlog directory.

**Timing:**

Instead of a fixed sleep, poll `GetCommitLogArchiveStatus` in a loop with a short retry interval (e.g. 2 s, max 60 s). Assert `running = true` and `pending_count = 0` before proceeding to the restore step.

**Row count at target timestamp:**

Record the row count immediately after the additional writes in step 2 but before the post-target writes. Store it in `context.pitr_expected_rows`. After restore, query the table and compare.
