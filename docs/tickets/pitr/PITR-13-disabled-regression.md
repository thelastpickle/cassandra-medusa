# PITR-13: Regression Check — PITR Disabled Produces Zero Behaviour Change

- **Type:** Task
- **Size:** XS (< 2 ideal days)
- **Estimated Ideal Days:** 1 day (Est. Calendar Time: 2–3 days)
- **Parallel Track:** Track G — Quality
- **Blocked By / Dependencies:** PITR-02, PITR-06, PITR-08, PITR-10, PITR-11
- **Blocks:** None

---

## Purpose

> WHY are we doing this work? What problem does it solve and what value does it deliver?

This ticket provides a formal verification of Goal G3 and Success Criterion SC4: when `pitr.enabled = false` (the default), every existing backup, restore, and purge behaviour is identical to the behaviour before any PITR work was merged. It ensures that operators who do not configure PITR see no regressions — no new errors, no changed outputs, no altered side effects.

The check is not a new test suite. It is a structured CI gate: run the full existing unit and integration test suite with no `[pitr]` section in `medusa.ini` and assert that the pass/fail count is identical to the pre-PITR baseline.

## Requirements

> WHAT needs to be true for this work to be successful?

- Run the complete existing unit test suite (`pytest tests/`) with no `[pitr]` section in any `medusa.ini` fixture or config file.
- Run the complete existing integration test suite (`behave tests/integration/features/ --tags=-@pitr`) excluding any newly added PITR scenarios (tagged `@pitr`).
- Assert zero new test failures compared to the pre-PITR baseline. If the baseline has known skips or failures, document them; the regression check must produce the same set.
- Verify that existing config fixture files (`tests/resources/` or similar) do not contain a `[pitr]` section. If any were accidentally added during earlier PITR tickets, remove them.
- Verify that the default `MedusaConfig` produced by `load_config` without a `[pitr]` section sets `pitr.enabled = False` and all other PITR fields to their documented defaults (covered by unit test added in PITR-01; confirm it still passes here).
- Confirm with pitr.enabled=false that no snapshot_time blob is written (PITR-02).
- Anti-requirement: Do NOT add new test logic. This ticket documents the check procedure and confirms it passes. If a new fixture or CI job configuration is required to enforce the check, that is the only new artefact.
- Anti-requirement: Do NOT alter any existing test. If an existing test fails after PITR changes, the root cause must be fixed in the relevant implementation ticket (PITR-06, PITR-08, PITR-10, or PITR-11), not by modifying the test.

## Definition of Done (Acceptance Criteria)

> How will we know we did the work right?

- [ ] `pytest tests/` (excluding PITR-specific test files) passes with zero new failures on a configuration with no `[pitr]` section.
- [ ] `behave tests/integration/features/ --tags=-@pitr` passes with zero new failures.
- [ ] No `[pitr]` section appears in any fixture or default config file used by non-PITR tests.
- [ ] CI pipeline includes a dedicated step or job that runs the full suite with `pitr.enabled` absent from config, labelled `regression-pitr-disabled` (or equivalent), blocking merge.
- [ ] The CI step result is recorded (pass/fail count) and attached to the PR or stored in CI artefacts.
- [ ] Linters and CI pass cleanly.

## Proposed Implementation (Optional / Starting Point)

> Suggested technical approach. The engineer who owns this ticket owns the final decisions.

**Step 1 — Audit config fixtures.**

```bash
grep -r '\[pitr\]' tests/
```

Remove any accidental `[pitr]` additions. The only acceptable location for `[pitr]` in test files is in files explicitly dedicated to PITR tests (e.g. `tests/config_test.py` PITR section, `tests/integration/features/pitr_tests.feature`).

**Step 2 — Run unit suite.**

```bash
pytest tests/ -v --ignore=tests/integration 2>&1 | tee unit_regression.txt
```

Compare the summary line to the pre-PITR baseline documented in the PR description.

**Step 3 — Run integration suite (non-PITR scenarios).**

```bash
behave tests/integration/features/ --tags=-@pitr --no-capture 2>&1 | tee integration_regression.txt
```

**Step 4 — Add CI job.**

In the CI configuration (`.github/workflows/` or equivalent), add a job `pitr-regression` that:

1. Checks out the branch.
2. Runs step 2 and step 3 above.
3. Fails the job if new failures are detected.

The job should run in parallel with the main test job, not block it, but must pass before merge.
