# PITR-15: Spike - restore_point_in_time resolution and UTC semantics

- **Type:** Spike
- **Size:** XS (<2 ideal days)
- **Estimated Ideal Days:** 1 day (Est. Calendar Time: 2-3 days)
- **Parallel Track:** Track E - Restore
- **Blocked By / Dependencies:** None
- **Blocks:** PITR-07 (format string), PITR-12 (timestamp gap)

---

## Purpose
> WHY are we doing this work?

Design doc section 7, questions 2 and 3, are open. PITR-07 hard-codes `%Y:%m:%d %H:%M:%S` in UTC. If Cassandra reads another resolution or timezone, restores stop at the wrong point.

## Requirements
> WHAT needs to be true?

- Read the Cassandra source (`CommitLogArchiver`, `CommitLogReplayer`, the `restore_point_in_time` parser) for the supported Cassandra versions in `docs/` and answer:
  1. Is a resolution finer than seconds supported? How does the `precision` setting (`restore_precision`) change which mutations replay?
  2. Are mutation timestamps and `restore_point_in_time` both read as UTC/GMT? Is any local-timezone conversion applied?
- Verify the answer with a ccm run: write rows with known `USING TIMESTAMP`, replay with a known cutoff, count rows.
- Update design doc section 7: replace each open question with the finding, or keep it open with the evidence.
- If the finding changes the format or adds `restore_precision`, create or update the PITR-07 and PITR-10 requirements.

## Definition of Done (Acceptance Criteria)

- [ ] Both questions answered with source references and the ccm result.
- [ ] Design doc section 7 updated.
- [ ] PITR-07 and PITR-10 updated if needed.

## Proposed Implementation (Optional / Starting Point)

- Cassandra source: `org.apache.cassandra.db.commitlog.CommitLogArchiver`.
- Reuse the ccm helpers in `tests/integration/features/steps/integration_steps.py`.
