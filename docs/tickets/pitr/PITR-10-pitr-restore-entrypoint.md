# PITR-10: Implement `medusa.service.grpc.pitr_restore` entrypoint and update `docker-entrypoint.sh`

- **Type:** Feature
- **Size:** S (2-5 ideal days)
- **Estimated Ideal Days:** 4 days (Est. Calendar Time: 8-10 days)
- **Parallel Track:** Track E - Restore
- **Blocked By / Dependencies:** PITR-01, PITR-07, PITR-09
- **Blocks:** PITR-12, PITR-13, PITR-14

---

## Purpose
> WHY are we doing this work?

After `PreparePitrRestore` succeeds, the operator restarts the pods. Each pod must restore itself. It needs the commitlog segments up to the target time. It also needs a correct `commitlog_archiving.properties` file.

This ticket adds the initContainer step that does this work. Cassandra then replays the segments at startup. It also keeps archiving on after the restore. No Medusa code starts or stops Cassandra.

Without this ticket, the SSTable restore from PITR-09 gives data at snapshot time only. PITR to an arbitrary time does not work.

Design doc reference: section 4.8.

## Requirements
> WHAT needs to be true for this work to be successful?

### New module `medusa/service/grpc/pitr_restore.py`

- Import `resolve_node_fqdn` from `medusa.service.grpc.restore` (PITR-09). Do not redefine it.
- Add `main()`. It reads `sys.argv[1]` (config path) and `sys.argv[2]` (restore key). If `sys.argv[2]` is missing, use the `RESTORE_KEY` env var, or `""`.
- Parse `RESTORE_MAPPING` first.
- If `config.pitr.enabled` is false:
  - and `"pitr"` is in the mapping: log at ERROR and `sys.exit(1)` (replay was requested but cannot run).
  - otherwise: log at INFO and return. Write no file. Touch no directory.
- On every run when PITR is enabled: empty and recreate `config.pitr.commitlog_staging_dir` first.
- Read `RESTORE_MAPPING` with `json.loads()`. Default to `{}` when not set.
- Read `/var/lib/cassandra/.last-restore`. Strip whitespace. Use `""` when the file is absent.
- Compute `is_pitr_restore`:
  - It is `True` only when `"pitr"` is in the mapping AND `restore_key` is not empty AND `.last-restore` value differs from `restore_key`.
  - It MUST be a real `bool`. Wrap the expression in `bool(...)`.
  - An empty `restore_key` MUST give `False`, even when the mapping has a `pitr` block.
- If `is_pitr_restore` is `True`:
  - Read `target_timestamp_s` from `mapping["pitr"]`.
  - Resolve `fqdn` with `resolve_node_fqdn`.
  - Read `backup_name` from `mapping["pitr"]["node_backups"][fqdn]`.
  - Open `Storage(config=config.storage)` in a `with` block.
  - Load the base backup with `storage.get_node_backup(fqdn=fqdn, name=backup_name)`. Use `base_backup.started` as `after_snapshot_time_s` (lazy `snapshot_time` is not used for the lower bound; design doc 4.5).
  - Call `find_next_backup(storage, fqdn, base_backup)`.
    - If a next backup exists: `upper_bound_s = next_backup.snapshot_time + config.pitr.commitlog_restore_grace_period_seconds`.
    - Else: `upper_bound_s = time.time()`.
  - Always pass `upper_bound_s` to `find_commitlog_segments`. Never call it without a bound.
  - Call `download_commitlog_segments` with `config.pitr.commitlog_staging_dir`.
  - Write `commitlog_archiving.properties` with `archive_command` AND `restore_directories` AND `restore_point_in_time` (use `generate_commitlog_archiving_properties`).
- If `is_pitr_restore` is `False`:
  - Do not open storage. Do not download.
  - Write `commitlog_archiving.properties` with `archive_command` only. No `restore_directories`. No `restore_point_in_time`.
- Write the properties file to `Path(config.cassandra.config_file).parent / "commitlog_archiving.properties"`.
- `archive_command` is `/bin/ln %path {config.pitr.commitlog_spool_dir}/%name`.
- Add the `if __name__ == '__main__':` guard.
- Reuse `create_config` and `configure_console_logging` from `medusa/service/grpc/restore.py`. Do not copy them.

### Changes to `k8s/docker-entrypoint.sh`

- Detect a PITR mapping by parsing JSON (`poetry run python -c 'import json,sys; ...'`), not with a regex match on the raw string (a host name containing "pitr" would match).
- Step 1 runs when `RESTORE_KEY` is set AND (`BACKUP_NAME` is set OR the mapping has a `pitr` key) AND `.last-restore` differs from `RESTORE_KEY`.
- Step 2 always runs.
- Step 3: write `RESTORE_KEY` to `.last-restore` after step 2 succeeds, only if step 1's conditions (key + name/pitr) held. Never write an empty key.
- Remove both early `exit 0` lines.
- `set -e` stays.

### Scenario behaviour

| Scenario | `.last-restore` vs `RESTORE_KEY` when step 2 starts | Result |
|---|---|---|
| First restore attempt | File absent → no match | Full SSTable restore, segments downloaded, restore properties written, then key recorded |
| Pod crash mid-download (step 2) | Not yet written | Both steps re-run on restart. Correct |
| Pod crash after step 1, before step 2 ends | Not yet written | Both steps re-run (SSTable restore repeats; it is idempotent) |
| Pod restart after successful PITR restore | Match | Step 1 skipped; step 2 stages 0 segments and writes archiving-only properties (no replay) |
| New restore job | No match (new `RESTORE_KEY`) | Full execution. Correct |
| `RESTORE_KEY` empty, `pitr` in mapping | n/a | `is_pitr_restore` is `False`; archiving-only properties; `.last-restore` not written |

### Cleanup guarantees

- The staging directory is emptied on every run when PITR is enabled.
- Restore settings are absent from the properties file whenever restore is skipped.
- If a pod crashes during step 1 or step 2, `.last-restore` has not been written (step 3 runs last). On restart, step 2 empties the staging directory and everything re-runs idempotently.

### Anti-requirements

- MUST NOT start or stop Cassandra.
- MUST NOT use the SSH `Orchestration` layer.
- MUST NOT change behaviour of non-PITR restores. A mapping with no `pitr` key follows the old SSTable path.
- MUST NOT call `find_commitlog_segments` without `upper_bound_s`.
- MUST NOT use a truthy non-bool value for `is_pitr_restore`.
- MUST NOT write `restore_directories` or `restore_point_in_time` when restore is skipped.
- MUST NOT write `.last-restore`. The shell script writes it after this step (design doc 4.8).

### Required unit tests: `tests/service/grpc/pitr_restore_test.py`

Mock `Storage`, the `medusa.pitr_restore` functions, and the filesystem (use `tmp_path`).

- `resolve_node_fqdn`:
  - `POD_NAME` in `host_map` returns `source[0]`.
  - `POD_NAME` not in `host_map` falls back to `POD_IP`.
  - No `POD_NAME` and no `POD_IP` falls back to `config.storage.fqdn`.
- `main()` with `pitr.enabled = false`: no staging clean, no properties file, no storage access.
- Active restore: staging is cleaned, correct segments are downloaded, properties contain `archive_command`, `restore_directories` and `restore_point_in_time`.
- Upper bound with next backup: `find_commitlog_segments` gets `next_backup.snapshot_time + grace_period`.
- Upper bound without next backup: `find_commitlog_segments` gets a value from a patched `time.time()`.
- Skip path (`.last-restore` equals `RESTORE_KEY`): no storage access, archiving-only properties.
- No `pitr` key in mapping: archiving-only properties.
- `restore_key = ""` with a `pitr` mapping: `is_pitr_restore` is `False`, no download, archiving-only properties. Assert the value is `False` and not `""`.
- Staging directory with old files: files are gone after `main()`.
- Properties path is `Path(config.cassandra.config_file).parent / "commitlog_archiving.properties"`.
- Shell script check: a test or script check runs `bash -n k8s/docker-entrypoint.sh`. Cover the step 1 condition by reading the script text or by running it with stubbed `poetry`.
- `pitr.enabled=false` with a `pitr` block in the mapping: `main()` exits with code 1, no file written.
- Shell test (`tests/k8s/docker_entrypoint_test.sh` or a pytest using `subprocess`): run `restore()` with stub `poetry` and a temp `.last-restore`. Assert: (a) first run calls restore, then pitr_restore, then writes the key; (b) step 2 failure leaves the key unwritten; (c) second run skips step 1 and still runs step 2; (d) empty `RESTORE_KEY` writes no key; (e) a host name containing "pitr" in `host_map` without a `pitr` key does NOT trigger step 1.

## Definition of Done (Acceptance Criteria)
> How will we know we did the work right?

- [ ] On a pod with a valid `pitr` mapping and a new `RESTORE_KEY`, the staging dir holds the expected segments and the properties file has all three keys.
- [ ] On pod restart after a finished restore, the properties file has `archive_command` only and staging is empty.
- [ ] A restore with empty `RESTORE_KEY` and a `pitr` mapping does not replay.
- [ ] A non-PITR restore (no `pitr` key) works as before.
- [ ] `docker-entrypoint.sh` always runs step 2, including when `BACKUP_NAME` and `RESTORE_KEY` are unset.
- [ ] `bash -n k8s/docker-entrypoint.sh` passes.
- [ ] Unit test coverage implemented (>= 80% coverage for new logic) in `tests/service/grpc/pitr_restore_test.py`.
- [ ] Docstrings and script comments updated.
- [ ] Linters (`flake8`) and CI pass cleanly.
- [ ] `.last-restore` is written after step 2, not after step 1.

## Proposed Implementation (Optional / Starting Point)
> Suggestion only. The engineer who owns the ticket owns the final decisions.

- Model the new module on [`medusa/service/grpc/restore.py`](../../../medusa/service/grpc/restore.py). Import `create_config` and `configure_console_logging` from it. Use the same `if __name__ == '__main__':` argument pattern (`sys.argv[1]` config path, `sys.argv[2]` restore key). Keep `Storage` usage as `with Storage(config=config.storage) as storage:`, as in `restore_backup()`.
- Copy the code sketch in design doc section 4.8 as a base. Change `is_pitr_restore` to `bool("pitr" in mapping and restore_key and last_restore_val != restore_key)`.
- Use `shutil.rmtree(..., ignore_errors=True)` then `Path.mkdir(parents=True, exist_ok=True)` for a `clean_staging_dir` helper.
- Use the helpers from PITR-07 in [`medusa/pitr_restore.py`](../../../medusa/pitr_restore.py): `find_next_backup`, `find_commitlog_segments`, `download_commitlog_segments`, `generate_commitlog_archiving_properties`.
- In [`k8s/docker-entrypoint.sh`](../../../k8s/docker-entrypoint.sh), rewrite the `restore()` function using the exact snippet from design doc 4.8:

```bash
restore() {
    echo "Running Medusa in restore mode"
    last_restore_file=/var/lib/cassandra/.last-restore

    # True when RESTORE_MAPPING is JSON with a top-level "pitr" key.
    has_pitr=false
    if [ -n "$RESTORE_MAPPING" ] && echo "$RESTORE_MAPPING" | \
        poetry run python -c 'import json,sys; sys.exit(0 if "pitr" in json.load(sys.stdin) else 1)'; then
        has_pitr=true
    fi

    # 1. SSTable restore: needs RESTORE_KEY and either BACKUP_NAME or a PITR mapping.
    restore_requested=false
    if [ -n "$RESTORE_KEY" ] && { [ -n "$BACKUP_NAME" ] || [ "$has_pitr" = true ]; }; then
        restore_requested=true
        last_key=""
        [ -f "$last_restore_file" ] && last_key=$(cat "$last_restore_file")
        if [ "$last_key" == "$RESTORE_KEY" ]; then
            echo "Skipping SSTable restore operation"
        else
            echo "Restoring SSTable snapshot"
            poetry run python -m medusa.service.grpc.restore "/etc/medusa/medusa.ini" "$RESTORE_KEY"
        fi
    fi

    # 2. Always run: stage segments (if needed) and write commitlog_archiving.properties.
    #    This step reads .last-restore to decide if a PITR replay is due.
    echo "Configuring PITR and commitlog archiving settings"
    poetry run python -m medusa.service.grpc.pitr_restore "/etc/medusa/medusa.ini" "$RESTORE_KEY"

    # 3. Record the restore only after BOTH steps succeeded (set -e aborts earlier on failure).
    #    A crash in step 1 or 2 leaves the old key, so the next start re-runs both steps.
    if [ "$restore_requested" = true ]; then
        echo "$RESTORE_KEY" > "$last_restore_file"
    fi
}
```

- Existing test layout to follow: [`tests/service/grpc/restore_test.py`](../../../tests/service/grpc/restore_test.py).

---

## Why this matters to you
This ticket is the point where all restore parts meet on a real pod. If it is wrong, a pod could replay old data or lose archiving. Take care with the empty `RESTORE_KEY` case and the `.last-restore` rules. Ask the PITR-07 and PITR-09 owners if an interface is unclear.
