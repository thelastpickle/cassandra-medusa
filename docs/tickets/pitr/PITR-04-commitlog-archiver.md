# PITR-04: Implement `CommitLogArchiver` thread

- **Type:** Feature
- **Size:** S (2-5 ideal days)
- **Estimated Ideal Days:** 4 days (Est. Calendar Time: 7-9 days)
- **Parallel Track:** Track C - Archiver
- **Blocked By / Dependencies:** PITR-01
- **Blocks:** PITR-06

---

## Purpose
> WHY are we doing this work?

PITR needs the commitlog segments of each node in object storage. Cassandra hardlinks each sealed segment into a spool directory (`archive_command`). Today, nothing uploads those files.

This ticket adds the background thread that uploads them. Without it, no recovery point exists between two backups. The thread must be safe: a bad upload must not crash the gRPC server, and a bad mount must be found at startup, not at first failure.

## Requirements
> WHAT needs to be true for this work to be successful?

Create `medusa/service/grpc/commitlog_archiver.py` (design doc 4.4). It holds the class `CommitLogArchiver(threading.Thread)` and a `drain_spool()` helper.

- Use /bin/ln in any example of archive_command (design doc 4.3).

**Thread**
- The class is a `threading.Thread` subclass. It is a daemon thread.
- It reads `pitr.commitlog_spool_dir` and `pitr.commitlog_archive_interval_seconds` from the `PitrConfig` added in PITR-01.
- It must NOT start itself in `__init__`. The caller calls `start()` (PITR-06).
- The thread polls in a loop. It waits on a `threading.Event`, not on `time.sleep`, so `stop()` returns fast.

**Startup validation (`__init__`)**
- Compare `os.stat(commitlog_spool_dir).st_dev` with `os.stat(<cassandra commitlog dir>).st_dev`.
- Raise `RuntimeError` if the values differ.
- Raise `RuntimeError` if either directory is missing or not accessible (`OSError`, including `FileNotFoundError`).
- The message must name both paths and say that hardlinks cannot cross filesystem boundaries. Use the text from design doc 4.4.
- Do not catch this error in the class. It must reach the caller (SC6).

**Spool drain (`drain_spool`)**
- List all files in `commitlog_spool_dir`.
- For each file, check `<prefix>/<fqdn>/commitlogs/<filename>` in storage:
  - Absent: upload.
  - Present and size matches: skip. If `[checks].enable_md5_checks` is true, also compare the MD5 hash.
  - Present and size differs: upload again (partial upload from an earlier crash).
- After a successful upload, or a confirmed skip, remove the hardlink from the spool.
- After a failed upload, log at `ERROR`. Leave the hardlink in the spool. Go on to the next file.
- Keep the segment file name unchanged in storage (design doc 4.11).

**Upload loop (`_run_once`)**
- Call `drain_spool()`. Finish all files in the spool before the thread waits for the next tick (SC2). A slow batch delays the next tick. The tick does not interrupt the batch.
- Update `last_upload_time` and `last_error` under a lock. The lock guards only the counters read by `status()`.
- Catch any exception from `drain_spool()`. Log it. Keep the thread alive (SC3).
- `_run_once` is called from one thread only. Do not add other locking.

**`last_error` semantics (design doc 4.4, 6.4)**
- If the spool directory cannot be read in a tick: set `last_error` to the exception message. Do NOT reset `pending_count`. Keep its last known value. Do not crash.
- If the spool directory is read fine: set `last_error` to the empty string.
- `pending_count=0` with `last_error=""` means the spool is empty.
- `pending_count=0` with a non-empty `last_error` means the spool is not readable.

**Lifecycle**
- `stop()` sets the `threading.Event`. Then it calls `join(timeout=interval + 5)`.
- `status()` returns a `dict` with these keys:
  - `running` (bool)
  - `last_upload_time` (Unix epoch seconds, `0` if never)
  - `pending_count` (int): number of files in `commitlog_spool_dir`, listed **on every `status()` call** (not cached from the last tick). If the listing fails, keep the last known value and set `last_error`.
  - `interval_seconds` (int)
  - `status` (`StatusType`: `IN_PROGRESS` while the thread is alive, `FAILED` if it stopped without `stop()`)
  - `last_error` (string)

**Failure modes (design doc 6.4) the code must follow**

| Scenario | Required behaviour |
|---|---|
| Spool and commitlog on different devices, or a directory is missing at startup | `RuntimeError` from `__init__` |
| Upload failure (network, quota) | Log `ERROR`. Keep the file. Retry next tick. Server stays up. |
| Spool read error during a tick | Set `last_error`. Keep `pending_count`. No crash. |
| Partial upload (size mismatch) | Upload again on the next tick |
| Server pod restart | Size and hash check stops double uploads |
| Disk full from long upload failures | Only failed segments stay. `pending_count` rises. It clears when storage works again. |

**Logging (design doc 6.3)**
- `INFO` after each successful upload batch.
- `ERROR` or `WARNING` on upload failure and on directory access errors.

**Anti-requirements**
- MUST NOT read the live Cassandra commitlog directory. Read the spool only.
- MUST NOT copy file data to make the spool entry. Cassandra makes the hardlink.
- MUST NOT delete a spool file before the upload (or the skip check) succeeds.
- MUST NOT add new config fields. PITR-01 owns config.
- MUST NOT wire the thread into `Server`. PITR-06 does that.

**Required tests** (`tests/service/grpc/commitlog_archiver_test.py`, new file)
- `__init__` raises `RuntimeError` when `os.stat` returns different `st_dev` values (SC6).
- `__init__` raises `RuntimeError` when `os.stat` raises `FileNotFoundError` for either directory (SC6).
- `__init__` succeeds when both `st_dev` values match.
- `drain_spool` with an empty spool does nothing.
- `drain_spool` uploads a file that is absent in storage, then removes it from the spool.
- `drain_spool` skips a file that is present with the same size, then removes it from the spool.
- `drain_spool` with `enable_md5_checks=true` compares the hash.
- `drain_spool` uploads again a file that is present with a different size.
- `_run_once` with a mock storage that returns N files: all N are uploaded and removed before the wait call (SC2).
- `_run_once` when the upload raises: no exception leaves `_run_once`. The segment is still in the spool (SC3).
- `_run_once` when the spool read fails: `last_error` is set, `pending_count` is unchanged.
- `_run_once` after a failed tick then a good tick: `last_error` is the empty string.
- `status()` returns `IN_PROGRESS` while alive and `FAILED` after an unexpected stop.
- `stop()` ends the thread within `interval + 5` seconds.
- `status()` after N files are written to the spool and no tick ran: `pending_count == N` (SC8b).
- `status()` when the spool listing raises: `pending_count` keeps its last value and `last_error` is set.

## Definition of Done (Acceptance Criteria)
> How will we know we did the work right?

- [ ] `CommitLogArchiver` fails fast with `RuntimeError` on a device mismatch or a missing directory (SC6).
- [ ] One `_run_once` uploads every file in the spool before the thread waits again (SC2).
- [ ] A failed upload does not raise out of `_run_once`. The segment stays in the spool and is retried (SC3).
- [ ] `last_error` and `pending_count` follow the rules in this ticket.
- [ ] `status()` returns all six keys with the right types.
- [ ] All tests listed above exist in `tests/service/grpc/commitlog_archiver_test.py` and pass.
- [ ] Unit test coverage is >= 80% for the new module.
- [ ] Docstrings or comments explain the `last_error` rules and the hardlink same-device rule.
- [ ] Linters and CI pass cleanly (`flake8`, unit tests).

## Proposed Implementation (Optional / Starting Point)
> Suggested approach. You own the final design.

- New file: `medusa/service/grpc/commitlog_archiver.py`. The `medusa/service/grpc/` package has no such file yet. Use the license header from [`server.py`](../../../medusa/service/grpc/server.py).
- Constructor inputs (suggestion): the Medusa config (for `config.pitr`, `config.storage`, `config.checks`, `config.cassandra`). Keep the signature small and easy to mock.
- Cassandra commitlog dir: use `CassandraConfigReader(config.cassandra.config_file).commitlog_directory` from [`medusa/cassandra_utils.py`](../../../medusa/cassandra_utils.py:256). It falls back to `/var/lib/cassandra/commitlog`. It raises `RuntimeError` if the `cassandra.yaml` file is missing. Check that this fits the "inaccessible directory" case in SC6.
- Storage: open it with `Storage(config=config.storage)` from [`medusa/storage/__init__.py`](../../../medusa/storage/__init__.py) as a context manager, as `MedusaService.PrepareRestore` does in [`server.py`](../../../medusa/service/grpc/server.py:329). Prefer one connection per tick. The thread owns it.
- Existence and size check: `AbstractStorage.get_blob(path)` in [`medusa/storage/abstract_storage.py`](../../../medusa/storage/abstract_storage.py:203) returns `None` for a missing object. Check what size and hash fields the blob object has before you write the compare.
- Upload: `AbstractStorage.upload_blobs(srcs, dest)` ([`abstract_storage.py`](../../../medusa/storage/abstract_storage.py:166)) takes a list of source paths and a destination folder without the file name. The storage key must be `<prefix>/<fqdn>/commitlogs/<filename>`. Check how the prefix and fqdn are built in `Storage` and in the backup upload code, so the path is the same.
- Wait: use `self._stop_event.wait(interval)` for the tick delay. Tests can then mock the event and check call order for SC2.
- Tests: follow the style in [`tests/service/grpc/server_test.py`](../../../tests/service/grpc/server_test.py) (`unittest` with `unittest.mock`). Use `tempfile.TemporaryDirectory` for the spool. Mock `os.stat` for the SC6 cases.

---

## Why this matters
> Closing context for the engineer.

This thread is the only source of recovery points between backups. If it is quiet, wrong, or crashes, operators find out during a restore, which is too late. Clear logs and an honest `status()` are part of the feature. Thanks for keeping it simple and safe.
