# PITR-06: Wire archiver into gRPC server + `GetCommitLogArchiveStatus`

- **Type:** Feature
- **Size:** S (2-5 ideal days)
- **Estimated Ideal Days:** 2 days (Est. Calendar Time: 4-5 days)
- **Parallel Track:** Track C - Archiver
- **Blocked By / Dependencies:** PITR-01, PITR-04, PITR-05
- **Blocks:** PITR-12, PITR-13, PITR-14

---

## Purpose
> WHY are we doing this work?

The archiver (PITR-04) and the API contract (PITR-05) exist, but nothing connects them. This ticket starts the archiver with the gRPC server when PITR is on, stops it on shutdown, and answers the `GetCommitLogArchiveStatus` call.

With PITR off, which is the default, the server must act exactly as it does today. Existing users must see no change.

## Requirements
> WHAT needs to be true for this work to be successful?

Edit [`medusa/service/grpc/server.py`](../../../medusa/service/grpc/server.py) (design doc 4.4, 4.1).

**Start (`Server.serve`)**
- If `pitr.enabled` is true, create a `CommitLogArchiver` and call `start()`.
- Start the archiver in `serve()`, NOT in `Server.__init__`. This avoids thread leaks in unit tests that build `Server` and never run it.
- Create the archiver before `grpc_server.start()`, so a bad mount stops the process before it accepts calls.
- Give the archiver to `MedusaService` so the status handler can read it.
- If `pitr.enabled` is false: create no archiver and no thread. Make no new log lines at `INFO` or above (SC4).

**Fail fast**
- `Server.serve()` MUST NOT catch the `RuntimeError` raised by `CommitLogArchiver.__init__`.
- The error must leave `serve()` and reach the process entry point. The process exits with a non-zero code. This is the wanted behaviour (design doc 4.4, 6.4, SC6).
- Do not wrap the archiver creation in a broad `try/except`.

**Stop (`Server.shutdown`)**
- If an archiver exists, call `stop()` on it.
- If no archiver exists, do nothing extra.
- Keep the existing calls (`handle_backup_removal_all`, `grpc_server.stop(0)`) and their behaviour.

**`GetCommitLogArchiveStatus` handler** (on `MedusaService`)
- No archiver configured: return `running=false`, `pendingCount=0`. The other fields use their defaults (SC8a).
- Archiver present: map `status()` into the response: `running`, `lastUploadTime`, `pendingCount`, `intervalSeconds`, `status`, `lastError`.
- `pendingCount` must come from the current contents of the spool (SC8b). N files in the spool means `pendingCount=N`.
- Follow the error style of the other handlers: on an unexpected exception, log with `logging.exception`, set `context.set_code(grpc.StatusCode.INTERNAL)` and `context.set_details(...)`, and return the response.
- Do not block the gRPC event loop for long. The handler only reads counters and lists a directory.

**Logging (design doc 6.3)**
- `INFO` when the archiver starts. Include the spool directory and the interval.
- `INFO` when the archiver stops.
- `ERROR` or `WARNING` for archiver problems comes from PITR-04. Do not duplicate it here.
- Do not log in the disabled path.

**Anti-requirements**
- MUST NOT change any existing RPC handler.
- MUST NOT change behaviour when `pitr.enabled = false` (SC4).
- MUST NOT add the `PreparePitrRestore` handler. PITR-08 owns it.
- MUST NOT catch `RuntimeError` from the archiver constructor.

**Required tests** (`tests/service/grpc/server_test.py`)
- `GetCommitLogArchiveStatus` with no archiver: `running=false`, `pendingCount=0` (SC8a).
- `GetCommitLogArchiveStatus` with N dummy files in a temp spool dir: `pendingCount=N` (SC8b).
- `GetCommitLogArchiveStatus` maps `lastUploadTime`, `intervalSeconds`, `status`, and `lastError` from the archiver.
- `GetCommitLogArchiveStatus` sets `INTERNAL` when the archiver raises.
- `serve()` with `pitr.enabled=true` creates and starts the archiver (patch `CommitLogArchiver`).
- `serve()` with `pitr.enabled=true` and a constructor that raises `RuntimeError`: the error propagates out of `serve()` (not caught).
- `serve()` with `pitr.enabled=false`: `CommitLogArchiver` is never created (SC4).
- `shutdown()` calls `stop()` on the archiver when one exists.
- `shutdown()` with no archiver does not fail.
- Existing tests in `server_test.py` pass without edits to their asserts, except for the config helper change below.

Note: `_make_config()` in `server_test.py` builds `MedusaConfig(...)` by name. PITR-01 adds a `pitr` field to `MedusaConfig`. Update the helper so the existing tests still build. Keep this edit minimal.

## Definition of Done (Acceptance Criteria)
> How will we know we did the work right?

- [ ] With `pitr.enabled=true`, `serve()` starts one archiver thread. `shutdown()` stops it.
- [ ] A device mismatch or missing directory crashes `serve()` with `RuntimeError`. The error is not caught.
- [ ] `GetCommitLogArchiveStatus` returns `running=false`, `pendingCount=0` with no archiver (SC8a).
- [ ] `GetCommitLogArchiveStatus` returns `pendingCount=N` for N files in the spool (SC8b).
- [ ] With `pitr.enabled=false`, no archiver, no thread, and no new log lines. The existing unit test suite shows zero new failures against the baseline (SC4).
- [ ] The tests listed above exist in `tests/service/grpc/server_test.py` and pass.
- [ ] Unit test coverage is >= 80% for the new and changed logic in `server.py`.
- [ ] Comments explain why the archiver starts in `serve()` and why `RuntimeError` is not caught.
- [ ] Linters and CI pass cleanly.

## Proposed Implementation (Optional / Starting Point)
> Suggested approach. You own the final design.

- `Server` in [`server.py`](../../../medusa/service/grpc/server.py:50): `__init__` (line 51) builds `self.medusa_config` and the `aio.server`. `shutdown()` is at line 61. `serve()` is at line 66. Add `self.commitlog_archiver = None` in `__init__`, and set it in `serve()`.
- In `serve()`, `config = self.create_config()` is read first. `MedusaService(config)` is created on line 70 and added to the server. Create the archiver before that line. Pass it to `MedusaService` (an optional constructor argument with default `None` keeps existing callers and tests working).
- `MedusaService.__init__` is at line 129 and takes only `config`. Existing tests build `MedusaService(config)`. Keep that call valid.
- Put the new handler `GetCommitLogArchiveStatus` next to `PrepareRestore` (line 325). Follow the pattern of `PrepareRestore`: build the response, wrap the body in `try/except Exception`, use `logging.exception`, `context.set_details`, and `context.set_code(grpc.StatusCode.INTERNAL)`.
- Response type: `medusa_pb2.GetCommitLogArchiveStatusResponse` (from PITR-05). Use `medusa_pb2.StatusType` values for `status`.
- `pending_count` comes from `CommitLogArchiver.status()`, which lists the spool on each call (PITR-04). Do not list the spool in `server.py`.
- Imports: `from medusa.service.grpc.commitlog_archiver import CommitLogArchiver`.
- Tests: [`tests/service/grpc/server_test.py`](../../../tests/service/grpc/server_test.py) uses `unittest` with `Mock` and `patch`. `ServerTest._make_config()` (line 36) builds `MedusaConfig` with `grpc=None` and `kubernetes=None`. Add the `pitr` entry there. For `serve()` tests, patch `aio.server` or use `testing=True` (it skips `wait_for_termination`). Check how `Server` loads config from a file path, and use a temp `medusa.ini` if needed.

---

## Why this matters
> Closing context for the engineer.

This is the point where PITR becomes real in a running pod. It must be boring when off and loud when misconfigured. Operators will use the status call to decide if it is safe to trust their recovery points. Thank you for keeping the disabled path untouched.
