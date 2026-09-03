# Full-Async Medusa Design

**Date:** 2026-09-03  
**Status:** Draft — awaiting user approval

## Goal

Make every execution path in Medusa (CLI, gRPC server) fully async — no sync-async mix, no `loop.run_until_complete()` bridges, no `asyncio.run()` called from within a running event loop.

## Context

The parallel-ssh / gevent / greenlet dependency tree was removed. `Orchestration` now uses asyncssh internally via `asyncio.run()` in a sync wrapper. `AbstractStorage` uses `get_or_create_event_loop()` + `loop.run_until_complete()` as a sync bridge for every I/O operation. The gRPC server runs on `grpc.aio` (asyncio), but its service handlers are sync and call sync functions that themselves call `asyncio.run()` — this will raise `RuntimeError: This event loop is already running` as soon as the gRPC server receives a request.

The fix is to go fully async end-to-end so there is one event loop, entered once at the top of each entry point, and every I/O operation is `await`-ed.

## Scope

Files changed:

| File | Change |
|------|--------|
| `medusa/medusacli.py` | Add `run_async` decorator; wrap every `@cli.command` handler |
| `medusa/storage/abstract_storage.py` | Public API → `async def`; delete `get_or_create_event_loop`; `@retry` → `@async_retry` |
| `medusa/storage/__init__.py` | `Storage` → async context manager (`__aenter__`/`__aexit__`) |
| `medusa/storage/google_storage.py` | `disconnect()` → `async def`; `@retry` on private methods → `@async_retry` |
| `medusa/storage/azure_storage.py` | Same as google |
| `medusa/storage/s3_base_storage.py` | `@retry` on private methods → `@async_retry` |
| `medusa/storage/local_storage.py` | No retry changes; `disconnect()` stays sync (no-op) |
| `medusa/orchestration.py` | `pssh_run` → `async def pssh_run`; remove `asyncio.run()` |
| `medusa/backup_node.py` | `handle_backup`, `start_backup`, `do_backup`, helpers → `async def`; `with Storage` → `async with`; `@retry` → `@async_retry` |
| `medusa/backup_cluster.py` | `orchestrate`, `BackupJob.execute`, `_create_snapshots`, `_upload_backup` → `async def` |
| `medusa/restore_cluster.py` | `orchestrate`, `RestoreJob.execute`, `_restore_data`, `prepare_restore` → `async def` |
| `medusa/restore_node.py` | `restore_node` → `async def` |
| `medusa/listing.py` | `list_backups`, `get_backups` → `async def` |
| `medusa/purge.py` | `main`, `delete_backup` → `async def` |
| `medusa/purge_decommissioned.py` | `main` → `async def` |
| `medusa/index.py` | `build_indices` and index helpers → `async def` |
| `medusa/verify.py` | `verify` → `async def` |
| `medusa/status.py` | `status` → `async def` |
| `medusa/report_latest.py` | `report_latest`, `get_latest_complete_cluster_backup` → `async def` |
| `medusa/download.py` | `download_cmd` → `async def` |
| `medusa/fetch_tokenmap.py` | `main` → `async def` |
| `medusa/service/grpc/server.py` | All `MedusaService` handlers → `async def`; `shutdown` → `async def` |
| `medusa/service/grpc/restore.py` | Entry point → `async def` |

Files **not** changed: `medusa/cassandra_utils.py` (CQL driver is sync; its `@retry` wrappers stay sync), `medusa/monitoring/`, `medusa/config.py`, `medusa/utils.py`, proto-generated files.

## Design

### 1. CLI entry point bridge (`medusacli.py`)

One `run_async` decorator wraps every `@cli.command` async handler:

```python
import asyncio, functools

def run_async(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        asyncio.run(f(*args, **kwargs))
    return wrapper
```

Applied as `@run_async` between `@pass_MedusaConfig` and `async def backup(...)`. `asyncio.run()` lives here and nowhere else. No other `asyncio.run()` call exists in the codebase after this change.

### 2. Storage async context manager (`storage/__init__.py`)

`Storage` gains `__aenter__` / `__aexit__` and drops `__enter__` / `__exit__`:

```python
async def __aenter__(self):
    self.storage_driver.connect()   # connect() stays sync for all backends
    return self

async def __aexit__(self, *_):
    await self.storage_driver.disconnect()
```

All `with Storage(...) as storage:` call sites become `async with Storage(...) as storage:`.

### 3. AbstractStorage: drop the bridge, promote `async def`

Every sync wrapper method (`list_blobs`, `upload_blobs`, `download_blobs`, `upload_object_via_stream`, `read_blob_as_bytes`, `delete_object`, `delete_objects`, `get_blobs_metadata`, `get_blob_metadata`, `get_blob_content_as_string`, `get_blob_content_as_bytes`) is deleted. Its `_`-prefixed async counterpart is renamed (drop the `_`) and becomes the public method, decorated with `@async_retry` where the old sync wrapper had `@retry`.

`get_or_create_event_loop()` is deleted entirely.

`AbstractStorage.disconnect()` abstract method changes signature to `async def disconnect()`.

Import change:

```python
from tenacity import async_retry, stop_after_attempt, wait_exponential, wait_fixed
```

`@retry(...)` → `@async_retry(...)` — same arguments, same retry parameters.

### 4. Storage backends

**`google_storage.py`**, **`azure_storage.py`**: `disconnect()` is already `async def _disconnect()` called by a sync wrapper. Drop the sync wrapper, rename `_disconnect` → `disconnect`.

**`s3_base_storage.py`**: `disconnect()` stays sync (boto3 is sync); no change needed. `connect()` stays sync. Private async methods decorated with `@retry` → `@async_retry`.

**`local_storage.py`**: no async disconnect needed; `disconnect()` is a no-op.

### 5. `Orchestration.pssh_run` → `async def`

Remove `asyncio.run(...)` call. The method becomes:

```python
async def pssh_run(self, hosts, command, hosts_variables=None, ssh_client=None):
    ...
    for parallel_hosts in divide_chunks(hosts, self.pool_size):
        ...
        batch_results = await _run_on_hosts(parallel_hosts, processed_commands, connect_kwargs)
        ...
```

`_run_on_hosts` is already `async def` — no change needed there.

### 6. Business logic modules

Each top-level function (`handle_backup`, `orchestrate`, `restore_node`, `list_backups`, `purge.main`, etc.) gains `async def`. Internal helpers they call gain `async def` where needed. `with Storage(...)` → `async with Storage(...)`. Any `await` on storage or orchestration calls added.

`backup_node.get_schema_and_tokenmap` and `get_server_type_and_version` call the CQL driver which is sync — they stay sync, called with plain `()`. Only their `@retry` decorators stay as sync `@retry` (they call sync code).

### 7. gRPC service handlers

All sync handlers in `MedusaService` become `async def`. The `AsyncBackup` handler already uses `loop.run_in_executor` to run `handle_backup` in a thread — under full async, `handle_backup` is a coroutine so the executor is no longer needed: `await backup_node.handle_backup(...)` directly.

`Server.shutdown()` becomes `async def shutdown()`:

```python
async def shutdown(self):
    handle_backup_removal_all()
    await self.grpc_server.stop(0)
```

## Tenacity async usage

`tenacity.async_retry` is the drop-in async equivalent of `retry`. Same `stop=`, `wait=` arguments. The decorated function must be `async def`. Example:

```python
from tenacity import async_retry, stop_after_attempt, wait_exponential

@async_retry(stop=stop_after_attempt(7), wait=wait_exponential(multiplier=10, max=120))
async def list_blobs(self, prefix=None):
    ...
```

## What is NOT changing

- **`cassandra_utils.py`** CQL session methods stay sync (the driver is sync). The `@retry` wrappers on `get_schema_and_tokenmap` and `get_server_type_and_version` in `backup_node.py` stay sync `@retry` because they call sync CQL code.
- **Proto-generated files** (`medusa_pb2.py`, `medusa_pb2_grpc.py`) — untouched.
- **`medusa/config.py`**, **`medusa/utils.py`**, **`medusa/monitoring/`** — no I/O, no async needed.
- **`BackupMan`** — already thread-safe dict; no changes needed.
- **`HostnameResolver`**, **`CqlSessionProvider`** — sync, no changes needed.

## Testing impact

- Tests that call `pssh_run` directly must `asyncio.run(orchestration.pssh_run(...))` or be `async def` with an async test runner.
- Tests that call storage methods directly must `await` them or wrap in `asyncio.run()`.
- Tests that call CLI entry functions must call the underlying `async def` function directly via `asyncio.run()`, not the click-wrapped version.
- Existing unit tests for `backup_node`, `listing`, `purge`, etc. need the same treatment.

## Out of scope

- Parallelising currently sequential operations (e.g., uploading from multiple nodes simultaneously within `backup_cluster`). The async conversion is a prerequisite; parallelism can be added after.
- Converting `cassandra_utils` CQL operations to an async driver.
- Any new features.
