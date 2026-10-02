# Full-Async Medusa Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make every Medusa execution path (CLI, gRPC server) fully async — one event loop entered once at each entry point, no `loop.run_until_complete()` bridges, no `asyncio.run()` called from inside a running loop.

**Architecture:** The storage layer's sync wrapper methods are replaced by promoted `async def` public methods. `Storage` becomes an async context manager. All business logic modules become `async def`. CLI commands gain a `run_async` decorator that calls `asyncio.run()` exactly once per command invocation. gRPC handlers are already in an `aio` server; they simply become `async def` and `await` their callees directly.

**Tech Stack:** Python 3.11+, asyncio (stdlib), tenacity (`async_retry`), asyncssh, grpc.aio, click.

**Spec:** `docs/superpowers/specs/2026-09-03-full-async-medusa-design.md`

## Global Constraints

- `asyncio.run()` must appear only inside `run_async` in `medusacli.py` and in `medusa/service/grpc/server.py:__main__`. Zero other occurrences.
- `loop.run_until_complete()` and `get_or_create_event_loop()` must be deleted entirely.
- `@retry` → `@async_retry` on every method that becomes `async def`. Methods that stay sync (CQL wrappers in `backup_node.py`, `cassandra_utils.py`) keep `@retry`.
- `with Storage(...)` → `async with Storage(...)` at every call site.
- `cassandra_utils.py`, proto-generated files, `config.py`, `utils.py`, `monitoring/` are not touched.
- Test runner: `pytest` via `tox -e py311` from venv `/Users/zvo/anaconda3/envs/medusa311/bin`.

---

## File Map

| File | Action |
|------|--------|
| `medusa/medusacli.py` | Add `run_async`; wrap all command handlers |
| `medusa/storage/abstract_storage.py` | Promote `_` methods to public `async def`; delete sync wrappers + `get_or_create_event_loop`; `@retry` → `@async_retry` |
| `medusa/storage/__init__.py` | Replace `__enter__`/`__exit__` with `__aenter__`/`__aexit__` |
| `medusa/storage/google_storage.py` | `disconnect()` sync wrapper → `async def`; `@retry` → `@async_retry` |
| `medusa/storage/azure_storage.py` | Same as google |
| `medusa/storage/s3_base_storage.py` | `@retry` → `@async_retry` on private async methods |
| `medusa/storage/local_storage.py` | `disconnect()` → `async def` (no-op body stays) |
| `medusa/orchestration.py` | `pssh_run` → `async def`; drop `asyncio.run()` |
| `medusa/index.py` | `build_indices`, `add_backup_start_to_index`, `add_backup_finish_to_index`, `set_latest_backup_in_index`, `clean_backup_from_index` → `async def` |
| `medusa/backup_node.py` | `handle_backup`, `start_backup`, `do_backup`, `backup_snapshots` → `async def`; `@retry` → `@async_retry` |
| `medusa/backup_cluster.py` | `orchestrate`, `BackupJob.execute`, `_create_snapshots`, `_upload_backup` → `async def` |
| `medusa/restore_cluster.py` | `orchestrate`, `RestoreJob.execute`, `prepare_restore`, `_restore_data` → `async def` |
| `medusa/restore_node.py` | `restore_node`, `restore_node_locally`, `restore_node_sstableloader`, `capture_release_version` → `async def` |
| `medusa/listing.py` | `list_backups`, `list_backups_w_storage`, `get_backups` → `async def` |
| `medusa/purge.py` | `main`, `delete_backup`, `purge_backups` → `async def` |
| `medusa/purge_decommissioned.py` | `main` → `async def` |
| `medusa/index.py` | `build_indices` → `async def` |
| `medusa/verify.py` | `verify`, `validate_manifest` → `async def` |
| `medusa/status.py` | `status` → `async def` |
| `medusa/report_latest.py` | `report_latest`, `get_latest_complete_cluster_backup` + helpers → `async def` |
| `medusa/download.py` | `download_cmd`, `download_data` → `async def` |
| `medusa/fetch_tokenmap.py` | `main` → `async def` |
| `medusa/service/grpc/server.py` | All `MedusaService` handlers + `Server.shutdown` → `async def` |
| `medusa/service/grpc/restore.py` | `restore_backup` → `async def`; `__main__` block → `asyncio.run()` |
| `tests/storage_test.py` | Wrap storage I/O calls in `asyncio.run()` |
| `tests/orchestration_test.py` | `pssh_run` calls → `asyncio.run(orchestration.pssh_run(...))` |
| Various other test files | Same pattern for any direct calls to now-async functions |

---

### Task 1: Storage layer — async context manager + abstract base

**Files:**
- Modify: `medusa/storage/__init__.py`
- Modify: `medusa/storage/abstract_storage.py`

**Interfaces:**
- Produces: `Storage.__aenter__(self) -> Storage`, `Storage.__aexit__(self, *_) -> None`
- Produces: All public storage methods are now `async def` (exact names: `list_blobs`, `upload_blobs_from_strings`, `upload_blob_from_string`, `upload_object_via_stream`, `download_blobs`, `upload_blobs`, `get_blob`, `get_object`, `get_blob_content_as_string`, `get_blob_content_as_bytes`, `read_blob_as_string`, `read_blob_as_bytes`, `delete_object`, `delete_objects`, `get_blobs_metadata`, `get_blob_metadata`)
- Produces: `AbstractStorage.disconnect()` is `async def` (abstract)
- Deleted: `get_or_create_event_loop()` — gone

- [ ] **Step 1: Replace `__enter__`/`__exit__` with `__aenter__`/`__aexit__` in `storage/__init__.py`**

```python
# In class Storage — replace __enter__ and __exit__ with:
async def __aenter__(self):
    self.storage_driver.connect()   # connect() stays sync for all backends
    return self

async def __aexit__(self, exc_type, exc_val, exc_tb):
    await self.storage_driver.disconnect()
```

- [ ] **Step 2: Update imports in `abstract_storage.py` — add `async_retry`**

```python
# Replace this line:
from tenacity import retry, stop_after_attempt, wait_exponential, wait_fixed
# With:
from tenacity import async_retry, stop_after_attempt, wait_exponential, wait_fixed
```

- [ ] **Step 3: Delete `get_or_create_event_loop` from `abstract_storage.py`**

Delete the entire static method (lines ~375–389 of current file).

- [ ] **Step 4: Promote `_list_blobs` → `list_blobs` and add `@async_retry`**

In `abstract_storage.py`, delete the sync `list_blobs` wrapper (lines ~80–84) and its `@retry` decorator. Rename `_list_blobs` → `list_blobs` in the abstract method declaration and add `@async_retry`:

```python
@async_retry(stop=stop_after_attempt(7), wait=wait_exponential(multiplier=10, max=120))
@abc.abstractmethod
async def list_blobs(self, prefix=None):
    raise NotImplementedError()
```

Also delete the sync `list_objects` wrapper and update it to `async def`:
```python
@async_retry(stop=stop_after_attempt(7), wait=wait_exponential(multiplier=10, max=120))
async def list_objects(self, path=None):
    logging.debug("[Storage] Listing objects in {}".format(path if path is not None else 'everywhere'))
    return await self.list_blobs(prefix=path)
```

- [ ] **Step 5: Promote all remaining sync wrapper → async, delete bridges**

For each sync wrapper / `_`-prefixed pair in `abstract_storage.py`, apply the same pattern: delete the sync wrapper, rename the `_`-prefixed method by dropping the `_`, convert to `async def`, add `@async_retry` where the old wrapper had `@retry`. Full list:

| Old sync wrapper | Old async impl | New public async method |
|---|---|---|
| `list_blobs` (done above) | `_list_blobs` | `list_blobs` |
| `upload_blobs_from_strings` | `_upload_blobs_from_strings` | `upload_blobs_from_strings` |
| `upload_object_via_stream` | `_upload_object` → keep as `_upload_object` (abstract); wrap becomes `upload_object_via_stream` async | keep pattern |
| `download_blobs` | `_download_blobs` | `download_blobs` |
| `upload_blobs` | `_upload_blobs` | `upload_blobs` |
| `get_object` | `_get_object` | keep `_get_object` abstract; make `get_object` async |
| `read_blob_as_bytes` | `_read_blob_as_bytes` | `read_blob_as_bytes` |
| `delete_object` | `_delete_object` | keep `_delete_object` abstract; make `delete_object` async |
| `delete_objects` | `_delete_objects` | `delete_objects` |
| `get_blobs_metadata` | `_get_blobs_metadata` | `get_blobs_metadata` |
| `get_blob_metadata` | `_get_blob_metadata` | `get_blob_metadata` |

`get_blob_content_as_string` and `get_blob_content_as_bytes` call other async methods; make them `async def` and `await`:

```python
@async_retry(stop=stop_after_attempt(7), wait=wait_exponential(multiplier=10, max=120))
async def get_blob_content_as_string(self, path):
    blob = await self.get_blob(str(path))
    if blob is None:
        return None
    return await self.read_blob_as_string(blob)

@async_retry(stop=stop_after_attempt(7), wait=wait_exponential(multiplier=10, max=120))
async def get_blob_content_as_bytes(self, path):
    blob = await self.get_blob(str(path))
    return await self.read_blob_as_bytes(blob)

async def read_blob_as_string(self, blob, encoding="utf-8"):
    return (await self.read_blob_as_bytes(blob)).decode(encoding)
```

`upload_blob_from_string` calls `upload_object_via_stream` — make it `async def` and `await`:
```python
@async_retry(stop=stop_after_attempt(7), wait=wait_exponential(multiplier=10, max=120))
async def upload_blob_from_string(self, path, content, encoding="utf-8"):
    headers = self.additional_upload_headers()
    obj = await self.upload_object_via_stream(
        data=io.BytesIO(bytes(content, encoding)),
        object_name=str(path),
        headers=headers,
    )
    return ManifestObject(obj.name, obj.size, obj.hash)

async def upload_object_via_stream(self, data, object_name, headers):
    return await self._upload_object(data, object_name, headers)
```

Also make `get_blob` async:
```python
@async_retry(stop=stop_after_attempt(7), wait=wait_exponential(multiplier=10, max=120))
async def get_blob(self, path):
    try:
        logging.debug("[Storage] Getting object {}".format(path))
        return await self.get_object(str(path))
    except ObjectDoesNotExistError:
        return None

async def get_object(self, object_key):
    try:
        loop = ...   # DELETE this pattern; replace:
        o = await self._get_object(object_key)
        return o
    except ObjectDoesNotExistError:
        return None
```

Change `AbstractStorage.disconnect()` abstract signature:
```python
@abc.abstractmethod
async def disconnect(self):
    raise NotImplementedError
```

- [ ] **Step 6: Verify `abstract_storage.py` has zero `loop.run_until_complete` and zero `get_or_create_event_loop` references**

```bash
grep -n "run_until_complete\|get_or_create_event_loop" medusa/storage/abstract_storage.py
```
Expected: no output.

- [ ] **Step 7: Commit**

```bash
git add medusa/storage/__init__.py medusa/storage/abstract_storage.py
git commit -m "refactor: storage async context manager and abstract async API"
```

---

### Task 2: Storage backends — async disconnect + async_retry

**Files:**
- Modify: `medusa/storage/google_storage.py`
- Modify: `medusa/storage/azure_storage.py`
- Modify: `medusa/storage/s3_base_storage.py`
- Modify: `medusa/storage/local_storage.py`

**Interfaces:**
- Consumes: `AbstractStorage.disconnect()` is now `async def` (from Task 1)
- Produces: All four backends implement `async def disconnect(self)`
- Produces: `@retry` → `@async_retry` on all `async def` methods in each backend

- [ ] **Step 1: Update `google_storage.py`**

Change import:
```python
from tenacity import async_retry
from tenacity.stop import stop_after_attempt
from tenacity.wait import wait_fixed
```

Delete the sync `disconnect()` wrapper. Rename `_disconnect` → `disconnect`:
```python
async def disconnect(self):
    try:
        await self.gcs_storage.close()
    except Exception:
        pass
    try:
        await self.session.close()
    except Exception:
        pass
```

Replace all `@retry(...)` decorators on `async def` methods with `@async_retry(...)` (same arguments).

- [ ] **Step 2: Update `azure_storage.py`**

Change import:
```python
from tenacity import async_retry, stop_after_attempt, wait_fixed
```

Delete sync `disconnect()` wrapper. Rename `_disconnect` → `disconnect`:
```python
async def disconnect(self):
    await self.azure_container_client.close()
    await self.azure_blob_service.close()
```

Replace all `@retry(...)` on `async def` methods with `@async_retry(...)`.

- [ ] **Step 3: Update `s3_base_storage.py`**

Change import:
```python
from tenacity import async_retry, stop_after_attempt, wait_fixed
```

`disconnect()` in S3 is sync (boto3) — make it `async def` with a sync body (this is fine; `async def` can have sync body):
```python
async def disconnect(self):
    logging.debug('Disconnecting from S3...')
    try:
        self.s3_client.close()
        self.executor.shutdown()
    except Exception as e:
        logging.error('Error disconnecting from S3: {}'.format(e))
```

Replace all `@retry(...)` on `async def` methods with `@async_retry(...)`.

- [ ] **Step 4: Update `local_storage.py`**

Make `disconnect` async (no-op body):
```python
async def disconnect(self):
    pass
```

No `@retry` changes needed in local_storage (none present).

- [ ] **Step 5: Verify no `loop.run_until_complete` in any backend**

```bash
grep -rn "run_until_complete\|get_or_create_event_loop" medusa/storage/
```
Expected: no output.

- [ ] **Step 6: Commit**

```bash
git add medusa/storage/google_storage.py medusa/storage/azure_storage.py \
        medusa/storage/s3_base_storage.py medusa/storage/local_storage.py
git commit -m "refactor: storage backends implement async disconnect and async_retry"
```

---

### Task 3: Storage tests — update for async API

**Files:**
- Modify: `tests/storage_test.py`
- Modify: `tests/storage_test_with_prefix.py`

**Interfaces:**
- Consumes: `Storage.__aenter__`/`__aexit__` (Task 1), all storage public methods now `async def`

- [ ] **Step 1: Replace sync `Storage` context manager usage in test setup with `asyncio.run`**

`StorageTest.setUp` creates `self.storage = Storage(config=...)` directly (no context manager). `connect()` was called by `__enter__`. Now call it explicitly since `setUp` is sync:

```python
def setUp(self):
    # ... existing dir setup ...
    self.storage = Storage(config=self.config.storage)
    self.storage.storage_driver.connect()   # connect() is still sync
```

- [ ] **Step 2: Wrap every storage I/O call in `asyncio.run()`**

Each test method that calls an async storage method must wrap it. Example pattern for `test_add_object_from_string`:

```python
def test_add_object_from_string(self):
    file_content = self.TEST_FILE_CONTENT
    asyncio.run(self.storage.storage_driver.upload_blob_from_string("test1/file.txt", file_content))
    result = asyncio.run(self.storage.storage_driver.get_blob_content_as_string("test1/file.txt"))
    self.assertEqual(result, file_content)
```

Apply the same pattern to every test method that calls:
- `upload_blob_from_string` → `asyncio.run(...)`
- `get_blob_content_as_string` → `asyncio.run(...)`
- `get_blob_content_as_bytes` → `asyncio.run(...)`
- `download_blobs` → `asyncio.run(...)`
- `list_objects` → `asyncio.run(...)`
- `read_blob_as_string` → `asyncio.run(...)`
- `get_blob` → `asyncio.run(...)`
- `build_indices` (from `medusa.index`) → `asyncio.run(...)` (once that is async in Task 8)

- [ ] **Step 3: Add `import asyncio` at top of test files if not present**

```python
import asyncio
```

- [ ] **Step 4: Apply same pattern to `storage_test_with_prefix.py`**

Same wrapping pattern for all async storage calls.

- [ ] **Step 5: Run storage tests**

```bash
/Users/zvo/anaconda3/envs/medusa311/bin/python -m pytest tests/storage_test.py tests/storage_test_with_prefix.py -v
```
Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add tests/storage_test.py tests/storage_test_with_prefix.py
git commit -m "test: update storage tests for async API"
```

---

### Task 4: `Orchestration.pssh_run` → `async def`

**Files:**
- Modify: `medusa/orchestration.py`
- Modify: `tests/orchestration_test.py`

**Interfaces:**
- Produces: `async def pssh_run(self, hosts, command, hosts_variables=None, ssh_client=None) -> bool`
- Deleted: `asyncio.run(...)` call inside `pssh_run`

- [ ] **Step 1: Remove `asyncio.run()` from `pssh_run`, make it `async def`**

In `medusa/orchestration.py`, change:
```python
def pssh_run(self, hosts, command, hosts_variables=None, ssh_client=None):
```
to:
```python
async def pssh_run(self, hosts, command, hosts_variables=None, ssh_client=None):
```

Change:
```python
            batch_results = asyncio.run(
                _run_on_hosts(parallel_hosts, processed_commands, connect_kwargs)
            )
```
to:
```python
            batch_results = await _run_on_hosts(parallel_hosts, processed_commands, connect_kwargs)
```

Update the docstring — remove "Must be called from a synchronous context — asyncio.run() is used internally."

- [ ] **Step 2: Update `tests/orchestration_test.py` — wrap `pssh_run` calls**

`pssh_run` is now a coroutine. Each call in the test must use `asyncio.run()`:

```python
def test_pssh_with_sudo(self):
    result = asyncio.run(self.orchestration.pssh_run(['127.0.0.1'], 'fake command'))
    self.assertTrue(result)
    self.assertEqual(len(_received_commands), 1)
    self.assertIn('sudo', _received_commands[0])
    self.assertIn('fake command', _received_commands[0])

def test_pssh_without_sudo(self):
    conf = self._build_config_parser()
    conf['cassandra']['use_sudo'] = 'False'
    conf['ssh']['login_shell'] = 'True'
    medusa_conf = self._build_medusa_config(conf)
    orchestration_no_sudo = Orchestration(medusa_conf)
    result = asyncio.run(orchestration_no_sudo.pssh_run(['127.0.0.1'], 'fake command'))
    self.assertTrue(result)
    self.assertEqual(len(_received_commands), 1)
    self.assertNotIn('sudo', _received_commands[0])
    self.assertIn('$SHELL -cl', _received_commands[0])

def test_pssh_run_failure(self):
    global _fail_on_call_index
    _fail_on_call_index = 1
    hosts = ['127.0.0.1', '127.0.0.1', '127.0.0.1']
    result = asyncio.run(self.orchestration.pssh_run(hosts, 'fake command'))
    self.assertFalse(result)

def test_pssh_run_with_hosts_variables(self):
    conf = self._build_config_parser()
    conf['cassandra']['use_sudo'] = 'False'
    medusa_conf = self._build_medusa_config(conf)
    orchestration = Orchestration(medusa_conf)
    hosts = ['127.0.0.1', '127.0.0.1']
    command_template = 'medusa --fqdn=%s restore-node %s'
    hosts_variables = [('10.0.0.1', '--seeds 10.0.0.2'), ('10.0.0.2', '')]
    result = asyncio.run(orchestration.pssh_run(hosts, command_template, hosts_variables=hosts_variables))
    self.assertTrue(result)

def test_pssh_run_unreachable_host(self):
    conf = self._build_config_parser()
    conf['ssh']['port'] = '1'   # nothing listening
    medusa_conf = self._build_medusa_config(conf)
    orchestration = Orchestration(medusa_conf)
    result = asyncio.run(orchestration.pssh_run(['127.0.0.1'], 'fake command'))
    self.assertFalse(result)
```

- [ ] **Step 3: Run orchestration tests**

```bash
/Users/zvo/anaconda3/envs/medusa311/bin/python -m pytest tests/orchestration_test.py -v
```
Expected: all pass.

- [ ] **Step 4: Commit**

```bash
git add medusa/orchestration.py tests/orchestration_test.py
git commit -m "refactor: orchestration pssh_run becomes async def"
```

---

### Task 5: `medusa/index.py` — async

**Files:**
- Modify: `medusa/index.py`

**Interfaces:**
- Consumes: `storage.storage_driver.upload_blob_from_string` is now `async def` (Task 1)
- Produces: `async def build_indices(config, noop)`, `async def add_backup_start_to_index(storage, node_backup)`, `async def add_backup_finish_to_index(storage, node_backup)`, `async def set_latest_backup_in_index(storage, node_backup)`, `async def clean_backup_from_index(storage, node_backup)`, `async def update_backup_index(storage, node_backup)`

- [ ] **Step 1: Make all index functions `async def` and `await` storage calls**

```python
async def update_backup_index(storage, node_backup):
    await add_backup_start_to_index(storage, node_backup)
    await add_backup_finish_to_index(storage, node_backup)
    await set_latest_backup_in_index(storage, node_backup)


async def build_indices(config, noop):
    try:
        async with medusa.storage.Storage(config=config.storage) as storage:
            is_ccm = int(shlex.split(config.cassandra.is_ccm)[0])
            all_backups = []
            if is_ccm != 1:
                cassandra = Cassandra(config)
                with cassandra.new_session() as cql_session:
                    tokenmap = cql_session.tokenmap()
                for fqdn in tokenmap.keys():
                    logging.info("processing {}".format(fqdn))
                    all_backups.extend(storage.discover_node_backups(fqdn=fqdn))
            else:
                all_backups = list(storage.discover_node_backups())
            latest_node_backups = {}
            if noop:
                logging.info('--noop was set, will only print the indices')
            for node_backup in all_backups:
                latest_node_backups = await process_backup(node_backup, latest_node_backups, storage, noop)
            if not noop:
                for fqdn, node_backup in latest_node_backups.items():
                    logging.debug('Latest backup {} is {}'.format(fqdn, node_backup.name))
                    await set_latest_backup_in_index(storage, node_backup)
    except Exception:
        traceback.print_exc()
        sys.exit(1)


async def process_backup(node_backup, latest_node_backups, storage, noop):
    if node_backup.finished is None:
        return latest_node_backups
    logging.debug('Found backup {} from {}'.format(node_backup.name, node_backup.fqdn))
    latest = latest_node_backups.get(node_backup.fqdn, node_backup)
    if node_backup.finished >= latest.finished:
        latest_node_backups[node_backup.fqdn] = node_backup
    if not noop:
        await add_backup_start_to_index(storage, node_backup)
        await add_backup_finish_to_index(storage, node_backup)
    return latest_node_backups


async def add_backup_start_to_index(storage, node_backup):
    dst = '{}index/backup_index/{}/tokenmap_{}.json'.format(
        storage.prefix_path, node_backup.name, node_backup.fqdn)
    await storage.storage_driver.upload_blob_from_string(dst, node_backup.tokenmap)
    dst = '{}index/backup_index/{}/schema_{}.cql'.format(
        storage.prefix_path, node_backup.name, node_backup.fqdn)
    await storage.storage_driver.upload_blob_from_string(dst, node_backup.schema)
    dst = '{}index/backup_index/{}/started_{}_{}.timestamp'.format(
        storage.prefix_path, node_backup.name, node_backup.fqdn, node_backup.started)
    await storage.storage_driver.upload_blob_from_string(dst, str(node_backup.started))
    if node_backup.is_differential is True:
        dst = '{}index/backup_index/{}/differential_{}'.format(
            storage.prefix_path, node_backup.name, node_backup.fqdn)
        await storage.storage_driver.upload_blob_from_string(dst, 'differential')


async def add_backup_finish_to_index(storage, node_backup):
    dst = '{}index/backup_index/{}/manifest_{}.json'.format(
        storage.prefix_path, node_backup.name, node_backup.fqdn)
    await storage.storage_driver.upload_blob_from_string(dst, node_backup.manifest)
    dst = '{}index/backup_index/{}/finished_{}_{}.timestamp'.format(
        storage.prefix_path, node_backup.name, node_backup.fqdn, node_backup.finished)
    await storage.storage_driver.upload_blob_from_string(dst, str(node_backup.finished))


async def set_latest_backup_in_index(storage, node_backup):
    dst = '{}index/latest_backup/{}/tokenmap.json'.format(storage.prefix_path, node_backup.fqdn)
    await storage.storage_driver.upload_blob_from_string(dst, node_backup.tokenmap)
    dst = '{}index/latest_backup/{}/backup_name.txt'.format(storage.prefix_path, node_backup.fqdn)
    await storage.storage_driver.upload_blob_from_string(dst, node_backup.name)


async def clean_backup_from_index(storage, node_backup):
    index_files = await storage.storage_driver.list_objects(
        "{}index/backup_index/{}".format(storage.prefix_path, node_backup.name))
    # ... rest of body: same logic, await any storage calls
```

Read the full `clean_backup_from_index` body before editing — it calls `storage.storage_driver.list_objects` and `storage.storage_driver.delete_objects`; both must be `await`-ed.

- [ ] **Step 2: Commit**

```bash
git add medusa/index.py
git commit -m "refactor: index functions become async def"
```

---

### Task 6: `backup_node.py` — async

**Files:**
- Modify: `medusa/backup_node.py`

**Interfaces:**
- Consumes: `async with Storage(...)`, all `storage.*` calls now `await`-able, `async def add_backup_start_to_index` / `add_backup_finish_to_index` / `set_latest_backup_in_index` (Task 5)
- Produces: `async def handle_backup(config, backup_name_arg, stagger_time, enable_md5_checks_flag, mode, keep_snapshot=False, use_existing_snapshot=False)`, `async def start_backup(...)`, `async def do_backup(...)`, `async def backup_snapshots(...)`
- Note: `get_schema_and_tokenmap` and `get_server_type_and_version` stay **sync** (CQL driver). Their `@retry` stays as sync `@retry`. They are called without `await`.

- [ ] **Step 1: Make `handle_backup` async, update Storage context manager**

```python
async def handle_backup(config, backup_name_arg, stagger_time, enable_md5_checks_flag, mode,
                        keep_snapshot=False, use_existing_snapshot=False):
    start = datetime.datetime.now()
    backup_name = backup_name_arg or start.strftime('%Y%m%d%H%M')
    monitoring = Monitoring(config=config.monitoring)
    backup_in_progress_marker = medusa.utils.MedusaTempFile()
    if backup_in_progress_marker.exists():
        marker_path = backup_in_progress_marker.get_path()
        raise IOError(
            f'Error: Backup already in progress. Please delete f{marker_path} if that is not the case to continue.'
        )
    else:
        backup_in_progress_marker.create()

    async with Storage(config=config.storage) as storage:
        try:
            # ... rest of body same, await start_backup(...)
            info = await start_backup(storage, node_backup, cassandra, differential_mode,
                                      stagger_time, start, mode, enable_md5_checks_flag,
                                      backup_name, config, monitoring, keep_snapshot, use_existing_snapshot)
            # ...
        except Exception as e:
            # ...
        finally:
            backup_in_progress_marker.delete()
```

- [ ] **Step 2: Make `start_backup` async**

```python
async def start_backup(storage, node_backup, cassandra, differential_mode, stagger_time, start, mode,
                       enable_md5_checks_flag, backup_name, config, monitoring, keep_snapshot=False,
                       use_existing_snapshot=False):
    # ...
    # get_schema_and_tokenmap is sync — call without await:
    schema, tokenmap = get_schema_and_tokenmap(cassandra)
    # ...
    # get_server_type_and_version is sync — call without await:
    server_type, release_version = get_server_type_and_version(cassandra)
    # ...
    await add_backup_start_to_index(storage, node_backup)
    # ...
    num_files, num_replaced, num_kept = await do_backup(...)
    # ...
```

- [ ] **Step 3: Make `do_backup` and `backup_snapshots` async**

```python
async def do_backup(cassandra, node_backup, storage, enable_md5_checks, md5_check_concurrency,
                    backup_name, keep_snapshot=False, use_existing_snapshot=False):
    # snapshot creation is sync (local Cassandra nodetool call) — no await needed
    # ...
    num_files, num_replaced, num_kept = await backup_snapshots(
        storage, manifest, node_backup, snapshot, enable_md5_checks, md5_check_concurrency
    )
    # ...
    await add_backup_finish_to_index(storage, node_backup)
    await set_latest_backup_in_index(storage, node_backup)
    return num_files, num_replaced, num_kept


async def backup_snapshots(storage, manifest, node_backup, snapshot, enable_md5_checks, md5_check_concurrency):
    try:
        # ...
        files_in_storage = await storage.list_files_per_table() if node_backup.is_differential else {}
        # ...
        for snapshot_path in snapshot.find_dirs():
            # check_already_uploaded is sync (local disk + thread pool) — no await
            needs_backup, needs_reupload, already_backed_up = check_already_uploaded(...)
            # ...
            manifest_objects = []
            if len(needs_upload) > 0:
                manifest_objects += await storage.storage_driver.upload_blobs(needs_upload, dst_path)
            # ...
```

Note: `storage.list_files_per_table()` — check if this method exists in `Storage.__init__.py` and whether it needs awaiting. Read that method before editing.

- [ ] **Step 4: Verify `get_schema_and_tokenmap` and `get_server_type_and_version` still have sync `@retry`**

```bash
grep -A2 "def get_schema_and_tokenmap\|def get_server_type_and_version" medusa/backup_node.py
```
Expected: both are `def` (not `async def`) with `@retry`.

- [ ] **Step 5: Commit**

```bash
git add medusa/backup_node.py
git commit -m "refactor: backup_node becomes async end-to-end"
```

---

### Task 7: `listing.py`, `verify.py`, `status.py`, `fetch_tokenmap.py`, `download.py` — async

**Files:**
- Modify: `medusa/listing.py`
- Modify: `medusa/verify.py`
- Modify: `medusa/status.py`
- Modify: `medusa/fetch_tokenmap.py`
- Modify: `medusa/download.py`

**Interfaces:**
- Produces: `async def list_backups(config, show_all, output)`, `async def list_backups_w_storage(config, show_all, storage, output)`, `async def get_backups(storage, config, show_all)`
- Produces: `async def verify(config, backup_name, enable_md5_checks_flag)`, `async def validate_manifest(storage, node_backup, enable_md5_checks)`
- Produces: `async def status(config, backup_name, output)`
- Produces: `async def main(config, backup_name)` in `fetch_tokenmap.py`
- Produces: `async def download_cmd(config, backup_name, ...)`, `async def download_data(storageconfig, backup, fqtns_to_restore, destination)`

- [ ] **Step 1: `listing.py` — make all three functions async**

```python
async def get_backups(storage, config, show_all):
    cluster_backups = sorted(
        storage.list_cluster_backups(),
        key=lambda b: b.started
    )
    # list_cluster_backups reads from in-memory index; check if it calls storage I/O.
    # If it does, await it. If not, keep as-is.
    if not show_all:
        cluster_backups = filter(
            lambda cluster_backup: config.storage.fqdn in cluster_backup.node_backups,
            cluster_backups
        )
    return cluster_backups


async def list_backups(config, show_all, output):
    async with Storage(config=config.storage) as storage:
        await list_backups_w_storage(config, show_all, storage, output)


async def list_backups_w_storage(config, show_all, storage, output):
    cluster_backups = await get_backups(storage, config, show_all)
    # ... rest of body unchanged (all print/format logic is sync)
```

- [ ] **Step 2: `verify.py` — make `verify` and `validate_manifest` async**

```python
async def verify(config, backup_name, enable_md5_checks_flag):
    async with Storage(config=config.storage) as storage:
        # ... cluster_backup = storage.get_cluster_backup(...) — check if this is async
        # validate_manifest is a generator; make it async def and use async for or list()
        consistency_errors = [
            error
            async for error in validate_manifest(storage, node_backup, enable_md5)
            for node_backup in cluster_backup.node_backups.values()
        ]
        # OR: if validate_manifest returns a list now, just await it
```

Note: `validate_manifest` is a generator that calls `storage.storage_driver.list_objects`. `list_objects` is now `async def`. Convert `validate_manifest` to an `async def` that returns a list (not a generator), and `await list_objects`:

```python
async def validate_manifest(storage, node_backup, enable_md5_checks):
    errors = []
    try:
        manifest = json.loads(node_backup.manifest)
    except Exception:
        logging.error('Unable to read manifest from storage')
        return errors
    data_path_prefix = storage.storage_driver.get_path_prefix(node_backup.data_path)
    objects_in_storage = {
        blob.name: blob
        for blob in await storage.storage_driver.list_objects(node_backup.data_path)
        if '-Statistics.db' not in blob.name
    }
    # ... rest same, append to errors list, return errors
    return errors
```

Update `verify` call:
```python
consistency_errors = []
for node_backup in cluster_backup.node_backups.values():
    consistency_errors.extend(await validate_manifest(storage, node_backup, enable_md5))
```

- [ ] **Step 3: `status.py` — make `status` async**

```python
async def status(config, backup_name, output):
    async with Storage(config=config.storage) as storage:
        # ... rest same; no storage I/O calls after context entry
```

- [ ] **Step 4: `fetch_tokenmap.py` — make `main` async**

```python
async def main(config, backup_name):
    async with Storage(config=config.storage) as storage:
        backup = storage.get_cluster_backup(backup_name)
        # ... rest same
```

- [ ] **Step 5: `download.py` — make `download_data` and `download_cmd` async**

```python
async def download_data(storageconfig, backup, fqtns_to_restore, destination):
    manifest = json.loads(backup.manifest)
    _check_available_space(manifest, destination)
    async with Storage(config=storageconfig) as storage:
        for section in manifest:
            # ...
            if len(srcs) > 0 and ...:
                # ...
                await storage.storage_driver.download_blobs(srcs, dst)
        # ...
        await storage.storage_driver.download_blobs(srcs=[...], dest=destination)


async def download_cmd(config, backup_name, download_destination, keyspaces, tables, ignore_system_keyspaces):
    async with Storage(config=config.storage) as storage:
        # ...
        await download_data(config.storage, node_backup, fqtns_to_download, download_destination)
```

- [ ] **Step 6: Commit**

```bash
git add medusa/listing.py medusa/verify.py medusa/status.py \
        medusa/fetch_tokenmap.py medusa/download.py
git commit -m "refactor: listing, verify, status, fetch_tokenmap, download become async"
```

---

### Task 8: `purge.py`, `purge_decommissioned.py`, `report_latest.py` — async

**Files:**
- Modify: `medusa/purge.py`
- Modify: `medusa/purge_decommissioned.py`
- Modify: `medusa/report_latest.py`

**Interfaces:**
- Produces: `async def main(config, max_backup_age=0, max_backup_count=0)` in `purge.py`
- Produces: `async def delete_backup(config, backup_names, all_nodes)` in `purge.py`
- Produces: `async def purge_backups(storage, backups_to_purge, grace_period_days, fqdn)` in `purge.py`
- Produces: `async def main(config)` in `purge_decommissioned.py`
- Produces: `async def report_latest(config, push_metrics)` in `report_latest.py`
- Produces: `async def get_latest_complete_cluster_backup(config)` in `report_latest.py`

- [ ] **Step 1: `purge.py` — read full file first**

```bash
cat medusa/purge.py
```

Make `main`, `delete_backup`, and `purge_backups` async. Pattern:
```python
async def main(config, max_backup_age=0, max_backup_count=0):
    backups_to_purge = set()
    monitoring = Monitoring(config=config.monitoring)
    try:
        async with Storage(config=config.storage) as storage:
            backup_index = await storage.list_backup_index_blobs()
            backups = list(storage.list_node_backups(fqdn=config.storage.fqdn, backup_index_blobs=backup_index))
            # ...
            object_counts = await purge_backups(storage, backups_to_purge, ...)
            # ...
```

`list_backup_index_blobs` and `list_node_backups` — check if these call async storage I/O and await accordingly. Read those methods in `storage/__init__.py`.

- [ ] **Step 2: `purge_decommissioned.py` — make `main` async**

```python
async def main(config):
    monitoring = Monitoring(config=config.monitoring)
    try:
        async with Storage(config=config.storage) as storage:
            blobs = await storage.list_root_blobs()
            # ...
            for node in decommissioned_nodes:
                backups = set(storage.list_node_backups(fqdn=node))
                counts = await purge_backups(storage, backups, ...)
```

- [ ] **Step 3: `report_latest.py` — read full file, make affected functions async**

```bash
cat medusa/report_latest.py
```

```python
async def report_latest(config, push_metrics):
    # ... same retry loop, but:
    async with Storage(config=config.storage) as storage:
        backup_index = await storage.list_backup_index_blobs()
        await check_node_backup(config, storage, fqdn, push_metrics, monitoring)
        await check_complete_cluster_backup(storage, push_metrics, monitoring, backup_index)
        await check_latest_cluster_backup(storage, push_metrics, monitoring, backup_index)
```

Make `check_node_backup`, `check_complete_cluster_backup`, `check_latest_cluster_backup`, and `get_latest_complete_cluster_backup` async if they call storage I/O.

- [ ] **Step 4: Commit**

```bash
git add medusa/purge.py medusa/purge_decommissioned.py medusa/report_latest.py
git commit -m "refactor: purge, purge_decommissioned, report_latest become async"
```

---

### Task 9: `restore_node.py` and `restore_cluster.py` — async

**Files:**
- Modify: `medusa/restore_node.py`
- Modify: `medusa/restore_cluster.py`

**Interfaces:**
- Consumes: `async def pssh_run` (Task 4), `async with Storage(...)` (Task 1)
- Produces: `async def restore_node(config, temp_dir, backup_name, in_place, keep_auth, seeds, verify, keyspaces, tables, use_sstableloader=False, version_target=None)`
- Produces: `async def restore_node_locally(...)`, `async def restore_node_sstableloader(...)`, `async def capture_release_version(...)`
- Produces: `async def orchestrate(config, backup_name, ...)` in `restore_cluster.py`
- Produces: `async def RestoreJob.execute(self)`, `async def RestoreJob.prepare_restore(self)`, `async def RestoreJob._restore_data(self)`

- [ ] **Step 1: `restore_node.py` — make top-level functions async**

```python
async def restore_node(config, temp_dir, backup_name, in_place, keep_auth, seeds, verify,
                       keyspaces, tables, use_sstableloader=False, version_target=None):
    if in_place and keep_auth:
        logging.error('Cannot keep system_auth when restoring in-place. It would be overwritten')
        sys.exit(1)
    async with Storage(config=config.storage) as storage:
        await capture_release_version(storage, version_target)
        if not use_sstableloader:
            await restore_node_locally(config, temp_dir, backup_name, in_place, keep_auth,
                                       seeds, storage, keyspaces, tables)
        else:
            await restore_node_sstableloader(config, temp_dir, backup_name, in_place, keep_auth,
                                             seeds, storage, keyspaces, tables)
        if verify:
            verify_restore([hostname_resolver.resolve_fqdn()], config)


async def capture_release_version(storage, version_target):
    # body unchanged — no I/O calls; sync code only

async def restore_node_locally(config, temp_dir, backup_name, in_place, keep_auth, seeds,
                                storage, keyspaces, tables):
    differential_blob = await storage.storage_driver.get_blob(
        os.path.join(config.storage.fqdn, backup_name, 'meta', 'differential'))
    # ...
    await download_data(config.storage, node_backup, fqtns_to_restore, destination=download_dir)
    # ...
```

`download_data` is now async (Task 7) — `await` it.

- [ ] **Step 2: `restore_cluster.py` — make `orchestrate`, `RestoreJob.execute`, `prepare_restore`, `_restore_data` async**

```python
async def orchestrate(config, backup_name, seed_target, temp_dir, host_list, keep_auth,
                      bypass_checks, verify, keyspaces, tables, parallel_restores,
                      use_sstableloader=False, version_target=None, ignore_racks=False):
    # ...
    async with Storage(config=config.storage) as storage:
        # ...
        restore = RestoreJob(...)
        await restore.execute()

class RestoreJob(object):
    async def execute(self):
        await self.prepare_restore()
        await self._restore_data()

    async def prepare_restore(self):
        # CqlSessionProvider.new_session() is sync — no change
        # ...

    async def _restore_data(self):
        # ...
        self.orchestration.pssh_run(...) → await self.orchestration.pssh_run(...)
```

- [ ] **Step 3: Commit**

```bash
git add medusa/restore_node.py medusa/restore_cluster.py
git commit -m "refactor: restore_node and restore_cluster become async"
```

---

### Task 10: `backup_cluster.py` — async

**Files:**
- Modify: `medusa/backup_cluster.py`

**Interfaces:**
- Consumes: `async def pssh_run` (Task 4)
- Produces: `async def orchestrate(config, backup_name_arg, ...)`, `async def BackupJob.execute(self, ...)`, `async def BackupJob._create_snapshots(self)`, `async def BackupJob._upload_backup(self)`

- [ ] **Step 1: Make `orchestrate` and `BackupJob` methods async**

```python
async def orchestrate(config, backup_name_arg, seed_target, stagger, enable_md5_checks, mode,
                      temp_dir, orchestration_config, cassandra_config=None, monitoring=None,
                      existing_storage=None, cql_session_provider=None):
    # ...
    backup = BackupJob(...)
    await backup.execute(cql_session_provider)
    # ...

class BackupJob(object):
    async def execute(self, cql_session_provider=None):
        # CqlSessionProvider is sync
        if not self.use_existing_snapshot:
            await self._create_snapshots()
        await self._upload_backup()

    async def _create_snapshots(self):
        pssh_run_success = await self.orchestration_snapshots.pssh_run(
            self.hosts, create_snapshot_command, hosts_variables={})
        # ...

    async def _upload_backup(self):
        pssh_run_success = await self.orchestration_uploads.pssh_run(
            self.hosts, backup_command, hosts_variables={})
        # ...
```

- [ ] **Step 2: Commit**

```bash
git add medusa/backup_cluster.py
git commit -m "refactor: backup_cluster becomes async"
```

---

### Task 11: gRPC server — async handlers

**Files:**
- Modify: `medusa/service/grpc/server.py`
- Modify: `medusa/service/grpc/restore.py`

**Interfaces:**
- Consumes: `async def handle_backup` (Task 6), `async def list_backups` (Task 7), `async def restore_node` (Task 9)
- Produces: All `MedusaService` methods are `async def`; `Server.shutdown` is `async def`

- [ ] **Step 1: Make all sync `MedusaService` handlers `async def`**

The following methods become `async def` (they currently are sync):
- `Backup` → `async def Backup`
- `BackupStatus` → `async def BackupStatus`
- `GetBackup` → `async def GetBackup`
- `GetBackups` → `async def GetBackups`
- `DeleteBackup` → `async def DeleteBackup`
- `PurgeBackups` → `async def PurgeBackups`
- `PrepareRestore` → `async def PrepareRestore`

For each, change `with Storage(...)` → `async with Storage(...)` and `await` any async callee.

- [ ] **Step 2: Simplify `AsyncBackup` — drop `ThreadPoolExecutor`**

`handle_backup` is now a coroutine (Task 6). Remove the `run_in_executor` pattern:

```python
async def AsyncBackup(self, request, context):
    logging.info("Performing ASYNC backup {} (type={})".format(request.name, request.mode))
    response = medusa_pb2.BackupResponse()
    mode = BACKUP_MODE_DIFFERENTIAL
    if medusa_pb2.BackupRequest.Mode.FULL == request.mode:
        mode = BACKUP_MODE_FULL
    try:
        response.backupName = request.name
        response.status = medusa_pb2.StatusType.IN_PROGRESS
        BackupMan.register_backup(request.name, is_async=True)
        # Schedule as a background task so we return immediately:
        loop = asyncio.get_running_loop()
        task = loop.create_task(backup_node.handle_backup(
            self.config, request.name, None, False, mode))
        task.add_done_callback(record_backup_info)
        BackupMan.set_backup_future(request.name, task)
    except Exception as e:
        response.status = medusa_pb2.StatusType.FAILED
        if request.name:
            BackupMan.update_backup_status(request.name, BackupMan.STATUS_FAILED)
        context.set_details("Failed to create async backup: {}".format(e))
        context.set_code(grpc.StatusCode.INTERNAL)
        logging.exception("Async backup failed due to error: {}".format(e))
    return response
```

Note: `BackupMan.set_backup_future` expects a future-like object. `asyncio.Task` has `.done()` and `.result()` — compatible with existing `_determine_backup_status` checks. Verify this before editing.

- [ ] **Step 3: Make `Server.shutdown` async**

```python
async def shutdown(self):
    logging.info("Shutting down GRPC server")
    handle_backup_removal_all()
    await self.grpc_server.stop(0)
```

Remove the old `asyncio.get_event_loop().run_until_complete(...)` call.

- [ ] **Step 4: `restore.py` — make `restore_backup` async, update `__main__`**

```python
async def restore_backup(in_place, config):
    backup_name = os.environ["BACKUP_NAME"]
    tmp_dir = Path("/tmp") if "MEDUSA_TMP_DIR" not in os.environ else Path(os.environ["MEDUSA_TMP_DIR"])
    # ...
    async with Storage(config=config.storage) as storage:
        cluster_backups = list(await medusa.listing.get_backups(storage, config, False))
        # ...
        await medusa.restore_node.restore_node(config, tmp_dir, backup_name, in_place,
                                               keep_auth, seeds, verify, keyspaces, tables,
                                               use_sstableloader)
    return f"Finished restore of backup {backup_name}"

if __name__ == '__main__':
    # ...
    in_place = apply_mapping_env()
    if in_place is not None:
        config = create_config(config_file_path)
        configure_console_logging(config.logging)
        output_message = asyncio.run(restore_backup(in_place, config))
        logging.info(output_message)
```

- [ ] **Step 5: Commit**

```bash
git add medusa/service/grpc/server.py medusa/service/grpc/restore.py
git commit -m "refactor: gRPC service handlers become async def"
```

---

### Task 12: CLI entry point — `run_async` decorator

**Files:**
- Modify: `medusa/medusacli.py`

**Interfaces:**
- Consumes: All business logic entry points are now `async def`
- Produces: `run_async` decorator; every `@cli.command` handler is `async def` and decorated with `@run_async`

- [ ] **Step 1: Add `run_async` decorator and `import asyncio, functools`**

Add near the top of `medusacli.py` (after existing imports):
```python
import asyncio
import functools

def run_async(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        asyncio.run(f(*args, **kwargs))
    return wrapper
```

- [ ] **Step 2: Wrap every command handler**

For every `@cli.command` decorated function, add `@run_async` between `@pass_MedusaConfig` (or the last decorator) and `def`, and change `def` to `async def`. Example:

```python
@cli.command(aliases=['backup', 'backup-node'])
# ... option decorators ...
@pass_MedusaConfig
@run_async
async def backup(medusaconfig, backup_name, stagger, enable_md5_checks, mode, keep_snapshot, use_existing_snapshot):
    # ... body: await backup_node.handle_backup(...)
    stagger_time = datetime.timedelta(seconds=stagger) if stagger else None
    if (use_existing_snapshot and not backup_name):
        raise RuntimeError("Cannot use existing snapshot without providing a backup name")
    actual_backup_name = backup_name or datetime.datetime.now().strftime('%Y%m%d%H%M')
    BackupMan.register_backup(actual_backup_name, is_async=False)
    return await backup_node.handle_backup(medusaconfig, actual_backup_name, stagger_time,
                                           enable_md5_checks, mode, keep_snapshot, use_existing_snapshot)
```

Commands to update (all of them):
- `backup` → `await backup_node.handle_backup(...)`
- `backup_cluster` → `await medusa.backup_cluster.orchestrate(...)`
- `fetch_tokenmap` → `await medusa.fetch_tokenmap.main(...)`
- `list_backups` → `await medusa.listing.list_backups(...)`
- `download` → `await medusa.download.download_cmd(...)`
- `restore_cluster` → `await medusa.restore_cluster.orchestrate(...)`
- `restore_node` → `await medusa.restore_node.restore_node(...)`
- `status` → `await medusa.status.status(...)`
- `verify` → `await medusa.verify.verify(...)`
- `report_last_backup` → `await medusa.report_latest.report_latest(...)`
- `get_last_complete_cluster_backup` → `await medusa.report_latest.get_latest_complete_cluster_backup(...)`
- `build_index` → `await medusa.index.build_indices(...)`
- `purge` → `await medusa.purge.main(...)`
- `purge_decommissioned` → `await medusa.purge_decommissioned.main(...)`
- `delete_backup` → `await medusa.purge.delete_backup(...)`

- [ ] **Step 3: Verify `asyncio.run` appears only in `run_async` wrapper (and grpc server `__main__`)**

```bash
grep -rn "asyncio\.run(" medusa/
```
Expected: only `medusa/medusacli.py` (in `run_async`) and `medusa/service/grpc/server.py` (in `__main__`).

- [ ] **Step 4: Commit**

```bash
git add medusa/medusacli.py
git commit -m "refactor: CLI commands become async, run_async decorator added"
```

---

### Task 13: Update remaining tests + full suite run

**Files:**
- Modify: `tests/backup_node_test.py`
- Modify: `tests/backup_cluster_test.py`
- Modify: `tests/restore_cluster_test.py`
- Modify: `tests/restore_node_test.py`
- Modify: `tests/purge_test.py`
- Modify: `tests/purge_decommissioned_test.py`
- Modify: `tests/download_test.py`
- Modify: `tests/list_backups_json_test.py`
- Modify: `tests/status_json_test.py`

**Interfaces:**
- Consumes: All business logic functions are now `async def`

- [ ] **Step 1: For each test file, wrap direct async function calls in `asyncio.run()`**

Pattern — wherever a test directly calls a now-async function:
```python
# Before:
result = backup_node.handle_backup(config, ...)
# After:
result = asyncio.run(backup_node.handle_backup(config, ...))
```

For tests that patch `Storage.__enter__`/`__exit__`, update to patch `Storage.__aenter__`/`__aexit__`:
```python
# Before:
@patch('medusa.storage.Storage.__enter__', return_value=mock_storage)
@patch('medusa.storage.Storage.__exit__', return_value=False)
# After:
# __aenter__ must return a coroutine:
mock_storage_instance = MagicMock()
mock_aenter = AsyncMock(return_value=mock_storage_instance)
mock_aexit = AsyncMock(return_value=False)
with patch.object(Storage, '__aenter__', mock_aenter), \
     patch.object(Storage, '__aexit__', mock_aexit):
    ...
```

`AsyncMock` is available from `unittest.mock` in Python 3.8+.

- [ ] **Step 2: Add `import asyncio` and `from unittest.mock import AsyncMock` where needed**

- [ ] **Step 3: Run the full test suite**

```bash
cd /Users/zvo/github/cassandra-medusa && \
/Users/zvo/anaconda3/envs/medusa311/bin/python -m pytest tests/ -v --ignore=tests/e2e --ignore=tests/integration -x
```
Expected: all pass. Fix any failures before proceeding.

- [ ] **Step 4: Verify zero `asyncio.run` outside entry points**

```bash
grep -rn "asyncio\.run(" medusa/ tests/
```
Expected in `medusa/`: only `medusacli.py` wrapper and `service/grpc/server.py` `__main__`.
Expected in `tests/`: only in test methods (not inside production code paths).

- [ ] **Step 5: Verify zero `loop.run_until_complete` and `get_or_create_event_loop`**

```bash
grep -rn "run_until_complete\|get_or_create_event_loop" medusa/
```
Expected: no output.

- [ ] **Step 6: Commit**

```bash
git add tests/
git commit -m "test: update all tests for full-async refactor"
```

---

## Self-Review vs Spec

**Spec section coverage check:**

| Spec requirement | Task |
|---|---|
| `asyncio.run()` only in `run_async` + grpc `__main__` | Tasks 12, 11 |
| Delete `get_or_create_event_loop` | Task 1 |
| `Storage` → async context manager | Task 1 |
| `AbstractStorage` public API → `async def` | Task 1 |
| `@retry` → `@async_retry` on async methods | Tasks 1, 2 |
| `google_storage`, `azure_storage` disconnect → async | Task 2 |
| `s3_base_storage`, `local_storage` → async disconnect | Task 2 |
| `Orchestration.pssh_run` → `async def` | Task 4 |
| `backup_node` fully async | Task 6 |
| `backup_cluster` fully async | Task 10 |
| `restore_cluster` fully async | Task 9 |
| `restore_node` fully async | Task 9 |
| `listing`, `verify`, `status`, `fetch_tokenmap`, `download` async | Task 7 |
| `purge`, `purge_decommissioned`, `report_latest` async | Task 8 |
| `index` async | Task 5 |
| gRPC handlers async; `AsyncBackup` drops executor | Task 11 |
| `Server.shutdown` async | Task 11 |
| `restore.py` `__main__` → `asyncio.run` | Task 11 |
| CLI `run_async` decorator, all handlers async | Task 12 |
| CQL wrappers (`get_schema_and_tokenmap` etc.) stay sync | Task 6 note |
| Tests updated | Tasks 3, 4, 13 |

All spec requirements covered. No gaps found.
