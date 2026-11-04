# PITR-05: Add PITR RPCs to `medusa.proto` and regenerate stubs

- **Type:** Task
- **Size:** XS (<2 ideal days)
- **Estimated Ideal Days:** 1 day (Est. Calendar Time: 2-3 days)
- **Parallel Track:** Track D - API
- **Blocked By / Dependencies:** None
- **Blocks:** PITR-06, PITR-08

---

## Purpose
> WHY are we doing this work?

The gRPC API must offer two new calls. `GetCommitLogArchiveStatus` lets the operator watch the archiver. `PreparePitrRestore` lets the operator start a PITR restore.

This ticket adds only the API contract and the generated Python stubs. Other tickets (PITR-06, PITR-08) write the handlers. Doing the contract first lets those tickets run in parallel.

## Requirements
> WHAT needs to be true for this work to be successful?

Edit [`medusa/service/grpc/medusa.proto`](../../../medusa/service/grpc/medusa.proto) (design doc 4.10).

**New RPCs** in `service Medusa`:

```protobuf
rpc GetCommitLogArchiveStatus(GetCommitLogArchiveStatusRequest)
    returns (GetCommitLogArchiveStatusResponse);

rpc PreparePitrRestore(PreparePitrRestoreRequest)
    returns (PreparePitrRestoreResponse);
```

**New messages**

```protobuf
message GetCommitLogArchiveStatusRequest {
}

message GetCommitLogArchiveStatusResponse {
  bool       running         = 1;
  int64      lastUploadTime  = 2;  // Unix epoch seconds; 0 if never uploaded
  int32      pendingCount    = 3;  // segments in spool not yet uploaded
  int32      intervalSeconds = 4;
  StatusType status          = 5;
  string     lastError       = 6;  // non-empty when the last poll could not read the spool directory; empty otherwise
}

message PreparePitrRestoreRequest {
  string targetTimestamp = 1;  // Unix epoch float string or ISO-8601 (UTC)
}

message PreparePitrRestoreResponse {
  StatusType          status           = 1;
  map<string, string> nodeBaseBackups  = 2;  // node FQDN -> backup name
  double              targetTimestampS = 3;  // echo of the parsed target timestamp (Unix epoch float)
}
```

Keep the field names, numbers, and types exactly as above. Keep the comments from design doc 4.10 in the proto file.

**Regenerate stubs**
- Regenerate `medusa_pb2.py` and `medusa_pb2_grpc.py` with `grpc_tools.protoc`.
- Use the command from [`k8s/README.md`](../../../k8s/README.md): run `python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. medusa.proto` from `medusa/service/grpc`.
- Commit the regenerated files.

**Compatibility (additive only)**
- MUST NOT remove, rename, or renumber any existing RPC, message, enum, or field.
- MUST NOT change any existing message.
- The diff on `medusa.proto` must be additions only.

**Out of scope**
- The `k8ssandra-operator` Go client must regenerate its own stubs with `protoc` and the `grpc-go` plugin. This is a separate task in the operator repo. Do NOT do it here. Add a note about it in the PR description and in the proto-adjacent docs if a place exists (PITR-14 covers the operator contract notes).
- Handler code in `server.py`. PITR-06 and PITR-08 own it.

**Tests**
- Add a unit test that imports `medusa_pb2` and `medusa_pb2_grpc`, and checks:
  - The four new message classes exist and can be built with default values.
  - `GetCommitLogArchiveStatusResponse` round-trips all six fields through `SerializeToString` and `ParseFromString`.
  - `PreparePitrRestoreResponse.nodeBaseBackups` accepts map entries.
  - `MedusaServicer` has `GetCommitLogArchiveStatus` and `PreparePitrRestore`. `MedusaStub` is built with both attributes on a channel.
  - An existing message (for example `BackupRequest`) still has its original fields.

## Definition of Done (Acceptance Criteria)
> How will we know we did the work right?

- [ ] `medusa.proto` has the two RPCs and four messages exactly as listed.
- [ ] `git diff` of `medusa.proto` shows additions only.
- [ ] `medusa_pb2.py` and `medusa_pb2_grpc.py` are regenerated with `grpc_tools.protoc`, and the full existing test suite still imports and runs.
- [ ] Unit tests listed above exist and pass.
- [ ] Unit test coverage is >= 80% for any new Python test helper or code added.
- [ ] The PR description states that the operator must regenerate its Go stubs (out of scope here).
- [ ] Linters and CI pass cleanly.

## Proposed Implementation (Optional / Starting Point)
> Suggested approach. You own the final design.

- Current proto: [`medusa/service/grpc/medusa.proto`](../../../medusa/service/grpc/medusa.proto). The `service Medusa` block is at the top (lines 3-19) and ends with `PrepareRestore`. `StatusType` is the enum at line 21. Put the new RPCs after `PrepareRestore`. Put the new messages at the end of the file.
- Pin your tool versions to the ones in the current generated files. The header of [`medusa_pb2.py`](../../../medusa/service/grpc/medusa_pb2.py) says `Protobuf Python Version: 4.25.3`. Use a matching `grpcio-tools` so the diff in generated files is limited to the new items. Check the `grpcio-tools` and `protobuf` pins in the repo requirements files before you generate.
- `medusa_pb2_grpc.py` imports `medusa.service.grpc.medusa_pb2 as medusa__pb2`. After running `protoc` from inside `medusa/service/grpc`, check this import line. `protoc` may write `import medusa_pb2` and you may need to restore the package-qualified form.
- Place new tests near existing gRPC tests under `tests/service/grpc/`.

---

## Why this matters
> Closing context for the engineer.

This small ticket unblocks two larger ones. A clean, additive contract means old clients keep working and the operator team can start on their side early. Thank you for keeping the diff tight.
