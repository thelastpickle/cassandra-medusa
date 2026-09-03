# SSH E2E Tests — Local Run Guide

These tests spin up a real 3-node Cassandra cluster in Docker and exercise
`medusa backup-cluster` end-to-end over a real sshd connection.  They are
skipped by default (unless `SSH_E2E=1`) so they never run accidentally in the
regular `pytest` or `tox` suite.

## Prerequisites

| Requirement | Minimum version | Check |
|---|---|---|
| Docker Engine | 24+ | `docker --version` |
| Docker Compose plugin | v2 (`docker compose`) | `docker compose version` |
| Python | 3.10 – 3.12 | `python3 --version` |
| Poetry | any recent | `poetry --version` |
| `ssh-keygen` / `ssh-keyscan` | bundled with OpenSSH | `which ssh-keygen` |

The tests do **not** need Cassandra or medusa installed on your machine — both
live inside the Docker image.

## One-time setup: build the Docker image

The image bakes a test SSH keypair in at build time, so generate the keypair
first and pass the public key as a build argument.

```bash
# 1. Generate a throwaway keypair (no passphrase)
ssh-keygen -t ed25519 -f /tmp/e2e_id_rsa -N ""

# 2. Build the image from the repo root (the build context must be .)
docker build -f tests/e2e/Dockerfile --build-arg SSH_PUBKEY="$(cat /tmp/e2e_id_rsa.pub)" -t cassandra-medusa-e2e .
```

The build installs medusa from your local working tree, so re-run this step
whenever you change Python source files.  Subsequent builds are fast thanks to
Docker layer caching — only layers after the `pip install` step are
re-evaluated.

## Running the tests

```bash
SSH_E2E=1 poetry run pytest tests/e2e/ssh_e2e_test.py -v -s
```

The `-s` flag lets the timestamped cluster-readiness log lines appear in your
terminal in real time, which makes it easy to see how long Cassandra takes to
form the cluster.

Expected output (abbreviated):

```
[14:02:05] Starting containers
[14:02:35] Copying config and SSH key into node1
[14:02:35] Waiting for Cassandra cluster to be ready (all 3 nodes UN)...
[14:02:36] nodetool UN count: 2/3
[14:02:41] nodetool UN count: 3/3
[14:02:41] Cluster ready — copying configs into node2 and node3
[14:02:41] Scanning SSH host keys
[14:02:43] Setup complete
[14:02:43] Running backup-cluster (backup name: e2e-test-a1b2c3d4)

tests/e2e/ssh_e2e_test.py::test_parallel_backup_cluster PASSED
tests/e2e/ssh_e2e_test.py::test_backup_fails_when_node_unreachable PASSED
```

Total wall time is typically **2–3 minutes** (dominated by Cassandra startup).

## What the tests assert

| Test | What it checks |
|---|---|
| `test_parallel_backup_cluster` | `medusa backup-cluster` exits 0 **and** all three nodes wrote backup files under `/tmp/medusa-e2e-bucket/medusa-e2e/` |
| `test_backup_fails_when_node_unreachable` | `medusa backup-cluster` exits **non-zero** when node2's sshd is stopped mid-run |

## SSH config options

The `[ssh]` section of `medusa.ini` controls how the orchestrator connects to nodes.
Key options relevant to e2e testing:

| Option | Default | Description |
|---|---|---|
| `forward_agent` | `False` | Forward the local SSH agent to remote nodes. Set `True` only when nodes need to make onward SSH connections themselves. Leave `False` in CI (no agent available) and when using `key_file` directly — enabling it with no agent causes asyncssh to hang indefinitely. |
| `use_pty` | `False` | Request a pseudo-terminal. Required by some `sudo` configurations. |
| `login_shell` | `False` | Wrap commands in `$SHELL -cl`. Needed when the remote PATH is set via `.bash_profile`. |
| `keepalive_seconds` | `60` | SSH keepalive interval in seconds. |

## Debugging a failed run

### Inspect container logs

```bash
docker-compose -p medusa-e2e -f tests/e2e/docker-compose.yml logs
```

### SSH into a node manually

```bash
ssh -i /tmp/e2e_id_rsa -o UserKnownHostsFile=/tmp/e2e_known_hosts \
    medusa@192.168.200.2
```

> `/tmp/e2e_known_hosts` is written by the test fixture after the cluster
> comes up.  If the fixture never completed, skip the `UserKnownHostsFile`
> option or use `-o StrictHostKeyChecking=no` instead.

### Check the backup bucket

```bash
find /tmp/medusa-e2e-bucket -type f | sort
```

### Run a single test

```bash
SSH_E2E=1 poetry run pytest tests/e2e/ssh_e2e_test.py::test_parallel_backup_cluster -v -s
```

## Teardown / cleanup

The session fixture always runs `docker compose down -v` on exit, even after a
failure.  If it gets interrupted (e.g. `Ctrl-C` twice), clean up manually:

```bash
docker-compose -p medusa-e2e -f tests/e2e/docker-compose.yml down -v
rm -rf /tmp/medusa-e2e-bucket /tmp/medusa-e2e.ini /tmp/medusa-node*.ini /tmp/e2e_known_hosts
```

The keypair at `/tmp/e2e_id_rsa` and `/tmp/e2e_id_rsa.pub` can be left in
place — they are reused on the next run as long as the image is not rebuilt.

## File map

```
tests/e2e/
├── Dockerfile                  # cassandra:4.1 + sshd + medusa; SSH_PUBKEY build-arg
├── supervisord.conf            # runs cassandra and sshd inside each container
├── cassandra-start.sh          # patches cassandra.yaml from env vars before starting
├── docker-compose.yml          # 3-node cluster on 192.168.200.0/24
├── medusa-node.ini.template    # per-container medusa config ({{NODE_IP}} placeholder)
├── medusa-runner.ini.template  # runner-side medusa config (SSH key + known_hosts)
├── ssh_e2e_test.py             # pytest module with happy-path and failure-path tests
└── README.md                   # this file
```
