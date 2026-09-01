import os
import shutil
import subprocess
import sys
import time
import uuid
from pathlib import Path
import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("SSH_E2E") != "1",
    reason="SSH e2e tests require SSH_E2E=1 and Docker"
)

NODE_IPS = ["192.168.200.2", "192.168.200.3", "192.168.200.4"]
COMPOSE_FILE = str(Path(__file__).parent / "docker-compose.yml")
TEMPLATE_NODE = str(Path(__file__).parent / "medusa-node.ini.template")
TEMPLATE_RUNNER = str(Path(__file__).parent / "medusa-runner.ini.template")
BUCKET = "/tmp/medusa-e2e-bucket"
RUNNER_CONFIG = "/tmp/medusa-e2e.ini"
# Resolve the medusa executable from the PATH that poetry run sets up.
# shutil.which searches the actual PATH at import time, which already includes
# the poetry venv's bin/ directory when invoked via `poetry run pytest`.
MEDUSA_BIN = shutil.which("medusa") or str(Path(sys.executable).parent / "medusa")
# Explicit project name keeps container names predictable across environments:
# medusa-e2e-node1-1, medusa-e2e-node2-1, medusa-e2e-node3-1
COMPOSE_PROJECT = "medusa-e2e"
NODE_CONTAINERS = ["medusa-e2e-node1-1", "medusa-e2e-node2-1", "medusa-e2e-node3-1"]


def _docker_compose_cmd():
    # Prefer standalone docker-compose (v2) over the CLI plugin.
    base = ["docker-compose"] if shutil.which("docker-compose") else ["docker", "compose"]
    return base + ["--ansi", "never", "--progress", "plain"]


# Unique suffix per test session so re-runs never collide with leftover backups.
_RUN_ID = uuid.uuid4().hex[:8]


@pytest.fixture(scope="session", autouse=True)
def cluster():
    # Setup
    print(f"[{time.strftime('%H:%M:%S')}] Writing config files", flush=True)
    _write_config_files()
    # Pre-create the bucket_name subdir world-writable (1777).
    # backup-cluster (docker exec, runs as root) calls LocalStorage.__init__ which
    # does root_dir.mkdir(parents=True, exist_ok=True) on base_path/bucket_name.
    # If root creates it first with the default umask (755), the backup-node workers
    # (SSH, medusa user) get PermissionError trying to create subdirs inside it.
    # Pre-creating it here with 1777 means any UID can write into it.
    Path(os.path.join(BUCKET, "medusa-e2e")).mkdir(mode=0o1777, parents=True, exist_ok=True)
    os.chmod(os.path.join(BUCKET, "medusa-e2e"), 0o1777)
    print(f"[{time.strftime('%H:%M:%S')}] Starting containers", flush=True)
    subprocess.run(
        _docker_compose_cmd() + ["-p", COMPOSE_PROJECT, "-f", COMPOSE_FILE, "up", "-d"],
        check=True
    )
    # node1 starts immediately; node2/node3 are held by depends_on until node1
    # is healthy.  Push node1's config, SSH private key, and authorized_keys right away.
    # Use docker cp — more reliable than bind-mounts on macOS/Docker Desktop
    # where filesystem sync can race with container startup.
    # authorized_keys is injected here rather than baked into the image so that
    # the Docker layer cache never serves a stale key from a previous CI run.
    print(f"[{time.strftime('%H:%M:%S')}] Copying config, SSH key and authorized_keys into node1", flush=True)
    subprocess.run(
        ["docker", "cp", "/tmp/medusa-node1.ini",
         f"{NODE_CONTAINERS[0]}:/tmp/medusa-e2e.ini"],
        check=True
    )
    subprocess.run(
        ["docker", "cp", "/tmp/e2e_id_rsa", f"{NODE_CONTAINERS[0]}:/tmp/e2e_id_rsa"],
        check=True
    )
    subprocess.run(
        ["docker", "exec", NODE_CONTAINERS[0], "chmod", "600", "/tmp/e2e_id_rsa"],
        check=True
    )
    _inject_authorized_keys(NODE_CONTAINERS[0])
    print(f"[{time.strftime('%H:%M:%S')}] Waiting for Cassandra cluster to be ready (all 3 nodes UN)...", flush=True)
    _wait_for_cluster_ready()
    # node2 and node3 are up now — push their configs and authorized_keys.
    print(f"[{time.strftime('%H:%M:%S')}] Cluster ready — copying configs and authorized_keys into node2 and node3",
          flush=True)
    for container, ini in zip(NODE_CONTAINERS[1:], [
        "/tmp/medusa-node2.ini", "/tmp/medusa-node3.ini"
    ]):
        subprocess.run(
            ["docker", "cp", ini, f"{container}:/tmp/medusa-e2e.ini"],
            check=True
        )
        _inject_authorized_keys(container)
    # Populate known_hosts from inside node1.  Scan by IP *and* by the Docker
    # hostnames that Cassandra's tokenmap returns (short container IDs and
    # compose DNS names like medusa-e2e-node2-1.medusa-e2e_medusa-e2e).
    # We scan without -H (no hashing) so asyncssh can match on any form.
    # Collect the Docker-internal hostnames for all three nodes.
    print(f"[{time.strftime('%H:%M:%S')}] Scanning SSH host keys", flush=True)
    scan_targets = list(NODE_IPS)
    for c in NODE_CONTAINERS:
        hostname = subprocess.run(
            ["docker", "exec", c, "hostname"],
            capture_output=True, text=True
        ).stdout.strip()
        if hostname:
            scan_targets.append(hostname)
        # Also add the compose DNS name (container_name.network_name)
        fqdn = subprocess.run(
            ["docker", "exec", c, "hostname", "-f"],
            capture_output=True, text=True
        ).stdout.strip()
        if fqdn and fqdn != hostname:
            scan_targets.append(fqdn)
    keyscan = subprocess.run(
        ["docker", "exec", NODE_CONTAINERS[0], "ssh-keyscan"] + scan_targets,
        capture_output=True, text=True, check=True
    )
    known_hosts_content = keyscan.stdout
    Path("/tmp/e2e_known_hosts").write_text(known_hosts_content)
    subprocess.run(
        ["docker", "exec", "-i", NODE_CONTAINERS[0],
         "tee", "/tmp/e2e_known_hosts"],
        input=known_hosts_content, text=True, check=True, capture_output=True
    )
    print(f"[{time.strftime('%H:%M:%S')}] Setup complete", flush=True)
    yield
    # Teardown — always runs
    subprocess.run(
        _docker_compose_cmd() + ["-p", COMPOSE_PROJECT, "-f", COMPOSE_FILE, "down", "-v"],
        check=False  # don't fail teardown on error
    )


def _inject_authorized_keys(container):
    """Write the test pubkey into the medusa user's authorized_keys inside container.

    Done at runtime (not baked into the image) so the Docker layer cache never
    serves a stale authorized_keys from a previous CI run that used a different keypair.
    """
    pubkey = Path("/tmp/e2e_id_rsa.pub").read_text().strip()
    subprocess.run(
        ["docker", "exec", container,
         "sh", "-c",
         f"printf '%s\\n' '{pubkey}' > /home/medusa/.ssh/authorized_keys"
         " && chmod 600 /home/medusa/.ssh/authorized_keys"
         " && chown medusa:medusa /home/medusa/.ssh/authorized_keys"],
        check=True
    )


def _write_config_files():
    runner_tmpl = Path(TEMPLATE_RUNNER).read_text()
    Path(RUNNER_CONFIG).write_text(runner_tmpl)

    node_tmpl = Path(TEMPLATE_NODE).read_text()
    for i, ip in enumerate(NODE_IPS, start=1):
        Path(f"/tmp/medusa-node{i}.ini").write_text(
            node_tmpl.replace("{{NODE_IP}}", ip)
        )


def _wait_for_cluster_ready(max_wait=300, interval=5):
    # Poll nodetool status until all 3 nodes are UN or deadline passes.
    deadline = time.time() + max_wait
    result = None
    while time.time() < deadline:
        result = subprocess.run(
            ["docker", "exec", NODE_CONTAINERS[0], "nodetool", "status"],
            capture_output=True, text=True
        )
        un_count = sum(1 for line in result.stdout.splitlines() if line.startswith("UN "))
        print(f"[{time.strftime('%H:%M:%S')}] nodetool UN count: {un_count}/3"
              + (f" (nodetool error: {result.stderr.strip()})" if result.returncode != 0 else ""))
        if un_count >= 3:
            print("Cluster ready: all 3 nodes UN")
            return
        time.sleep(interval)
    raise RuntimeError(
        f"Cluster not ready after {max_wait}s. "
        f"Last nodetool output:\n{result.stdout if result else ''}\n{result.stderr if result else ''}"
    )


def _run_backup_cluster(backup_name):
    # Run from inside node1 — on macOS, Docker Desktop doesn't route the bridge network to the host.
    return subprocess.run(
        ["docker", "exec", NODE_CONTAINERS[0],
         "medusa", "--config-file", "/tmp/medusa-e2e.ini",
         "backup-cluster", "--backup-name", backup_name,
         "--parallel-uploads", str(len(NODE_CONTAINERS))],
        capture_output=True, text=True
    )


def _dump_node_job_logs(container):
    # Print medusa-wrapper job logs (stdout/stderr files) left in /tmp/medusa-job-* inside container.
    dirs = subprocess.run(
        ["docker", "exec", container, "find", "/tmp", "-maxdepth", "1",
         "-name", "medusa-job-*", "-type", "d"],
        capture_output=True, text=True
    ).stdout.strip().splitlines()
    for d in dirs:
        for fname in ("stdout", "stderr"):
            content = subprocess.run(
                ["docker", "exec", container, "cat", f"{d}/{fname}"],
                capture_output=True, text=True
            )
            if content.stdout.strip() or content.stderr.strip():
                print(f"[{container}] {d}/{fname}:\n{content.stdout}{content.stderr}")


def test_parallel_backup_cluster():
    print(
        f"[{time.strftime('%H:%M:%S')}] Running backup-cluster (backup name: e2e-test-{_RUN_ID})",
        flush=True,  # noqa: E225
    )
    result = _run_backup_cluster(f"e2e-test-{_RUN_ID}")
    if result.returncode != 0:
        print("STDOUT:", result.stdout)
        print("STDERR:", result.stderr)
        print(f"[{time.strftime('%H:%M:%S')}] Dumping backup-node logs from all containers", flush=True)
        for container in NODE_CONTAINERS:
            _dump_node_job_logs(container)
    assert result.returncode == 0, (
        f"backup-cluster exited {result.returncode}\n"
        f"stderr: {result.stderr}"
    )
    # The bucket is a bind-mount shared by all containers.  On macOS/Docker Desktop
    # the host cannot see writes made inside the Linux VM, so we assert from inside
    # node1 where the path is always live.
    # Layout: base_path/bucket_name/prefix/ip/backup_name/
    # Config: base_path=/tmp/medusa-e2e-bucket, bucket_name=medusa-e2e, prefix=e2e-test
    for ip in NODE_IPS:
        check = subprocess.run(
            ["docker", "exec", NODE_CONTAINERS[0],
             "find", f"{BUCKET}/medusa-e2e", "-path", f"*/{ip}/*", "-type", "f"],
            capture_output=True, text=True
        )
        assert check.stdout.strip(), (
            f"No backup files found for node {ip} inside container. "
            f"Bucket contents:\n"
            + subprocess.run(
                ["docker", "exec", NODE_CONTAINERS[0], "find", f"{BUCKET}/medusa-e2e", "-maxdepth", "4"],
                capture_output=True, text=True
            ).stdout
        )


def test_backup_fails_when_node_unreachable():
    """pssh_run must return False (not silently succeed) when a node's sshd is down."""
    try:
        # Stop sshd on node2
        print(f"[{time.strftime('%H:%M:%S')}] Stopping sshd on node2", flush=True)
        subprocess.run(
            ["docker", "exec", NODE_CONTAINERS[1], "supervisorctl", "stop", "sshd"],
            check=True
        )
        print(
            f"[{time.strftime('%H:%M:%S')}] Running backup-cluster with node2 unreachable "
            f"(backup name: e2e-fail-test-{_RUN_ID})",
            flush=True  # noqa: E225
        )
        result = _run_backup_cluster(f"e2e-fail-test-{_RUN_ID}")
        assert result.returncode != 0, (
            "backup-cluster should have failed with node2 unreachable, "
            f"but exited 0\nstdout: {result.stdout}"
        )
    finally:
        # Always restore sshd on node2
        subprocess.run(
            ["docker", "exec", NODE_CONTAINERS[1], "supervisorctl", "start", "sshd"],
            check=False
        )
