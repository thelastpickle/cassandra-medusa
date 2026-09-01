# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import asyncio
import configparser
import tempfile
import threading
import unittest

import asyncssh

from medusa.config import (_namedtuple_from_dict, MedusaConfig, CassandraConfig, SSHConfig)
from medusa.orchestration import Orchestration

# ---------------------------------------------------------------------------
# Shared state written by the server-side handler, read by test assertions.
# Reset in setUp() before each test.
# ---------------------------------------------------------------------------
_received_commands = []   # list of command strings received by the server
_fail_on_call_index = -1  # if >= 0, the handler returns exit 1 on this call index
_call_counter = [0]       # mutable counter shared with the handler coroutine


class _FakeSSHServer(asyncssh.SSHServer):
    """Minimal SSH server that accepts all connections without authentication."""

    def begin_auth(self, username):
        # Return False → no authentication required; accept immediately.
        return False


async def _fake_command_handler(process):
    """
    server-side process_factory callback.
    Records process.command and exits with 0 or 1 depending on _fail_on_call_index.
    """
    global _call_counter
    idx = _call_counter[0]
    _call_counter[0] += 1

    _received_commands.append(process.command)

    if _fail_on_call_index >= 0 and idx == _fail_on_call_index:
        process.stdout.write('fake stderr for failure\n')
        process.exit(1)
    else:
        process.stdout.write('ok\n')
        process.exit(0)

    await process.wait_closed()


class OrchestrationTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Start an in-process asyncssh server on a random port in a background thread.

        Each test's pssh_run() call uses its own event loop; the server runs in a separate one.
        threading.Event (not asyncio.Event) is used for cross-thread signalling.
        """
        cls._tmpdir = tempfile.TemporaryDirectory()
        cls._loop = asyncio.new_event_loop()
        cls._ready = threading.Event()   # set when server is listening
        cls._stop = threading.Event()    # set when tearDownClass wants shutdown

        async def _start_server():
            host_key = asyncssh.generate_private_key('ssh-rsa')
            server = await asyncssh.listen(
                '127.0.0.1', 0,
                server_factory=_FakeSSHServer,
                server_host_keys=[host_key],
                process_factory=_fake_command_handler,
            )
            cls._server = server
            cls._port = server.sockets[0].getsockname()[1]
            cls._ready.set()
            # Poll for shutdown signal. asyncio.sleep keeps the loop alive
            # without busy-waiting; 0.05s poll interval is fast enough.
            while not cls._stop.is_set():
                await asyncio.sleep(0.05)
            server.close()
            await server.wait_closed()

        def _run_loop():
            cls._loop.run_until_complete(_start_server())

        cls._thread = threading.Thread(target=_run_loop, daemon=True)
        cls._thread.start()
        assert cls._ready.wait(timeout=10), 'SSH test server failed to start within 10s'

    @classmethod
    def tearDownClass(cls):
        cls._stop.set()           # signal the server coroutine to stop
        cls._thread.join(timeout=5)
        cls._loop.close()
        cls._tmpdir.cleanup()

    def setUp(self):
        global _received_commands, _fail_on_call_index, _call_counter
        _received_commands = []
        _fail_on_call_index = -1
        _call_counter = [0]

        self.config = self._build_config_parser()
        self.medusa_config = self._build_medusa_config(self.config)
        self.orchestration = Orchestration(self.medusa_config)

        # Patch asyncssh.connect to also pass known_hosts=None for the loopback
        # test server (which uses a freshly generated key not in known_hosts).
        # We do this by monkey-patching connect_kwargs injection into _run_one.
        import medusa.orchestration as orch_mod
        self._orig_run_one = orch_mod._run_one

        async def _patched_run_one(host, command, connect_kwargs):
            kw = dict(connect_kwargs)
            kw['known_hosts'] = None
            return await self._orig_run_one(host, command, kw)

        orch_mod._run_one = _patched_run_one

    def tearDown(self):
        import medusa.orchestration as orch_mod
        orch_mod._run_one = self._orig_run_one

    def _build_config_parser(self):
        config = configparser.ConfigParser(interpolation=None)
        config['cassandra'] = {
            'use_sudo': 'True',
        }
        config['ssh'] = {
            'username': 'guest',
            'key_file': '',
            'port': str(self.__class__._port),
            'cert_file': '',
            'keepalive_seconds': '60',
            'use_pty': 'False',
            'login_shell': 'False',
            'known_hosts': '',
            'forward_agent': 'False',
        }
        return config

    @staticmethod
    def _build_medusa_config(config):
        return MedusaConfig(
            file_path=None,
            storage=None,
            monitoring={},
            cassandra=_namedtuple_from_dict(CassandraConfig, config['cassandra']),
            ssh=_namedtuple_from_dict(SSHConfig, config['ssh']),
            checks=None,
            logging=None,
            grpc=None,
            kubernetes=None,
        )

    # ------------------------------------------------------------------
    # Core behavioural tests
    # ------------------------------------------------------------------

    def test_pssh_with_sudo(self):
        """pssh_run succeeds and the command received by the server contains 'sudo'."""
        result = self.orchestration.pssh_run(['127.0.0.1'], 'fake command')
        self.assertTrue(result)
        self.assertEqual(len(_received_commands), 1)
        self.assertIn('sudo', _received_commands[0])
        self.assertIn('fake command', _received_commands[0])

    def test_pssh_without_sudo(self):
        """With use_sudo=False and login_shell=True, command uses $SHELL -cl but no sudo."""
        conf = self._build_config_parser()
        conf['cassandra']['use_sudo'] = 'False'
        conf['ssh']['login_shell'] = 'True'
        medusa_conf = self._build_medusa_config(conf)
        orchestration_no_sudo = Orchestration(medusa_conf)

        result = orchestration_no_sudo.pssh_run(['127.0.0.1'], 'fake command')
        self.assertTrue(result)
        self.assertEqual(len(_received_commands), 1)
        self.assertNotIn('sudo', _received_commands[0])
        self.assertIn('$SHELL -cl', _received_commands[0])
        self.assertIn('fake command', _received_commands[0])

    def test_pssh_run_failure(self):
        """When one host returns exit 1, pssh_run returns False."""
        global _fail_on_call_index
        # Three hosts — all pointing at 127.0.0.1 with different port config, but we
        # use three entries of 127.0.0.1 to keep DNS resolution fast and reliable.
        # The server returns exit 1 on call index 1 (the second call).
        _fail_on_call_index = 1
        hosts = ['127.0.0.1', '127.0.0.1', '127.0.0.1']

        result = self.orchestration.pssh_run(hosts, 'fake command')
        self.assertFalse(result)
        self.assertEqual(len(_received_commands), 3)

    def test_pssh_run_with_hosts_variables(self):
        """hosts_variables tuples are substituted into the command string per host."""
        conf = self._build_config_parser()
        conf['cassandra']['use_sudo'] = 'False'
        medusa_conf = self._build_medusa_config(conf)
        orchestration = Orchestration(medusa_conf)

        # Both hosts are 127.0.0.1 — the in-process server's only address.
        hosts = ['127.0.0.1', '127.0.0.1']
        command_template = 'medusa --fqdn=%s restore-node %s'
        hosts_variables = [
            ('10.0.0.1', '--seeds 10.0.0.2'),
            ('10.0.0.2', ''),
        ]

        result = orchestration.pssh_run(hosts, command_template, hosts_variables=hosts_variables)
        self.assertTrue(result)
        self.assertEqual(len(_received_commands), 2)

        # asyncio.gather runs both hosts concurrently so arrival order at the server
        # is non-deterministic. Search the full set rather than checking by index.
        all_cmds = ' '.join(_received_commands)
        self.assertIn('--fqdn=10.0.0.1', all_cmds)
        self.assertIn('--seeds 10.0.0.2', all_cmds)
        self.assertIn('--fqdn=10.0.0.2', all_cmds)

        # Verify both substitutions were applied (not just one repeated twice)
        cmd_with_fqdn1 = next(c for c in _received_commands if '--fqdn=10.0.0.1' in c)
        self.assertIn('--seeds 10.0.0.2', cmd_with_fqdn1)
        cmd_with_fqdn2 = next(c for c in _received_commands if '--fqdn=10.0.0.2' in c)
        self.assertNotIn('--seeds', cmd_with_fqdn2)

    def test_pssh_run_unreachable_host(self):
        """When a host is unreachable, pssh_run returns False rather than raising."""
        conf = self._build_config_parser()
        # Port 1 — nothing listening there; connection will be refused.
        conf['ssh']['port'] = '1'
        medusa_conf = self._build_medusa_config(conf)
        orchestration = Orchestration(medusa_conf)

        result = orchestration.pssh_run(['127.0.0.1'], 'fake command')
        self.assertFalse(result)


if __name__ == '__main__':
    unittest.main()
