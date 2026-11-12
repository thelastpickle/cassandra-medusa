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

"""Tests for the restore entrypoint using local storage with Cassandra restore mocked."""

import json
import logging
import os
import runpy
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from medusa.index import update_backup_index
from medusa.purge import purge_backup
from medusa.service.grpc import restore
from medusa.storage import NodeBackup, Storage


@pytest.fixture
def local_restore(tmp_path, monkeypatch):
    for name in ('POD_IP', 'POD_NAME', 'RESTORE_MAPPING', 'BACKUP_NAME', 'MEDUSA_TMP_DIR'):
        monkeypatch.setenv(name, '')
        monkeypatch.delenv(name)

    config_path = tmp_path / 'medusa.ini'
    config_path.write_text(
        '[storage]\n'
        'storage_provider = local\n'
        f'base_path = {tmp_path}\n'
        'bucket_name = backups\n'
        'prefix = restore-tests\n'
        'fqdn = node-0.example\n'
        '[kubernetes]\n'
        'enabled = true\n'
        '[cassandra]\n'
        'use_sudo = false\n'
    )
    config = restore.create_config(config_path)
    with Storage(config=config.storage) as storage:
        for timestamp, (fqdn, name) in enumerate([
            ('node-1.example', 'base-for-node-1'),
            ('node-0.example', 'base-for-node-0'),
            ('node-0.example', 'legacy-backup'),
        ], start=100):
            backup = NodeBackup(
                storage=storage, fqdn=fqdn, name=name,
                started_timestamp=timestamp, finished_timestamp=timestamp + 1,
            )
            backup.schema = 'CREATE KEYSPACE example WITH replication = {};'  # Not executed by these tests.
            backup.tokenmap = json.dumps({fqdn: {'tokens': [timestamp], 'is_up': True}})
            backup.manifest = '[]'
            update_backup_index(storage, backup)

    mapping = {
        'in_place': True,
        'host_map': {
            'target-0': {'source': ['node-0.example'], 'seed': False},
            'target-1': {'source': ['node-1.example'], 'seed': True},
        },
        'pitr': {
            'target_timestamp_s': 1721000000.0,
            'node_backups': {
                'node-0.example': 'base-for-node-0',
                'node-1.example': 'base-for-node-1',
            },
        },
    }
    root_logger = logging.getLogger()
    original_handlers = list(root_logger.handlers)
    original_level = root_logger.level
    with patch.object(restore.medusa.restore_node, 'restore_node') as restore_node:
        try:
            yield SimpleNamespace(config_path=config_path, mapping=mapping, restore_node=restore_node)
        finally:
            # The real entrypoint installs a console handler and changes the root level.
            for handler in list(root_logger.handlers):
                if handler not in original_handlers:
                    root_logger.removeHandler(handler)
                    handler.close()
            root_logger.setLevel(original_level)


def run_restore_entrypoint(monkeypatch, config_path):
    monkeypatch.setattr(sys, 'argv', ['medusa.service.grpc.restore', str(config_path), 'new-restore-key'])
    # run_path executes the real __main__ block without reimport warnings caused
    # by the unit tests also importing medusa.service.grpc.restore.
    runpy.run_path(restore.__file__, run_name='__main__')


@pytest.mark.parametrize('in_place', [True, False], ids=['inplace', 'remote'])
@pytest.mark.parametrize('legacy_backup', [None, 'legacy-backup'], ids=['no-backup-env', 'conflicting-backup-env'])
def test_entrypoint_restores_each_nodes_assigned_base(monkeypatch, local_restore, in_place, legacy_backup):
    mapping = local_restore.mapping
    mapping['in_place'] = in_place
    monkeypatch.setenv('RESTORE_MAPPING', json.dumps(mapping))
    if legacy_backup is not None:
        monkeypatch.setenv('BACKUP_NAME', legacy_backup)

    # Every pod receives identical JSON, but must select its own base snapshot.
    for index in range(2):
        fqdn = f'node-{index}.example'
        monkeypatch.setenv('POD_NAME', f'target-{index}')
        monkeypatch.setenv('POD_IP', fqdn if in_place else f'target-ip-{index}')
        local_restore.restore_node.reset_mock()

        run_restore_entrypoint(monkeypatch, local_restore.config_path)

        local_restore.restore_node.assert_called_once()
        args = local_restore.restore_node.call_args.args
        assert args[0].storage.fqdn == fqdn
        assert args[1:] == (
            Path('/tmp'), f'base-for-node-{index}', in_place, not in_place, None, False, {}, {}, False,
        )
        assert os.environ['POD_IP'] == fqdn


@pytest.mark.parametrize('with_pod_ip', [False, True], ids=['config-fqdn', 'pod-ip'])
def test_entrypoint_uses_fqdn_fallbacks_without_host_map(monkeypatch, local_restore, with_pod_ip):
    mapping = local_restore.mapping
    mapping.pop('host_map')
    fqdn = 'node-1.example' if with_pod_ip else 'node-0.example'
    if with_pod_ip:
        monkeypatch.setenv('POD_IP', fqdn)
    monkeypatch.setenv('RESTORE_MAPPING', json.dumps(mapping))

    run_restore_entrypoint(monkeypatch, local_restore.config_path)

    local_restore.restore_node.assert_called_once()
    args = local_restore.restore_node.call_args.args
    assert args[0].storage.fqdn == fqdn
    assert args[2] == mapping['pitr']['node_backups'][fqdn]
    assert args[3] is True


@pytest.mark.parametrize('mapping_kind', ['absent', 'inplace', 'remote'])
def test_entrypoint_preserves_standard_restore(monkeypatch, local_restore, mapping_kind):
    monkeypatch.setenv('BACKUP_NAME', 'legacy-backup')
    if mapping_kind != 'absent':
        mapping = local_restore.mapping
        mapping.pop('pitr')
        mapping['in_place'] = mapping_kind == 'inplace'
        monkeypatch.setenv('POD_NAME', 'target-0')
        monkeypatch.setenv('RESTORE_MAPPING', json.dumps(mapping))

    run_restore_entrypoint(monkeypatch, local_restore.config_path)

    local_restore.restore_node.assert_called_once()
    args = local_restore.restore_node.call_args.args
    assert args[0].storage.fqdn == 'node-0.example'
    assert args[2] == 'legacy-backup'
    assert args[3] is (mapping_kind != 'remote')
    assert args[4] is (mapping_kind == 'remote')


@pytest.mark.parametrize('pitr', [False, True], ids=['standard', 'pitr'])
def test_entrypoint_skips_unmapped_remote_pod(monkeypatch, local_restore, pitr):
    mapping = local_restore.mapping
    mapping['in_place'] = False
    if not pitr:
        mapping.pop('pitr')
    monkeypatch.setenv('POD_NAME', 'unmapped-target')
    monkeypatch.setenv('RESTORE_MAPPING', json.dumps(mapping))

    # A skipped pod must not even try loading its configuration.
    run_restore_entrypoint(monkeypatch, local_restore.config_path.parent / 'nonexistent.ini')

    local_restore.restore_node.assert_not_called()
    assert 'POD_IP' not in os.environ


def test_entrypoint_rejects_null_pitr_block(monkeypatch, local_restore):
    local_restore.mapping['pitr'] = None
    monkeypatch.setenv('BACKUP_NAME', 'legacy-backup')
    monkeypatch.setenv('RESTORE_MAPPING', json.dumps(local_restore.mapping))

    with pytest.raises(ValueError, match="The 'pitr' field in RESTORE_MAPPING must be a JSON object"):
        run_restore_entrypoint(monkeypatch, local_restore.config_path)

    local_restore.restore_node.assert_not_called()


def test_entrypoint_missing_pitr_assignment_does_not_restore_legacy_backup(monkeypatch, local_restore):
    mapping = local_restore.mapping
    del mapping['pitr']['node_backups']['node-0.example']
    monkeypatch.setenv('POD_NAME', 'target-0')
    monkeypatch.setenv('BACKUP_NAME', 'legacy-backup')
    monkeypatch.setenv('RESTORE_MAPPING', json.dumps(mapping))

    with pytest.raises(ValueError) as error:
        run_restore_entrypoint(monkeypatch, local_restore.config_path)

    assert str(error.value) == 'No PITR base backup for node node-0.example'
    local_restore.restore_node.assert_not_called()


def test_entrypoint_fails_when_assigned_pitr_base_is_purged(monkeypatch, local_restore):
    mapping = local_restore.mapping
    fqdn = 'node-0.example'
    backup_name = mapping['pitr']['node_backups'][fqdn]
    monkeypatch.setenv('POD_NAME', 'target-0')
    monkeypatch.setenv('BACKUP_NAME', 'legacy-backup')
    monkeypatch.setenv('RESTORE_MAPPING', json.dumps(mapping))

    config = restore.create_config(local_restore.config_path)
    with Storage(config=config.storage) as storage:
        base = next(backup for backup in storage.list_node_backups(fqdn=fqdn) if backup.name == backup_name)
        purge_backup(storage, base)

    with pytest.raises(RuntimeError) as error:
        run_restore_entrypoint(monkeypatch, local_restore.config_path)

    assert backup_name in str(error.value)
    assert fqdn in str(error.value)
    local_restore.restore_node.assert_not_called()
