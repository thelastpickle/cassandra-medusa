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

"""Unit tests for restore mappings and backup selection."""

import json
import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from medusa.service.grpc import restore


@pytest.fixture(autouse=True)
def restore_environment(monkeypatch):
    # Restore the caller's environment after every case, including failed cases.
    for name in ('POD_IP', 'POD_NAME', 'RESTORE_MAPPING', 'BACKUP_NAME', 'MEDUSA_TMP_DIR'):
        monkeypatch.delenv(name, raising=False)


@pytest.fixture
def config():
    return SimpleNamespace(storage=SimpleNamespace(fqdn='configured-node.example'))


@pytest.fixture
def mapping():
    return {
        'in_place': True,
        'host_map': {
            'target-0': {'source': ['source-node.example', 'second-source.example'], 'seed': False},
            'target-1': {'source': ['other-node.example'], 'seed': True},
        },
        'pitr': {
            'target_timestamp_s': 1721000000.0,
            'node_backups': {
                'other-node.example': 'other-base',
                'source-node.example': 'mapped-base',
                'pod-ip.example': 'ip-base',
                'configured-node.example': 'config-base',
            },
        },
    }


@pytest.fixture
def restore_io():
    # Keep these unit tests away from object storage and Cassandra data paths.
    with patch.object(restore, 'Storage') as storage, \
            patch.object(restore.medusa.listing, 'get_backups') as get_backups, \
            patch.object(restore.medusa.restore_node, 'restore_node') as restore_node:
        get_backups.return_value = [
            SimpleNamespace(name=name)
            for name in ('unrelated', 'env-backup', 'mapped-base', 'ip-base', 'config-base', 'explicit-base')
        ]
        yield SimpleNamespace(storage=storage, get_backups=get_backups, restore_node=restore_node)


def assert_sstable_restore(restore_io, config, backup_name, in_place=True, tmp_dir=Path('/tmp')):
    restore_io.restore_node.assert_called_once_with(
        config, tmp_dir, backup_name, in_place, not in_place, None, False, {}, {}, False,
    )


def test_restore_without_mapping(monkeypatch):
    monkeypatch.setenv('POD_IP', 'existing-ip')

    assert restore.apply_mapping_env() == (True, {})
    assert os.environ['POD_IP'] == 'existing-ip'


@pytest.mark.parametrize('pitr', [False, True], ids=['standard', 'pitr'])
def test_restore_inplace(monkeypatch, mapping, pitr):
    if not pitr:
        mapping.pop('pitr')
    monkeypatch.setenv('POD_NAME', 'target-0')
    monkeypatch.setenv('RESTORE_MAPPING', json.dumps(mapping))

    assert restore.apply_mapping_env() == (True, mapping)
    assert 'POD_IP' not in os.environ


@pytest.mark.parametrize('pitr', [False, True], ids=['standard', 'pitr'])
def test_restore_remote(monkeypatch, mapping, pitr):
    if not pitr:
        mapping.pop('pitr')
    mapping['in_place'] = False
    monkeypatch.setenv('POD_NAME', 'target-0')
    monkeypatch.setenv('POD_IP', 'target-ip')
    monkeypatch.setenv('RESTORE_MAPPING', json.dumps(mapping))

    assert restore.apply_mapping_env() == (False, mapping)
    assert os.environ['POD_IP'] == 'source-node.example'


@pytest.mark.parametrize('existing_ip', [None, 'target-ip'])
def test_restore_no_match(monkeypatch, mapping, existing_ip):
    mapping['in_place'] = False
    monkeypatch.setenv('POD_NAME', 'unmapped-target')
    if existing_ip is not None:
        monkeypatch.setenv('POD_IP', existing_ip)
    monkeypatch.setenv('RESTORE_MAPPING', json.dumps(mapping))

    assert restore.apply_mapping_env() is None
    assert os.environ.get('POD_IP') == existing_ip


def test_restore_inplace_does_not_require_host_match(monkeypatch, mapping):
    monkeypatch.setenv('POD_NAME', 'unmapped-target')
    monkeypatch.setenv('RESTORE_MAPPING', json.dumps(mapping))

    assert restore.apply_mapping_env() == (True, mapping)
    assert 'POD_IP' not in os.environ


@pytest.mark.parametrize(
    'pod_name,pod_ip,with_host_map,expected',
    [
        ('target-0', 'pod-ip.example', True, 'source-node.example'),
        ('target-0', None, True, 'source-node.example'),
        ('unmapped-target', 'pod-ip.example', True, 'pod-ip.example'),
        (None, 'pod-ip.example', True, 'pod-ip.example'),
        ('target-0', 'pod-ip.example', False, 'pod-ip.example'),
        ('unmapped-target', None, True, 'configured-node.example'),
        (None, None, True, 'configured-node.example'),
        ('target-0', None, False, 'configured-node.example'),
    ],
)
def test_resolve_node_address(monkeypatch, config, mapping, pod_name, pod_ip, with_host_map, expected):
    if pod_name is not None:
        monkeypatch.setenv('POD_NAME', pod_name)
    if pod_ip is not None:
        monkeypatch.setenv('POD_IP', pod_ip)
    if not with_host_map:
        mapping.pop('host_map')

    assert restore.resolve_node_address(mapping, config) == expected


@pytest.mark.parametrize(
    'pod_name,pod_ip,with_host_map,expected_backup',
    [
        ('target-0', 'pod-ip.example', True, 'mapped-base'),
        ('unmapped-target', 'pod-ip.example', True, 'ip-base'),
        (None, 'pod-ip.example', False, 'ip-base'),
        ('unmapped-target', None, True, 'config-base'),
        (None, None, False, 'config-base'),
    ],
)
@pytest.mark.parametrize('legacy_backup', [None, 'env-backup'], ids=['no-backup-env', 'conflicting-backup-env'])
def test_restore_backup_selects_pitr_base(
    monkeypatch, config, mapping, restore_io, pod_name, pod_ip, with_host_map, expected_backup, legacy_backup,
):
    if pod_name is not None:
        monkeypatch.setenv('POD_NAME', pod_name)
    if pod_ip is not None:
        monkeypatch.setenv('POD_IP', pod_ip)
    if not with_host_map:
        mapping.pop('host_map')
    if legacy_backup is not None:
        monkeypatch.setenv('BACKUP_NAME', legacy_backup)

    result = restore.restore_backup(True, config, mapping=mapping)

    assert result == f'Finished restore of backup {expected_backup}'
    assert_sstable_restore(restore_io, config, expected_backup)


def test_restore_backup_uses_shared_address_resolver(config, mapping, restore_io):
    resolver = Mock(return_value='source-node.example')
    # patch.object requires the public helper to exist; do not create a test stub.
    with patch.object(restore, 'resolve_node_address', resolver):
        restore.restore_backup(True, config, mapping=mapping)

    resolver.assert_called_once_with(mapping, config)
    assert_sstable_restore(restore_io, config, 'mapped-base')


@pytest.mark.parametrize('mapping_kind', ['omitted', 'empty', 'standard'])
@pytest.mark.parametrize('in_place', [True, False], ids=['inplace', 'remote'])
def test_restore_backup_uses_legacy_environment(monkeypatch, config, mapping, restore_io, mapping_kind, in_place):
    monkeypatch.setenv('BACKUP_NAME', 'env-backup')
    kwargs = {}
    if mapping_kind == 'empty':
        kwargs['mapping'] = {}
    elif mapping_kind == 'standard':
        mapping.pop('pitr')
        kwargs['mapping'] = mapping

    result = restore.restore_backup(in_place, config, **kwargs)

    assert result == 'Finished restore of backup env-backup'
    assert_sstable_restore(restore_io, config, 'env-backup', in_place=in_place)


@pytest.mark.parametrize('mapping_kind', ['omitted', 'standard', 'pitr', 'empty-pitr'])
@pytest.mark.parametrize('legacy_backup', [None, 'env-backup'], ids=['no-backup-env', 'conflicting-backup-env'])
def test_restore_backup_explicit_name_takes_precedence(
    monkeypatch, config, mapping, restore_io, mapping_kind, legacy_backup,
):
    kwargs = {}
    if mapping_kind != 'omitted':
        kwargs['mapping'] = mapping
        if mapping_kind == 'standard':
            mapping.pop('pitr')
        elif mapping_kind == 'empty-pitr':
            # An explicit name must work even if the node has no PITR assignment.
            mapping['pitr']['node_backups'] = {}
    if legacy_backup is not None:
        monkeypatch.setenv('BACKUP_NAME', legacy_backup)

    result = restore.restore_backup(True, config, backup_name='explicit-base', **kwargs)

    assert result == 'Finished restore of backup explicit-base'
    assert_sstable_restore(restore_io, config, 'explicit-base')


def test_restore_backup_accepts_positional_backup_name(config, restore_io):
    assert restore.restore_backup(True, config, 'explicit-base') == 'Finished restore of backup explicit-base'
    assert_sstable_restore(restore_io, config, 'explicit-base')


@pytest.mark.parametrize('pitr', [None, []], ids=['null', 'array'])
def test_restore_backup_rejects_non_object_pitr_block(monkeypatch, config, restore_io, pitr):
    monkeypatch.setenv('BACKUP_NAME', 'env-backup')

    with pytest.raises(ValueError, match="The 'pitr' field in RESTORE_MAPPING must be a JSON object"):
        restore.restore_backup(True, config, mapping={'pitr': pitr})

    restore_io.storage.assert_not_called()
    restore_io.restore_node.assert_not_called()


@pytest.mark.parametrize(
    'pod_name,pod_ip,fqdn',
    [
        ('target-0', 'pod-ip.example', 'source-node.example'),
        ('unmapped-target', 'pod-ip.example', 'pod-ip.example'),
        (None, None, 'configured-node.example'),
    ],
)
@pytest.mark.parametrize('legacy_backup', [None, 'env-backup'], ids=['no-backup-env', 'conflicting-backup-env'])
def test_restore_backup_missing_pitr_assignment_fails(
    monkeypatch, config, mapping, restore_io, pod_name, pod_ip, fqdn, legacy_backup,
):
    if pod_name is not None:
        monkeypatch.setenv('POD_NAME', pod_name)
    if pod_ip is not None:
        monkeypatch.setenv('POD_IP', pod_ip)
    if legacy_backup is not None:
        monkeypatch.setenv('BACKUP_NAME', legacy_backup)
    del mapping['pitr']['node_backups'][fqdn]

    with pytest.raises(ValueError) as error:
        restore.restore_backup(True, config, mapping=mapping)

    assert str(error.value) == f'No PITR base backup for node {fqdn}'
    restore_io.restore_node.assert_not_called()


def test_restore_backup_empty_pitr_assignments_do_not_fall_back(monkeypatch, config, mapping, restore_io):
    monkeypatch.setenv('BACKUP_NAME', 'env-backup')
    mapping['pitr']['node_backups'] = {}

    with pytest.raises(ValueError) as error:
        restore.restore_backup(True, config, mapping=mapping)

    assert str(error.value) == 'No PITR base backup for node configured-node.example'
    restore_io.restore_node.assert_not_called()


def test_restore_backup_skips_missing_standard_backup(monkeypatch, config, mapping, restore_io):
    monkeypatch.setenv('BACKUP_NAME', 'missing-base')
    mapping.pop('pitr')

    assert restore.restore_backup(True, config, mapping=mapping) == 'Skipped restore of missing backup missing-base'
    restore_io.restore_node.assert_not_called()


def test_restore_backup_fails_when_assigned_pitr_base_is_missing(monkeypatch, config, mapping, restore_io):
    monkeypatch.setenv('BACKUP_NAME', 'env-backup')
    fqdn = config.storage.fqdn
    mapping['pitr']['node_backups'][fqdn] = 'missing-base'

    with pytest.raises(RuntimeError) as error:
        restore.restore_backup(True, config, mapping=mapping)

    assert 'missing-base' in str(error.value)
    assert fqdn in str(error.value)
    restore_io.restore_node.assert_not_called()


def test_restore_backup_respects_temporary_directory(monkeypatch, config, restore_io, tmp_path):
    monkeypatch.setenv('BACKUP_NAME', 'env-backup')
    monkeypatch.setenv('MEDUSA_TMP_DIR', str(tmp_path))

    restore.restore_backup(True, config)

    assert_sstable_restore(restore_io, config, 'env-backup', tmp_dir=tmp_path)
