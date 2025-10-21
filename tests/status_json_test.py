# -*- coding: utf-8 -*-
"""Tests for JSON output of status command.

These tests monkeypatch medusa.status to test JSON serialization logic
without requiring real storage backends.
"""
import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import medusa.status as status_module


EPOCH_BASE = 1700000000


class StubNodeBackup:
    def __init__(self, fqdn, started_offset, finished_offset,
                 backup_size, backup_num_objects,
                 server_type, release_version):
        self.fqdn = fqdn
        self.started = EPOCH_BASE + started_offset
        self.finished = None if finished_offset is None else EPOCH_BASE + finished_offset
        self._backup_size = backup_size
        self._backup_num_objects = backup_num_objects
        self.server_type = server_type
        self.release_version = release_version

    def size(self):
        return self._backup_size

    def num_objects(self):
        return self._backup_num_objects


class StubClusterBackup:
    def __init__(self, name, started_offset, finished_offset, node_backups_list,
                 tokenmap_size, missing_nodes_list=None):
        self.name = name
        self._started = EPOCH_BASE + started_offset
        self._finished = None if finished_offset is None else EPOCH_BASE + finished_offset
        self._node_backups = node_backups_list
        self._tokenmap = {f"node{i}": [] for i in range(tokenmap_size)}
        self._missing_nodes = set(missing_nodes_list) if missing_nodes_list else set()

    @property
    def started(self):
        return self._started

    @property
    def finished(self):
        return self._finished

    @property
    def tokenmap(self):
        return self._tokenmap

    @property
    def backup_type(self):
        return "full"

    def is_complete(self):
        return self._finished is not None and len(self._missing_nodes) == 0 and \
            all(nb.finished is not None for nb in self._node_backups)

    def complete_nodes(self):
        return [nb for nb in self._node_backups if nb.finished is not None]

    def incomplete_nodes(self):
        return [nb for nb in self._node_backups if nb.finished is None]

    def missing_nodes(self):
        return self._missing_nodes

    def size(self):
        return sum(nb.size() for nb in self.complete_nodes())

    def num_objects(self):
        return sum(nb.num_objects() for nb in self.complete_nodes())

    def to_json_dict(self):
        nodes_list = []
        incomplete_nodes_list = []
        total_size = 0
        total_objects = 0
        for node_backup in self._node_backups:
            if node_backup.finished:
                node_size = node_backup.size()
                node_objects = node_backup.num_objects()
                nodes_list.append({
                    'fqdn': node_backup.fqdn,
                    'started': node_backup.started,
                    'finished': node_backup.finished,
                    'size': node_size,
                    'num_objects': node_objects,
                    'server_type': node_backup.server_type,
                    'release_version': node_backup.release_version
                })
                total_size += node_size
                total_objects += node_objects
            else:
                incomplete_nodes_list.append({
                    'fqdn': node_backup.fqdn,
                    'started': node_backup.started,
                    'finished': None,
                    'size': 0,
                    'num_objects': 0,
                    'server_type': node_backup.server_type,
                    'release_version': node_backup.release_version
                })
        missing_nodes = self.missing_nodes()
        return {
            'name': self.name,
            'started': self.started,
            'finished': self.finished,
            'complete': self.is_complete(),
            'backup_type': self.backup_type,
            'size': total_size,
            'num_objects': total_objects,
            'total_nodes': len(self.tokenmap),
            'completed_nodes': len(nodes_list),
            'nodes': nodes_list,
            'incomplete_nodes': len(incomplete_nodes_list),
            'incomplete_nodes_list': incomplete_nodes_list,
            'missing_nodes': len(missing_nodes),
            'missing_nodes_list': list(missing_nodes)
        }


def _dummy_config():
    return SimpleNamespace(storage=SimpleNamespace(fqdn='dummy'))


def test_status_json_complete_backup(capsys):
    """Test status with JSON output for a complete backup."""
    node_backups = [
        StubNodeBackup('node1.example.com', 0, 1800, 500000, 50, 'cassandra', '5.0.4'),
        StubNodeBackup('node2.example.com', 100, 1900, 600000, 60, 'cassandra', '5.0.4'),
        StubNodeBackup('node3.example.com', 200, 2000, 700000, 70, 'cassandra', '5.0.4'),
    ]
    backup = StubClusterBackup('test_backup_complete', 0, 2000, node_backups, 3)

    with patch('medusa.status.Storage') as mock_storage:
        mock_storage_instance = MagicMock()
        mock_storage_instance.__enter__.return_value = mock_storage_instance
        mock_storage_instance.get_cluster_backup.return_value = backup
        mock_storage.return_value = mock_storage_instance

        status_module.status(_dummy_config(), 'test_backup_complete', output='json')

    out = capsys.readouterr().out.strip()
    result = json.loads(out)

    assert result['name'] == 'test_backup_complete'
    assert result['started'] == EPOCH_BASE
    assert result['finished'] == EPOCH_BASE + 2000
    assert result['complete'] is True
    assert result['backup_type'] == 'full'
    assert result['total_nodes'] == 3
    assert result['completed_nodes'] == 3
    assert result['incomplete_nodes'] == 0
    assert result['missing_nodes'] == 0
    assert result['size'] == 1800000
    assert result['num_objects'] == 180

    assert len(result['nodes']) == 3
    assert all('fqdn' in node for node in result['nodes'])
    assert all('started' in node for node in result['nodes'])
    assert all('finished' in node for node in result['nodes'])
    assert all('size' in node for node in result['nodes'])
    assert all('num_objects' in node for node in result['nodes'])
    assert all('server_type' in node for node in result['nodes'])
    assert all('release_version' in node for node in result['nodes'])
    assert all(node['finished'] is not None for node in result['nodes'])

    assert result['incomplete_nodes_list'] == []
    assert result['missing_nodes_list'] == []


def test_status_json_incomplete_backup(capsys):
    """Test status with JSON output for an incomplete backup."""
    node_backups = [
        StubNodeBackup('node1.example.com', 0, 1800, 500000, 50, 'cassandra', '5.0.4'),
        StubNodeBackup('node2.example.com', 100, None, 0, 0, 'cassandra', '5.0.4'),  # incomplete
    ]
    backup = StubClusterBackup('test_backup_incomplete', 0, None, node_backups, 3,
                               missing_nodes_list=['node3.example.com'])

    with patch('medusa.status.Storage') as mock_storage:
        mock_storage_instance = MagicMock()
        mock_storage_instance.__enter__.return_value = mock_storage_instance
        mock_storage_instance.get_cluster_backup.return_value = backup
        mock_storage.return_value = mock_storage_instance

        status_module.status(_dummy_config(), 'test_backup_incomplete', output='json')

    out = capsys.readouterr().out.strip()
    result = json.loads(out)

    assert result['name'] == 'test_backup_incomplete'
    assert result['finished'] is None
    assert result['complete'] is False
    assert result['total_nodes'] == 3
    assert result['completed_nodes'] == 1
    assert result['incomplete_nodes'] == 1
    assert result['missing_nodes'] == 1

    assert len(result['nodes']) == 1
    assert result['nodes'][0]['fqdn'] == 'node1.example.com'
    assert result['nodes'][0]['finished'] is not None

    assert len(result['incomplete_nodes_list']) == 1
    assert result['incomplete_nodes_list'][0]['fqdn'] == 'node2.example.com'
    assert result['incomplete_nodes_list'][0]['finished'] is None
    assert result['incomplete_nodes_list'][0]['size'] == 0
    assert result['incomplete_nodes_list'][0]['num_objects'] == 0

    # Check missing nodes list
    assert result['missing_nodes_list'] == ['node3.example.com']


def test_status_json_backup_not_found(capsys):
    """Test status with JSON output when backup is not found."""
    with patch('medusa.status.Storage') as mock_storage:
        mock_storage_instance = MagicMock()
        mock_storage_instance.__enter__.return_value = mock_storage_instance
        mock_storage_instance.get_cluster_backup.side_effect = KeyError('Backup not found')
        mock_storage.return_value = mock_storage_instance

        try:
            status_module.status(_dummy_config(), 'nonexistent_backup', output='json')
        except SystemExit as e:
            assert e.code == 1

    out = capsys.readouterr().out.strip()
    result = json.loads(out)

    assert result == {}


def test_status_json_all_keys_present(capsys):
    """Test that all expected keys are present in JSON output."""
    node_backups = [
        StubNodeBackup('node1.example.com', 0, 1800, 100000, 10, 'cassandra', '4.0.0'),
    ]
    backup = StubClusterBackup('test_backup', 0, 1800, node_backups, 1)

    with patch('medusa.status.Storage') as mock_storage:
        mock_storage_instance = MagicMock()
        mock_storage_instance.__enter__.return_value = mock_storage_instance
        mock_storage_instance.get_cluster_backup.return_value = backup
        mock_storage.return_value = mock_storage_instance

        status_module.status(_dummy_config(), 'test_backup', output='json')

    out = capsys.readouterr().out.strip()
    result = json.loads(out)

    # Check all expected top-level keys
    expected_keys = {
        'name', 'started', 'finished', 'complete', 'backup_type',
        'size', 'num_objects', 'total_nodes',
        'completed_nodes', 'nodes',
        'incomplete_nodes', 'incomplete_nodes_list',
        'missing_nodes', 'missing_nodes_list'
    }
    assert set(result.keys()) == expected_keys

    node_keys = {
        'fqdn', 'started', 'finished', 'size', 'num_objects',
        'server_type', 'release_version'
    }
    assert set(result['nodes'][0].keys()) == node_keys
