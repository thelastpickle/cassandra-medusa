# -*- coding: utf-8 -*-
"""Tests for JSON output of list-backups command.

These tests monkeypatch medusa.listing.get_backups to return simple stub objects so we
can focus on serialization logic without touching real storage systems.
"""
import json
from types import SimpleNamespace

import medusa.listing as listing

EPOCH_BASE = 1700000000


class StubClusterBackup:
    def __init__(self, name, started_offset, finished_offset, total_nodes, completed_nodes,
                 incomplete_nodes_count, missing_nodes_count, backup_type,
                 backup_size, backup_num_objects):
        self.name = name
        self._started = EPOCH_BASE + started_offset
        self._finished = None if finished_offset is None else EPOCH_BASE + finished_offset
        self._total_nodes = total_nodes
        self._completed_nodes = completed_nodes
        self._incomplete_nodes_count = incomplete_nodes_count
        self._missing_nodes_count = missing_nodes_count
        self._backup_type = backup_type
        self._backup_size = backup_size
        self._backup_num_objects = backup_num_objects

    @property
    def started(self):
        return self._started

    @property
    def finished(self):
        return self._finished

    @property
    def tokenmap(self):
        return {str(i): [] for i in range(self._total_nodes)}

    @property
    def backup_type(self):
        return self._backup_type

    def complete_nodes(self):
        return [object() for _ in range(self._completed_nodes)]

    def incomplete_nodes(self):
        return [object() for _ in range(self._incomplete_nodes_count)]

    def missing_nodes(self):
        return {f"missing_node_{i}" for i in range(self._missing_nodes_count)}

    def size(self):
        return self._backup_size

    def num_objects(self):
        return self._backup_num_objects

    def is_complete(self):
        return self._finished is not None and self._incomplete_nodes_count == 0 and self._missing_nodes_count == 0

    def to_json_dict(self):
        complete_nodes = self.complete_nodes()
        incomplete_nodes = self.incomplete_nodes()
        missing_nodes = self.missing_nodes()

        nodes_list = []
        incomplete_nodes_list = []
        total_size = self._backup_size
        total_objects = self._backup_num_objects

        for _ in complete_nodes:
            nodes_list.append({
                'fqdn': f'node_{len(nodes_list)}.example.com',
                'started': self.started,
                'finished': self.finished,
                'size': total_size // len(complete_nodes) if complete_nodes else 0,
                'num_objects': total_objects // len(complete_nodes) if complete_nodes else 0,
                'server_type': 'cassandra',
                'release_version': '5.0.0'
            })

        for _ in incomplete_nodes:
            incomplete_nodes_list.append({
                'fqdn': f'incomplete_node_{len(incomplete_nodes_list)}.example.com',
                'started': self.started,
                'finished': None,
                'size': 0,
                'num_objects': 0,
                'server_type': 'cassandra',
                'release_version': '5.0.0'
            })

        return {
            'name': self.name,
            'started': self.started,
            'finished': self.finished,
            'backup_type': self.backup_type,
            'size': total_size,
            'num_objects': total_objects,
            'completed_nodes': len(nodes_list),
            'nodes': nodes_list,
            'incomplete_nodes': len(incomplete_nodes_list),
            'incomplete_nodes_list': incomplete_nodes_list,
            'missing_nodes': len(missing_nodes),
            'missing_nodes_list': list(missing_nodes)
        }


def _dummy_config():
    return SimpleNamespace(storage=SimpleNamespace(fqdn='dummy'))


def test_list_backups_json_all_complete(monkeypatch, capsys):
    backups = [
        StubClusterBackup('b1', 0, 3600, 3, 3, 0, 0, 'full', 1000000, 100),
        StubClusterBackup('b2', 7200, 10800, 5, 5, 0, 0, 'differential', 2000000, 200),
    ]
    monkeypatch.setattr(listing, 'get_backups', lambda storage, config, show_all: backups)
    listing.list_backups_w_storage(_dummy_config(), True, storage=None, output='json')
    out = capsys.readouterr().out.strip()
    backups_json = json.loads(out)
    assert len(backups_json) == 2
    for b in backups_json:
        expected_keys = {
            "name", "started", "finished", "backup_type",
            "size", "num_objects",
            "completed_nodes", "nodes",
            "incomplete_nodes", "incomplete_nodes_list",
            "missing_nodes", "missing_nodes_list"
        }
        assert set(b.keys()) == expected_keys
        assert b['finished'] is not None
        assert b['incomplete_nodes'] == 0
        assert b['missing_nodes'] == 0
        assert len(b['nodes']) == b['completed_nodes']
        assert len(b['incomplete_nodes_list']) == 0
        assert len(b['missing_nodes_list']) == 0
        assert b['backup_type'] in ['full', 'differential']
        assert b['size'] > 0
        assert b['num_objects'] > 0


def test_list_backups_json_with_incomplete(monkeypatch, capsys):
    backups = [
        StubClusterBackup('b1', 0, 3600, 4, 4, 0, 0, 'full', 1500000, 150),
        StubClusterBackup('b2', 4000, None, 5, 3, 1, 1, 'differential', 800000, 80),
    ]
    monkeypatch.setattr(listing, 'get_backups', lambda storage, config, show_all: backups)
    listing.list_backups_w_storage(_dummy_config(), True, storage=None, output='json')
    out = capsys.readouterr().out.strip()
    backups_json = json.loads(out)
    assert len(backups_json) == 2
    incomplete = next(b for b in backups_json if b['name'] == 'b2')
    assert incomplete['finished'] is None
    assert incomplete['completed_nodes'] == 3
    assert incomplete['incomplete_nodes'] == 1
    assert incomplete['missing_nodes'] == 1
    assert len(incomplete['nodes']) == 3
    assert len(incomplete['incomplete_nodes_list']) == 1
    assert len(incomplete['missing_nodes_list']) == 1
    assert incomplete['backup_type'] == 'differential'
    assert incomplete['size'] == 800000
    assert incomplete['num_objects'] == 80
    complete = next(b for b in backups_json if b['name'] == 'b1')
    assert complete['incomplete_nodes'] == 0
    assert complete['missing_nodes'] == 0
    assert len(complete['nodes']) == 4
    assert len(complete['incomplete_nodes_list']) == 0
    assert len(complete['missing_nodes_list']) == 0
    assert complete['backup_type'] == 'full'
    assert complete['size'] == 1500000
    assert complete['num_objects'] == 150


def test_list_backups_json_empty(monkeypatch, capsys):
    backups = []
    monkeypatch.setattr(listing, 'get_backups', lambda storage, config, show_all: backups)
    listing.list_backups_w_storage(_dummy_config(), True, storage=None, output='json')
    out = capsys.readouterr().out.strip()
    backups_json = json.loads(out)
    assert len(backups_json) == 0
    assert isinstance(backups_json, list)
