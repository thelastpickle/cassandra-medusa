# -*- coding: utf-8 -*-
# Copyright 2020- Datastax, Inc. All rights reserved.
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

import unittest
from unittest.mock import Mock

from prometheus_client import CollectorRegistry, generate_latest
from prometheus_client.parser import text_string_to_metric_families

from medusa.config import MonitoringConfig
from medusa.service.grpc import medusa_pb2
from medusa.service.grpc.metrics import CommitLogArchiverCollector, build_registry, parse_prometheus_port

P = 'medusa_commitlog_archiver_'

STATUS = {
    'running': True,
    'interval_seconds': 60,
    'pending_count': 3,
    'oldest_pending_age_seconds': 42.5,
    'last_progress_time': 1700000100.25,
    'last_success_time': 1700000090.5,
    'last_upload_time': 1700000080,
    'ticks_success': 7,
    'ticks_error': 2,
    'segments_uploaded': 11,
    'segments_skipped': 4,
    'uploaded_bytes': 123456789,
    'status': medusa_pb2.StatusType.IN_PROGRESS,
    'last_error': 'Failed to archive commitlog segment x: "403"',
    'spool_listing_error': '',
}


def _scrape(status):
    archiver = Mock()
    archiver.status.return_value = status
    registry = CollectorRegistry()
    registry.register(CommitLogArchiverCollector(archiver))
    text = generate_latest(registry).decode()
    families = {f.name: f for f in text_string_to_metric_families(text)}
    return archiver, text, families


def _value(families, name, labels=None):
    samples = [s for s in families[name].samples
               if s.name in (name, name + '_total') and (labels is None or s.labels == labels)]
    assert len(samples) == 1, samples
    return samples[0].value


class CommitLogArchiverCollectorTest(unittest.TestCase):

    def test_all_metrics_with_types_and_values(self):
        archiver, _, families = _scrape(STATUS)

        expected = {
            P + 'running': ('gauge', 1),
            P + 'interval_seconds': ('gauge', 60),
            P + 'pending_segments': ('gauge', 3),
            P + 'oldest_pending_segment_age_seconds': ('gauge', 42.5),
            P + 'last_progress_timestamp_seconds': ('gauge', 1700000100.25),
            P + 'last_success_timestamp_seconds': ('gauge', 1700000090.5),
            P + 'last_upload_timestamp_seconds': ('gauge', 1700000080),
            P + 'segments_uploaded': ('counter', 11),
            P + 'segments_skipped': ('counter', 4),
            P + 'uploaded_bytes': ('counter', 123456789),
        }
        for name, (kind, value) in expected.items():
            with self.subTest(name=name):
                self.assertEqual(kind, families[name].type)
                self.assertEqual(value, _value(families, name))

        self.assertEqual('counter', families[P + 'ticks'].type)
        self.assertEqual(7, _value(families, P + 'ticks', {'result': 'success'}))
        self.assertEqual(2, _value(families, P + 'ticks', {'result': 'error'}))
        self.assertEqual(set(expected) | {P + 'ticks'}, set(families))
        archiver.status.assert_called_once()

    def test_exposed_names(self):
        _, text, _ = _scrape(STATUS)
        for line in (
                P + 'pending_segments 3.0',
                P + 'ticks_total{result="success"} 7.0',
                P + 'ticks_total{result="error"} 2.0',
                P + 'segments_uploaded_total 11.0',
                P + 'segments_skipped_total 4.0',
                P + 'uploaded_bytes_total 1.23456789e+08',
                '# TYPE ' + P + 'pending_segments gauge',
        ):
            self.assertIn(line, text.splitlines())
        # gauges never carry _total, and the free-text error is never exported
        self.assertNotIn('pending_segments_total', text)
        self.assertNotIn('403', text)

    def test_running_is_zero_when_stopped(self):
        _, _, families = _scrape(dict(STATUS, running=False))
        self.assertEqual(0, _value(families, P + 'running'))

    def test_unset_timestamps_are_omitted(self):
        status = dict(STATUS, last_progress_time=0, last_success_time=0, last_upload_time=0)
        _, text, families = _scrape(status)
        for name in ('last_progress_timestamp_seconds', 'last_success_timestamp_seconds',
                     'last_upload_timestamp_seconds'):
            self.assertNotIn(P + name, families)
            self.assertNotIn(P + name, text)
        # the other families are still there
        self.assertEqual(3, _value(families, P + 'pending_segments'))

    def test_describe_does_not_call_status(self):
        archiver = Mock()
        names = {f.name for f in CommitLogArchiverCollector(archiver).describe()}
        archiver.status.assert_not_called()
        self.assertIn(P + 'last_progress_timestamp_seconds', names)
        self.assertIn(P + 'ticks', names)
        self.assertEqual(11, len(names))

    def test_build_registry_with_and_without_archiver(self):
        text = generate_latest(build_registry()).decode()
        self.assertIn('python_info', text)
        self.assertNotIn(P, text)

        archiver = Mock()
        archiver.status.return_value = STATUS
        text = generate_latest(build_registry(archiver)).decode()
        self.assertIn('python_info', text)
        self.assertIn(P + 'running', text)

        # one registry per server: building another one does not clash
        build_registry(archiver)


class ParsePrometheusPortTest(unittest.TestCase):

    def _monitoring(self, port):
        return MonitoringConfig(monitoring_provider='None', send_backup_name_tag='False', prometheus_port=port,
                                prometheus_address='0.0.0.0')

    def test_unset_means_disabled(self):
        for value in (None, '', '  '):
            self.assertIsNone(parse_prometheus_port(self._monitoring(value)), value)
        self.assertIsNone(parse_prometheus_port(None))
        self.assertIsNone(parse_prometheus_port({}))

    def test_valid_port(self):
        self.assertEqual(9500, parse_prometheus_port(self._monitoring(' 9500 ')))

    def test_invalid_port(self):
        for value in ('abc', '0', '70000'):
            with self.assertRaises(RuntimeError, msg=value):
                parse_prometheus_port(self._monitoring(value))


if __name__ == '__main__':
    unittest.main()
