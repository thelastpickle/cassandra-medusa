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

"""
Prometheus /metrics endpoint of the gRPC server.

Only the gRPC server imports this module, so the CLI does not need prometheus_client.
"""

import logging

from prometheus_client import CollectorRegistry, GCCollector, PlatformCollector, ProcessCollector, start_http_server
from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily
from prometheus_client.registry import Collector

PREFIX = 'medusa_commitlog_archiver_'


class CommitLogArchiverCollector(Collector):
    """
    Pull collector: every scrape reads one CommitLogArchiver.status() snapshot. The archiver does not know about
    metrics. Timestamps that were never set (0) are left out, so ``time() - x`` alerts cannot fire on a fresh start.
    """

    def __init__(self, archiver):
        self._archiver = archiver

    def describe(self):
        # lets the registry check names without listing the spool
        return list(self._families(None))

    def collect(self):
        return self._families(self._archiver.status())

    @staticmethod
    def _families(status):
        def gauge(name, doc, key, value=None, skip_unset=False):
            family = GaugeMetricFamily(PREFIX + name, doc)
            if status is not None:
                v = status[key] if value is None else value
                if skip_unset and not v:
                    return None
                family.add_metric([], v)
            return family

        def counter(name, doc, key):
            family = CounterMetricFamily(PREFIX + name, doc)
            if status is not None:
                family.add_metric([], status[key])
            return family

        yield gauge('running', '1 if the commitlog archiver thread is alive, 0 otherwise.', 'running',
                    value=None if status is None else (1 if status['running'] else 0))
        yield gauge('interval_seconds', 'Seconds the archiver waits between two ticks.', 'interval_seconds')
        yield gauge('pending_segments', 'Number of commitlog segments waiting in the spool directory.',
                    'pending_count')
        yield gauge('oldest_pending_segment_age_seconds',
                    'Age of the oldest commitlog segment waiting in the spool directory, 0 when it is empty.',
                    'oldest_pending_age_seconds')
        for name, doc, key in (
                ('last_progress_timestamp_seconds',
                 'Unix time the archiver last started a tick or archived a segment.', 'last_progress_time'),
                ('last_success_timestamp_seconds',
                 'Unix time the last tick that archived every listed segment ended.', 'last_success_time'),
                ('last_upload_timestamp_seconds',
                 'Unix time of the end of the last tick that uploaded at least one segment.', 'last_upload_time')):
            family = gauge(name, doc, key, skip_unset=True)
            if family is not None:
                yield family

        ticks = CounterMetricFamily(PREFIX + 'ticks', 'Archiver ticks by outcome.', labels=['result'])
        if status is not None:
            ticks.add_metric(['success'], status['ticks_success'])
            ticks.add_metric(['error'], status['ticks_error'])
        yield ticks
        yield counter('segments_uploaded', 'Commitlog segments uploaded to storage.', 'segments_uploaded')
        yield counter('segments_skipped', 'Commitlog segments skipped because storage already had them.',
                      'segments_skipped')
        yield counter('uploaded_bytes', 'Bytes of commitlog segments uploaded to storage.', 'uploaded_bytes')


def build_registry(archiver=None):
    """
    A registry with the process, platform and GC collectors that prometheus_client puts on its default registry,
    plus the archiver collector when there is an archiver. Each server gets its own registry, so building several
    servers in one process (as tests do) never registers the same collector twice.
    """
    registry = CollectorRegistry()
    ProcessCollector(registry=registry)
    PlatformCollector(registry=registry)
    GCCollector(registry=registry)
    if archiver is not None:
        registry.register(CommitLogArchiverCollector(archiver))
    return registry


class MetricsEndpoint:
    """The /metrics HTTP server. It runs in its own thread, so a busy gRPC event loop cannot stall scrapes."""

    def __init__(self, port, address, registry):
        self.port = port
        self.address = address
        try:
            self._httpd, self._thread = start_http_server(port, addr=address, registry=registry)
        except OSError as e:
            raise RuntimeError('Cannot start the Prometheus metrics endpoint on {}:{}: {}'.format(
                address, port, e)) from e
        logging.info('Prometheus metrics endpoint listening on {}:{}'.format(address, port))

    def stop(self):
        self._httpd.shutdown()
        self._httpd.server_close()
        self._thread.join(timeout=5)
        logging.info('Prometheus metrics endpoint stopped')


def parse_prometheus_port(monitoring_config):
    """Returns the configured port, or None when the endpoint is disabled (unset or empty)."""
    value = getattr(monitoring_config, 'prometheus_port', None)
    if value is None or str(value).strip() == '':
        return None
    try:
        port = int(str(value).strip())
    except ValueError:
        raise RuntimeError("[monitoring] prometheus_port must be a port number, got '{}'".format(value))
    if not 0 < port < 65536:
        raise RuntimeError("[monitoring] prometheus_port must be between 1 and 65535, got {}".format(port))
    return port
