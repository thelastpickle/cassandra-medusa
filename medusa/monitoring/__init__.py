# -*- coding: utf-8 -*-
# Copyright 2019 Spotify AB. All rights reserved.
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

import logging

from medusa.monitoring.noop import NoopMonitoring
from medusa.monitoring.local import LocalMonitoring
from medusa.monitoring.dogstatsd import DogStatsdMonitoring


PROVIDER_DOG_STATSD = 'dog-statsd'
PROVIDER_NONE = 'None'
PROVIDER_INMEM = 'local'


class Monitoring(object):

    def __init__(self, config):
        self._config = config
        self._monitoring_driver = self._connect_monitoring()

    def _connect_monitoring(self):

        if self._config.monitoring_provider == PROVIDER_NONE:
            logging.info('Monitoring provider is noop')
            return NoopMonitoring(self._config)
        elif self._config.monitoring_provider == PROVIDER_DOG_STATSD:
            logging.info('Monitoring provider is dog-statsd')
            return DogStatsdMonitoring(self._config)
        elif self._config.monitoring_provider == PROVIDER_INMEM:
            logging.info('Monitoring provider is local')
            return LocalMonitoring(self._config)

        raise NotImplementedError('Unsupported monitoring provider')

    def send(self, tags, value):
        return self._monitoring_driver.send(tags, value)
