# -*- coding: utf-8 -*-
# Copyright 2021-present Shopify. All rights reserved.
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

import medusa.utils
from datadog.dogstatsd import DogStatsd
from medusa.monitoring.abstract import AbstractMonitoring


class DogStatsdMonitoring(AbstractMonitoring):

    def __init__(self, config):
        super().__init__(config)
        self.client = DogStatsd()

    def send(self, tags, value):
        if len(tags) != 3:
            raise AssertionError("Datadog monitoring implementation needs 3 tags: 'name', 'what' and 'backup_name'")

        name, what, backup_name = tags
        metric = '{name}.{what}'.format(name=name, what=what)
        backup_name_tag = 'backup_name:{}'.format(backup_name)

        # The backup_name  would be a rather high cardinality metrics series if backups are at all frequent.
        # This could be a expensive metric so backup_name is droppped from the tags sent by default
        if medusa.utils.evaluate_boolean(self.config.send_backup_name_tag):
            self.client.gauge(metric, value, tags=[backup_name_tag])
        else:
            self.client.gauge(metric, value)
