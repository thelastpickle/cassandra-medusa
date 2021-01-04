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

import os
import json

from medusa.monitoring.abstract import AbstractMonitoring


class LocalMonitoring(AbstractMonitoring):

    metric_file = 'metrics.json'

    def __init__(self, config):
        super().__init__(config)
        self.seq_no = 0

    def send(self, tags, value):
        metric = {
            'seq': self.seq_no,
            'tags': tags,
            'value': value
        }
        self._persist_metric(metric)
        self.seq_no += 1

    def _persist_metric(self, metric):
        data = json.dumps(metric)
        with open(self.metric_file, 'a+') as f:
            f.write('{}\n'.format(data))

    def truncate_metric_file(self):
        try:
            os.remove(self.metric_file)
        except FileNotFoundError:
            pass

    def load_metrics(self):

        with open(self.metric_file, 'r') as f:
            for line in f.readlines():
                yield json.loads(line.strip())
