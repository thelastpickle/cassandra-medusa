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
import sys
from medusa.storage import Storage


def main(config, backup_name):
    with Storage(config=config.storage) as storage:
        backup = storage.get_cluster_backup(backup_name)
        if not backup:
            logging.error('No such backup')
            sys.exit(1)

        for hostname, ringitem in backup.tokenmap.items():
            print(hostname)
            print(ringitem['tokens'])

        return backup.tokenmap
