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

import logging
import os
import sys
from collections import defaultdict
from pathlib import Path

import medusa.config
import medusa.restore_node


def create_config(config_file_path):
    config_file = Path(config_file_path)
    args = defaultdict(lambda: None)

    return medusa.config.load_config(args, config_file)


def configure_console_logging(config):
    root_logger = logging.getLogger('')
    root_logger.setLevel(logging.DEBUG)

    log_format = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s')

    console_handler = logging.StreamHandler()
    console_handler.setLevel(getattr(logging, config.level))
    console_handler.setFormatter(log_format)
    root_logger.addHandler(console_handler)

    if console_handler.level > logging.DEBUG:
        # Disable debugging logging for external libraries
        for logger_name in 'urllib3', 'google_cloud_storage.auth.transport.requests', 'paramiko', 'cassandra':
            logging.getLogger(logger_name).setLevel(logging.WARN)


if len(sys.argv) > 2:
    config_file_path = sys.argv[2]
else:
    config_file_path = "/etc/medusa/medusa.ini"

config = create_config(config_file_path)
configure_console_logging(config.logging)

backup_name = os.environ["BACKUP_NAME"]
tmp_dir = Path("/tmp")
in_place = True
keep_auth = False
seeds = None
verify = False
keyspaces = {}
tables = {}
use_sstableloader = False

logging.info("Starting restore of backup {}".format(backup_name))

medusa.restore_node.restore_node(config, tmp_dir, backup_name, in_place, keep_auth, seeds, verify, keyspaces, tables,
                                 use_sstableloader)

logging.info("Finished restore of backup {}".format(backup_name))
