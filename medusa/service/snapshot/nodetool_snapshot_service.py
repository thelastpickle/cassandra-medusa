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
import subprocess

from medusa.nodetool import Nodetool
from medusa.service.snapshot.abstract_snapshot_service import AbstractSnapshotService


class NodetoolSnapshotService(AbstractSnapshotService):

    def __init__(self, config):
        super().__init__(config)
        self._nodetool = Nodetool(self.config)

    def create_snapshot(self, *, tag):
        # create the Nodetool command
        cmd = self._nodetool.nodetool + ['snapshot', '-t', tag]
        logging.debug('Executing: {}'.format(' '.join(cmd)))
        try:
            subprocess.check_output(cmd, universal_newlines=True)
        except subprocess.CalledProcessError as e:
            logging.error('nodetool output: {}'.format(e.output))
            logging.error('Creating snapshot failed and without a snapshot we cannot do a backup')

    def delete_snapshot(self, *, tag):
        # create the Nodetool command
        cmd = self._nodetool.nodetool + ['clearsnapshot', '-t', tag]
        logging.debug('Executing: {}'.format(' '.join(cmd)))
        try:
            output = subprocess.check_output(cmd, universal_newlines=True)
            logging.debug('nodetool output: {}'.format(output))
        except subprocess.CalledProcessError as e:
            logging.debug('nodetool resulted in error: {}'.format(e.output))
            logging.warning(
                'Medusa may have failed at cleaning up snapshot {}. '
                'Check if the snapshot exists and clear it manually '
                'by running: {}'.format(tag, ' '.join(cmd)))
