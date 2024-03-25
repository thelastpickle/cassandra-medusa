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
import sys
import pathlib
import traceback


class MedusaTempFile(object):

    _tempfile_path = '/tmp/medusa_backup_in_progress'

    def __init__(self):
        pass

    def create(self):
        self._tempfile = open(self._tempfile_path, 'wb')

    def delete(self):
        self._tempfile.close()
        pathlib.Path(self._tempfile_path).unlink()

    def exists(self):
        return pathlib.Path(self._tempfile_path).exists()

    def get_path(self):
        return self._tempfile_path


def evaluate_boolean(value):
    # same behaviour as python's configparser
    if str(value).lower() in ('0', 'false', 'no', 'off'):
        return False
    elif str(value).lower() in ('1', 'true', 'yes', 'on'):
        return True
    else:
        raise TypeError('{} not a boolean'.format(value))


def handle_exception(exception, msg, config):
    if evaluate_boolean(config.grpc.enabled):
        # Propagate the exception when running gRPC server so that exception/error
        # details can be sent back to client.
        raise exception
    else:
        logging.error(msg)
        traceback.print_exc()
        sys.exit(1)


def null_if_empty(value):
    if (value is None):
        return None
    if (str(value) == ''):
        return None
    return value
