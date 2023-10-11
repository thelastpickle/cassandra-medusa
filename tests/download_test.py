# -*- coding: utf-8 -*-
# Copyright 2021 DataStax, Inc.
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
import shutil
import unittest
import uuid

from unittest.mock import patch

from medusa.download import _get_download_size, _check_available_space


class DownloadTest(unittest.TestCase):

    def test_get_download_size(self):
        manifest = [
            {
                'keyspace': 'k1',
                'columnfamily': 't1-bigBadCfId',
                'objects': [
                    {'path': '', 'size': 100, 'MD5': ''},
                    {'path': '', 'size': 101, 'MD5': ''},
                    {'path': '', 'size': 100, 'MD5': ''}
                ]
            },
            {
                'keyspace': 'k2',
                'columnfamily': 't2-81ffe430e50c11e99f91a15641db358f',
                'objects': [
                    {'path': '', 'size': '100', 'MD5': ''},
                    {'path': '', 'size': '123000', 'MD5': ''}
                ]
            },
        ]
        self.assertEqual(123401, _get_download_size(manifest))

    def test_check_available_space(self):
        destination = 'whatever'
        manifest = [
            {
                'keyspace': 'k1',
                'columnfamily': 't1-bigBadCfId',
                'objects': [
                    {'path': '', 'size': 100, 'MD5': ''}
                ]
            }
        ]
        # we fake the return value of _get_available_size to 100
        with patch('medusa.download._get_available_size', return_value=50):
            self.assertRaises(RuntimeError, _check_available_space, manifest, destination)
        # now we provide more space and the exception does not happen
        with patch('medusa.download._get_available_size', return_value=200):
            _check_available_space(manifest, destination)

        # just /tmp should always exist and there ought to be 100 bytes free
        _check_available_space(manifest, destination='/tmp')
        # we also check that a previously non-existing directory is present already at the time of the space check
        random_destination = f'/tmp/medusa-restore-{str(uuid.uuid4())}'
        try:
            _check_available_space(manifest, random_destination)
        finally:
            shutil.rmtree(random_destination)
