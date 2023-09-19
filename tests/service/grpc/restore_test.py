# -*- coding: utf-8 -*-
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
import os

from medusa.service.grpc.restore import apply_mapping_env


class ServiceRestoreTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def setUp(self):
        os.environ.pop('POD_IP', None)
        os.environ.pop('POD_NAME', None)
        os.environ.pop('RESTORE_MAPPING', None)

    def test_restore_inplace(self):
        os.environ['POD_NAME'] = 'test-dc1-sts-0'
        os.environ['RESTORE_MAPPING'] = '{"in_place": true, "host_map": {' \
            + '"test-dc1-sts-0": {"source": ["test-dc1-sts-0"], "seed": false},' \
            + '"test-dc1-sts-1": {"source": ["test-dc1-sts-1"], "seed": false},' \
            + '"test-dc1-sts-2": {"source": "prod-dc1-sts-2", "seed": false}}}'
        in_place = apply_mapping_env()

        assert in_place is True
        assert "POD_IP" not in os.environ.keys()

    def test_restore_remote(self):
        os.environ.update({'POD_NAME': 'test-dc1-sts-0'})
        os.environ['RESTORE_MAPPING'] = '{"in_place": false, "host_map": {' \
            + '"test-dc1-sts-0": {"source": ["prod-dc1-sts-3"], "seed": false},' \
            + '"test-dc1-sts-1": {"source": ["prod-dc1-sts-1"], "seed": false},' \
            + '"test-dc1-sts-2": {"source": "prod-dc1-sts-2", "seed": false}}}'
        in_place = apply_mapping_env()

        assert in_place is False
        assert "POD_IP" in os.environ.keys()
        assert os.environ['POD_IP'] == 'prod-dc1-sts-3'

    def test_restore_no_match(self):
        os.environ['POD_NAME'] = 'test-dc1-sts-0'
        os.environ['RESTORE_MAPPING'] = '{"in_place": false, "host_map": {' \
            + '"test-dc1-sts-3": {"source": ["prod-dc1-sts-3"], "seed": false},' \
            + '"test-dc1-sts-1": {"source": ["prod-dc1-sts-1"], "seed": false},' \
            + '"test-dc1-sts-2": {"source": "prod-dc1-sts-2", "seed": false}}}'
        in_place = apply_mapping_env()

        assert in_place is None
        assert "POD_IP" not in os.environ.keys()


if __name__ == '__main__':
    unittest.main()
