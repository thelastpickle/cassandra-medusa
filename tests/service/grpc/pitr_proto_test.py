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

import grpc

from google.protobuf.descriptor import FieldDescriptor as FD

from medusa.service.grpc import medusa_pb2, medusa_pb2_grpc


class PitrProtoTest(unittest.TestCase):

    def test_new_messages_exist_with_defaults(self):
        self.assertEqual([], medusa_pb2.GetCommitLogArchiveStatusRequest().ListFields())

        status = medusa_pb2.GetCommitLogArchiveStatusResponse()
        self.assertFalse(status.running)
        self.assertEqual(0, status.lastUploadTime)
        self.assertEqual(0, status.pendingCount)
        self.assertEqual(0, status.intervalSeconds)
        self.assertEqual(medusa_pb2.StatusType.IN_PROGRESS, status.status)
        self.assertEqual('', status.lastError)

        request = medusa_pb2.PreparePitrRestoreRequest()
        self.assertEqual('', request.targetTimestamp)
        self.assertEqual('', request.datacenter)

        response = medusa_pb2.PreparePitrRestoreResponse()
        self.assertEqual(medusa_pb2.StatusType.IN_PROGRESS, response.status)
        self.assertEqual(0, len(response.nodeBaseBackups))
        self.assertEqual(0.0, response.targetTimestamp)

    def test_commitlog_archive_status_response_round_trip(self):
        original = medusa_pb2.GetCommitLogArchiveStatusResponse(
            running=True,
            lastUploadTime=1700000000,
            pendingCount=3,
            intervalSeconds=60,
            status=medusa_pb2.StatusType.SUCCESS,
            lastError='spool directory unreadable',
        )
        parsed = medusa_pb2.GetCommitLogArchiveStatusResponse()
        parsed.ParseFromString(original.SerializeToString())

        self.assertTrue(parsed.running)
        self.assertEqual(1700000000, parsed.lastUploadTime)
        self.assertEqual(3, parsed.pendingCount)
        self.assertEqual(60, parsed.intervalSeconds)
        self.assertEqual(medusa_pb2.StatusType.SUCCESS, parsed.status)
        self.assertEqual('spool directory unreadable', parsed.lastError)

    def test_prepare_pitr_restore_request_round_trip(self):
        original = medusa_pb2.PreparePitrRestoreRequest(
            targetTimestamp='2026-01-01T00:00:00Z',
            datacenter='dc1',
        )
        parsed = medusa_pb2.PreparePitrRestoreRequest()
        parsed.ParseFromString(original.SerializeToString())
        self.assertEqual('2026-01-01T00:00:00Z', parsed.targetTimestamp)
        self.assertEqual('dc1', parsed.datacenter)

    def test_prepare_pitr_restore_response_accepts_map_entries(self):
        response = medusa_pb2.PreparePitrRestoreResponse(
            status=medusa_pb2.StatusType.SUCCESS,
            targetTimestamp=1700000000.5,
        )
        response.nodeBaseBackups['node1.example.com'] = 'backup1'
        response.nodeBaseBackups['node2.example.com'] = 'backup2'

        parsed = medusa_pb2.PreparePitrRestoreResponse()
        parsed.ParseFromString(response.SerializeToString())

        self.assertEqual(
            {'node1.example.com': 'backup1', 'node2.example.com': 'backup2'},
            dict(parsed.nodeBaseBackups),
        )
        self.assertEqual(medusa_pb2.StatusType.SUCCESS, parsed.status)
        self.assertEqual(1700000000.5, parsed.targetTimestamp)

    def test_new_messages_field_numbers_and_types_are_pinned(self):
        # Field numbers and types are the wire contract shared with k8ssandra-operator's Go stubs.
        # Renumbering or retyping a field would still pass the round-trip tests above,
        # but would silently break the operator.
        expected = {
            'GetCommitLogArchiveStatusRequest': {},
            'GetCommitLogArchiveStatusResponse': {
                'running': (1, FD.TYPE_BOOL),
                'lastUploadTime': (2, FD.TYPE_INT64),
                'pendingCount': (3, FD.TYPE_INT32),
                'intervalSeconds': (4, FD.TYPE_INT32),
                'status': (5, FD.TYPE_ENUM),
                'lastError': (6, FD.TYPE_STRING),
            },
            'PreparePitrRestoreRequest': {
                'targetTimestamp': (1, FD.TYPE_STRING),
                'datacenter': (2, FD.TYPE_STRING),
            },
            'PreparePitrRestoreResponse': {
                'status': (1, FD.TYPE_ENUM),
                # map<string, string> is encoded as a repeated map-entry message
                'nodeBaseBackups': (2, FD.TYPE_MESSAGE),
                'targetTimestamp': (3, FD.TYPE_DOUBLE),
            },
        }
        for message_name, expected_fields in expected.items():
            descriptor = medusa_pb2.DESCRIPTOR.message_types_by_name[message_name]
            actual_fields = {f.name: (f.number, f.type) for f in descriptor.fields}
            self.assertEqual(expected_fields, actual_fields, message_name)

    def test_servicer_has_new_rpcs(self):
        servicer = medusa_pb2_grpc.MedusaServicer()
        self.assertTrue(callable(servicer.GetCommitLogArchiveStatus))
        self.assertTrue(callable(servicer.PreparePitrRestore))

    def test_stub_has_new_rpcs(self):
        channel = grpc.insecure_channel('127.0.0.1:0')
        try:
            stub = medusa_pb2_grpc.MedusaStub(channel)
            self.assertTrue(hasattr(stub, 'GetCommitLogArchiveStatus'))
            self.assertTrue(hasattr(stub, 'PreparePitrRestore'))
        finally:
            channel.close()

    def test_existing_backup_request_unchanged(self):
        fields = {f.name: (f.number, f.type) for f in medusa_pb2.BackupRequest.DESCRIPTOR.fields}
        self.assertEqual(
            {
                'name': (1, medusa_pb2.BackupRequest.DESCRIPTOR.fields_by_name['name'].TYPE_STRING),
                'mode': (2, medusa_pb2.BackupRequest.DESCRIPTOR.fields_by_name['mode'].TYPE_ENUM),
            },
            fields,
        )
        self.assertEqual(0, medusa_pb2.BackupRequest.DIFFERENTIAL)
        self.assertEqual(1, medusa_pb2.BackupRequest.FULL)


if __name__ == '__main__':
    unittest.main()
