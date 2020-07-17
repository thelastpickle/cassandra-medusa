import grpc
from grpc_health.v1 import health_pb2
from grpc_health.v1 import health_pb2_grpc

from medusa.service.grpc import medusa_pb2
from medusa.service.grpc import medusa_pb2_grpc


class Client:
    def __init__(self, target, channel_options=[]):
        self.channel = grpc.insecure_channel(target, options=channel_options)

    def health_check(self):
        health_stub = health_pb2_grpc.HealthStub(self.channel)
        request = health_pb2.HealthCheckRequest()
        return health_stub.Check(request)

    def backup(self, name, mode):
        stub = medusa_pb2_grpc.MedusaStub(self.channel)
        if mode == "differential":
            backup_mode = 0
        elif mode == "full":
            backup_mode = 1
        else:
            raise Exception("{} is not a recognized backup mode".format(mode))

        request = medusa_pb2.BackupRequest(name=name, mode=backup_mode)
        return stub.Backup(request)

    def delete_backup(self, name):
        stub = medusa_pb2_grpc.MedusaStub(self.channel)
        request = medusa_pb2.DeleteBackupRequest(name=name)
        stub.DeleteBackup(request)

    def backup_exists(self, name):
        stub = medusa_pb2_grpc.MedusaStub(self.channel)
        try:
            request = medusa_pb2.BackupStatusRequest(backupName=name)
            stub.BackupStatus(request)
            return True
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.NOT_FOUND:
                return False
            raise e
