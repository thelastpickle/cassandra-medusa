import time
from collections import defaultdict
from concurrent import futures
from pathlib import Path

import grpc
import grpc_health.v1.health

import medusa.backup
import medusa.config
from medusa.service.grpc_svc import medusa_pb2
from medusa.service.grpc_svc import medusa_pb2_grpc
from grpc_health.v1 import health_pb2_grpc
from datetime import datetime

from medusa.storage import Storage

TIMESTAMP_FORMAT = '%Y-%m-%d %H:%M:%S'


class MedusaService(medusa_pb2_grpc.MedusaServicer):

    def __init__(self):
        print("Init service")
        config_file = Path("/etc/medusa/medusa.ini")
        # config_file = Path("/Users/jsanda/tmp/medusa/medusa.ini")
        args = defaultdict(lambda: None)
        self.config = medusa.config.load_config(args, config_file)
        self.storage = Storage(config=self.config.storage)

    def Backup(self, request, context):
        print("Performing backup {}".format(request.name))
        # TODO pass the staggered and mode args
        medusa.backup.main(self.config, request.name, None, "differential")
        return medusa_pb2.BackupResponse()

    def BackupStatus(self, request, context):
        try:
            backup = self.storage.get_cluster_backup(request.name)

            response = medusa_pb2.BackupStatusResponse()
            # TODO how is the startTime determined?
            response.startTime = datetime.fromtimestamp(backup.started).strftime(TIMESTAMP_FORMAT)
            response.finishedNodes = [node.fqdn for node in backup.complete_nodes()]
            response.unfinishedNodes = [node.fqdn for node in backup.incomplete_nodes()]
            response.missingNodes = [node.fqdn for node in backup.missing_nodes()]

            if backup.finished:
                response.finishTime = datetime.fromtimestamp(backup.finished).strftime(TIMESTAMP_FORMAT)
            else:
                response.finishTime = ""

            return response
        except KeyError:
            raise Exception("backup <{}> does not exist".format(request.name))


server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

medusa_pb2_grpc.add_MedusaServicer_to_server(MedusaService(), server)
health_pb2_grpc.add_HealthServicer_to_server(grpc_health.v1.health.HealthServicer(), server)

print('Starting server. Listening on port 50051.')
server.add_insecure_port('[::]:50051')
server.start()

# since server.start() will not block,
# a sleep-loop is added to keep alive
try:
    while True:
        time.sleep(86400)
except KeyboardInterrupt:
    server.stop(0)