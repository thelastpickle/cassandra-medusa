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


class MedusaService(medusa_pb2_grpc.MedusaServicer):

    def __init__(self):
        print("Init service")
        config_file = Path("/etc/medusa/medusa.ini")
        args = defaultdict(lambda: None)
        self.config = medusa.config.load_config(args, config_file)

    def Backup(self, request, context):
        print("Performing backup {}".format(request.name))
        # medusa.backup.main(self.config, request.name, None, "differential")
        medusa.backup.main(self.config, request.name, None, "differential")
        return medusa_pb2.BackupResponse()


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