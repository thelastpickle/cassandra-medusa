import grpc
import sys

from medusa.service.grpc import medusa_pb2
from medusa.service.grpc import medusa_pb2_grpc

from grpc_health.v1 import health_pb2_grpc
from grpc_health.v1 import health_pb2

# open a gRPC channel
channel = grpc.insecure_channel('localhost:50051')

# create a health check stub
health_stub = health_pb2_grpc.HealthStub(channel)
request = health_pb2.HealthCheckRequest()
response = health_stub.Check(request)
print("health check response: {}".format(response))

# create a stub (client)
stub = medusa_pb2_grpc.MedusaStub(channel)
request = medusa_pb2.BackupRequest(name=sys.argv[2], mode=1)

response = stub.Backup(request)

print("Done!")
