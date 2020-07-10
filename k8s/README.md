# Overview
There are a several things involved for Kubernetes integration, some of which live in other repos. The purpose of this README is to provide an overview of the various components as well as to explain how to build things.

I want to point out that this k8s integration is independent of any k8s operators for Cassandra. There are several such operators, but not everyone is using an operator. This lower level of integration will make it easier for anyone to use Medusa in k8s. It will also facilitate integration with some of the Cassandra operators.

# Backups
There is a gRPC service for performing backups. It lives under the `medusa/service/grpc` directory. The service runs as a sidecar container in the Cassandra pod. There is a k8s operator for Medusa, [medusa-operator](https://github.com/jsanda/medusa-operator), that is the primary client of the gRPC service.

See [medusa-operator](https://github.com/jsanda/medusa-operator) for details on setting it up to perform restores.

## Jolokia
The current implementation uses [Jolokia](https://jolokia.org/) for creating and deleting snapshots. This will eventually be made configurable, but for now the Cassandra image will need to include the Jolokia agent.

# Restores
For k8s, we do not utilize `medusa/restore_cluster.py`. The `restore_cluster` module provides an orchestration layer that is handled differently in k8s. In k8s, we only use `medusa/restore_node.py`.

The image built out of this directory can be deployed in an [init container](https://kubernetes.io/docs/concepts/workloads/pods/init-containers/) in order to do restores. With this approach, it is possible to perform in-place as well as cluster-wide restores.

# gRPC Serivce
The gRPC API is defined in `medusa/service/grpc/medusa.proto`. At some point we might want to move the protobuf file to a different repo. Any clients, namely medusa-operator, need a copy of `medusa-proto` in order to generate client code.

While the gRPC service layer was added for k8s integration, it is worth pointing out that there is nothing prevents using it outside of k8s. In fact, I created and use `medusa/service/grpc/client.py` to do ad hoc testing locally on my laptop.

## Generating gRPC Code
You need to first install the protobuf and gRPC modules:

```
$ pip install -r requirements-grpc.txt
```

Next you need to run the protobuf compiler:

```
$ cd medusa/service/grpc

$ python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. medusa.protoc
``` 

These steps should be integrated into the build or setup.py at some point. I am just not sure where the appropriate integrations points are yet.

# Kubernetes Image
The `Dockerfile` and other scripts in this directory are intended for use in Kubernetes. 

For details on how to set up and configure the backup sidecar container and the restore init container, please see [cassandra-medusa-k8s](https://github.com/jsanda/cassandra-medusa-k8s).

The image can be used for backups or for restores. The `docker-entrypoint.sh` script looks at the `MEDUSA_MODE` env var to determine whehter it should run the gRPC service for backups or do a restore.

## Building the Image
If you made any changes to `medusa.proto`, then you first need to run the protobuf compiler as described above.

Run the following from the project root:

```
$ python setup.py build

$ docker build -t <tag name> -f k8s/Dockerfile .
```