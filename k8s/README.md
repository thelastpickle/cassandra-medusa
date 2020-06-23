The `Dockerfile` and other scripts in this directory are intended to be used with the image Kubernetes.

To build the image, run the following from the project root:

```
$ docker build -t <tag name> -f k8s/Dockerfile .
```