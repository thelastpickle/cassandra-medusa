# Testing and Development

The integration test suite relies on [CCM](https://github.com/riptano/ccm) to create local single node clusters.

Usage: 

```
% ./run_integration_tests.sh -h
run_integration_tests.sh: Run the integration test suite for Medusa

options:
-h, --help                                  show brief help
-t, --test=1                                Test scenario to run (1, 2, 3, etc...). If not provided, all tests will run
--no-local                                  Don't run the tests with the local storage backend
--s3                                        Include S3 in the storage backends
--gcs                                       Include GCS in the storage backends
--azure                                     Include Azure in the storage backends
--ibm                                       Include IBM in the storage backends
--minio                                     Include Minio in the storage backends
--cassandra-version                         Cassandra version to test
-v                                          Verbose output (logging won't be captured by behave)
```

Run the integration tests on local storage only

```
./run_integration_tests.sh
```

To only run a specific scenario in the feature file, use the `-t`/`--tags` flag.

```
./run_integration_tests.sh -t <scenario number>
```

To run tests on GCS only:

```
./run_integration_tests.sh --no-local --gcs
```

To run tests with local storage and using a Cassandra 3.11.7 cluster:

```
./run_integration_tests.sh --cassandra-version=3.11.7
```

