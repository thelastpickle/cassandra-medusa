Contributing
============

# Code of Conduct

This project adheres to the [Spotify FOSS Code of Conduct][code-of-conduct]. By participating, you are expected to honor this code.

[code-of-conduct]: CODE_OF_CONDUCT.md

# Tests

Before pushing a PR, please make sure you've added the appropriate unit tests (under `/tests/`) and integration tests (under `/tests/integration/`) for the changes you are making to the codebase.

We use [flake8](http://flake8.pycqa.org/en/latest/) for code style checking.  

We use [pytest for unit tests](http://pytest.readthedocs.io/en/latest/).
Older commits make use of [the standard Python framework](https://docs.python.org/3.6/library/unittest.html) and [nose](https://nose.readthedocs.io/en/latest/) but they are no longer used.
Feel free to migrate existing tests to pytest when adding new test cases.

We use [Aloe](https://aloe.readthedocs.io/en/latest/) as framework for running integration tests. As [Cucumber](https://cucumber.io/), it is a [Gherkin-based](https://cucumber.io/docs/gherkin/reference/) framework for writing test scenarios using natural language.

## Running tests

### Code style checking and unit tests

You can use tox to run both the code style checks and the unit tests.
Install tox in your virtual environment by running : `pip3 install tox`

Then, from the root of Medusa's repo, run : 

```
tox
```

You can run checks and unit tests individually with the following commands:

```
# Code checks
python3 -m "flake8" --ignore=W503,E402

# Unit tests
python3 -m pytest
```

### Integration tests

We've created a script to help running the integration tests at the root of the repository: `run_integration_tests.sh` 
Integration tests require [CCM](https://github.com/riptano/ccm) to be installed on your development machine:

```
pip install ccm
```

or on Mac:

```
brew install ccm
```

Calling `./run_integration_tests.sh` will run all the scenarios with local storage (uses `/tmp` as location to store the local bucket). 
Integration tests can run on S3 and/or Google Cloud Storage by uncommenting the appropriate lines in `tests/integration/features/integration_tests.feature`:

```
        Examples:
        | Storage   |
        | local      |
#        | google_storage      |
#        | s3_us_west_oregon      |
```

#### Testing on GCS

Enabling `google_storage` will have the following requirements:

* You already have a GCS bucket called `medusa_it_bucket` in your test project.
* You already created a service account [with the appropriate rights](docs/gcs_setup.md) and the key file for this service account is present in your home dir as `~/medusa_credentials.json`

#### Testing on AWS

Enabling S3 for integration tests can be done for any AWS region by changing `s3_us_west_oregon` to the appropriate value (see [the complete list here](https://github.com/apache/libcloud/blob/trunk/libcloud/storage/types.py#L87-L105)) and uncommenting the line.  
Then you'll have to:

* Set the name of your test bucket in `tests/integration/features/steps/integration_steps.py`, in the following block (replace `bucket_name`): 

```
   elif storage_provider.startswith("s3"):
        config['storage'] = {
            'host_file_separator': ',',
            'bucket_name': 'tlp-medusa-dev',
            'key_file': '~/.aws/credentials',
            'storage_provider': storage_provider,
            'fqdn': 'localhost',
            'api_key_or_username': '',
            'api_secret_or_password': '',
            'api_profile': 'default',
            'base_path': '/tmp'
        }
```        
* Place the appropriate aws `credentials` file under your home directory : `~/.aws/credentials` (see [the AWS S3 docs](docs/aws_s3_setup.md) for guidance)

#### Testing on Azure

Enabling `azure_blobs` will have the following requirements:

* You already have a storage account and the key file for this storage account is present in your home dir as `~/medusa_azure_credentials.json`
* You already have a Azure Blob container called `medusa-integration-tests` in your storage account.

# Submitting Pull Requests

We are happily accepting pull requests to improve Medusa.
Every push to a PR will automatically trigger a build, which will run code style checking, unit tests and integration tests.

Build must be passing for the PR to be eligible for merge.
