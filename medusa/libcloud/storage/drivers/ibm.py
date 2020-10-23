# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from libcloud.common.types import LibcloudError
from libcloud.common.aws import SignedAWSConnection
from libcloud.storage.drivers.s3 import BaseS3Connection
from libcloud.storage.drivers.s3 import BaseS3StorageDriver, API_VERSION

__all__ = [
    'IBMCloudStorageDriver',
]

IBM_CLOUD_DEFAULT_REGION = 's3.eu.cloud-object-storage.appdomain.cloud'

IBM_CLOUD_HOSTS_BY_REGION = \
    {"us-standard": "s3.us.cloud-object-storage.appdomain.cloud",
     "us-vault": "s3.us.cloud-object-storage.appdomain.cloud",
     "us-cold": "s3.us.cloud-object-storage.appdomain.cloud",
     "us-smart": "s3.us.cloud-object-storage.appdomain.cloud",
     "us-east-standard": "s3.us.cloud-object-storage.appdomain.cloud",
     "us-east-vault": "s3.us.cloud-object-storage.appdomain.cloud",
     "us-east-cold": "s3.us.cloud-object-storage.appdomain.cloud",
     "us-east-smart": "s3.us.cloud-object-storage.appdomain.cloud",
     "us-south-standard": "s3.us.cloud-object-storage.appdomain.cloud",
     "us-south-vault": "s3.us.cloud-object-storage.appdomain.cloud",
     "us-south-cold": "s3.us.cloud-object-storage.appdomain.cloud",
     "us-south-smart": "s3.us.cloud-object-storage.appdomain.cloud",
     "eu-standard": "s3.eu.cloud-object-storage.appdomain.cloud",
     "eu-vault": "s3.eu.cloud-object-storage.appdomain.cloud",
     "eu-cold": "s3.eu.cloud-object-storage.appdomain.cloud",
     "eu-smart": "s3.eu.cloud-object-storage.appdomain.cloud",
     "eu-gb-standard": "s3.eu.cloud-object-storage.appdomain.cloud",
     "eu-gb-vault": "s3.eu.cloud-object-storage.appdomain.cloud",
     "eu-gb-cold": "s3.eu.cloud-object-storage.appdomain.cloud",
     "eu-gb-smart": "s3.eu.cloud-object-storage.appdomain.cloud",
     "eu-de-standard": "s3.eu.cloud-object-storage.appdomain.cloud",
     "eu-de-vault": "s3.eu.cloud-object-storage.appdomain.cloud",
     "eu-de-cold": "s3.eu.cloud-object-storage.appdomain.cloud",
     "eu-de-smart": "s3.eu.cloud-object-storage.appdomain.cloud",
     "ap-standard": "s3.ap.cloud-object-storage.appdomain.cloud",
     "ap-vault": "s3.ap.cloud-object-storage.appdomain.cloud",
     "ap-cold": "s3.ap.cloud-object-storage.appdomain.cloud",
     "ap-smart": "s3.ap.cloud-object-storage.appdomain.cloud",
     "jp-tok-standard": "s3.tok.ap.cloud-object-storage.appdomain.cloud",
     "jp-tok-vault": "s3.tok.ap.cloud-object-storage.appdomain.cloud",
     "jp-tok-cold": "s3.tok.ap.cloud-object-storage.appdomain.cloud",
     "jp-tok-smart": "s3.tok.ap.cloud-object-storage.appdomain.cloud",
     "jp-osa-standard": "s3.jp-osa.cloud-object-storage.appdomain.cloud",
     "jp-osa-vault": "s3.jp-osa.cloud-object-storage.appdomain.cloud",
     "jp-osa-cold": "s3.jp-osa.cloud-object-storage.appdomain.cloud",
     "jp-osa-smart": "s3.jp-osa.cloud-object-storage.appdomain.cloud",
     "au-syd-standard": "s3.au-syd.cloud-object-storage.appdomain.cloud",
     "au-syd-vault": "s3.au-syd.cloud-object-storage.appdomain.cloud",
     "au-syd-cold": "s3.au-syd.cloud-object-storage.appdomain.cloud",
     "au-syd-smart": "s3.au-syd.cloud-object-storage.appdomain.cloud",
     "ams03-standard": "s3.ams.eu.cloud-object-storage.appdomain.cloud",
     "ams03-vault": "s3.ams.eu.cloud-object-storage.appdomain.cloud",
     "ams03-cold": "s3.ams.eu.cloud-object-storage.appdomain.cloud",
     "ams03-smart": "s3.ams.eu.cloud-object-storage.appdomain.cloud",
     "che01-standard": "s3.che01.cloud-object-storage.appdomain.cloud",
     "che01-vault": "s3.che01.cloud-object-storage.appdomain.cloud",
     "che01-cold": "s3.che01.cloud-object-storage.appdomain.cloud",
     "che01-smart": "s3.che01.cloud-object-storage.appdomain.cloud",
     "hkg02-standard": "s3.hkg.ap.cloud-object-storage.appdomain.cloud",
     "hkg02-vault": "s3.hkg.ap.cloud-object-storage.appdomain.cloud",
     "hkg02-cold": "s3.hkg.ap.cloud-object-storage.appdomain.cloud",
     "hkg02-smart": "s3.hkg.ap.cloud-object-storage.appdomain.cloud",
     "mel01-standard": "s3.mel01.cloud-object-storage.appdomain.cloud",
     "mel01-vault": "s3.mel01.cloud-object-storage.appdomain.cloud",
     "mel01-cold": "s3.mel01.cloud-object-storage.appdomain.cloud",
     "mel01-smart": "s3.mel01.cloud-object-storage.appdomain.cloud",
     "mex01-standard": "s3.mex01.cloud-object-storage.appdomain.cloud",
     "mex01-vault": "s3.mex01.cloud-object-storage.appdomain.cloud",
     "mex01-cold": "s3.mex01.cloud-object-storage.appdomain.cloud",
     "mex01-smart": "s3.mex01.cloud-object-storage.appdomain.cloud",
     "mil01-standard": "s3.mil01.cloud-object-storage.appdomain.cloud",
     "mil01-vault": "s3.mil01.cloud-object-storage.appdomain.cloud",
     "mil01-cold": "s3.mil01.cloud-object-storage.appdomain.cloud",
     "mil01-smart": "s3.mil01.cloud-object-storage.appdomain.cloud",
     "mon01-standard": "s3.mon01.cloud-object-storage.appdomain.cloud",
     "mon01-vault": "s3.mon01.cloud-object-storage.appdomain.cloud",
     "mon01-cold": "s3.mon01.cloud-object-storage.appdomain.cloud",
     "mon01-smart": "s3.mon01.cloud-object-storage.appdomain.cloud",
     "par01-standard": "s3.par01.cloud-object-storage.appdomain.cloud",
     "par01-vault": "s3.par01.cloud-object-storage.appdomain.cloud",
     "par01-cold": "s3.par01.cloud-object-storage.appdomain.cloud",
     "par01-smart": "s3.par01.cloud-object-storage.appdomain.cloud",
     "osl01-standard": "s3.osl01.cloud-object-storage.appdomain.cloud",
     "osl01-vault": "s3.osl01.cloud-object-storage.appdomain.cloud",
     "osl01-cold": "s3.osl01.cloud-object-storage.appdomain.cloud",
     "osl01-smart": "s3.osl01.cloud-object-storage.appdomain.cloud",
     "sjc04-standard": "s3.sjc04.cloud-object-storage.appdomain.cloud",
     "sjc04-vault": "s3.sjc04.cloud-object-storage.appdomain.cloud",
     "sjc04-cold": "s3.sjc04.cloud-object-storage.appdomain.cloud",
     "sjc04-smart": "s3.sjc04.cloud-object-storage.appdomain.cloud",
     "sao01-standard": "s3.sao01.cloud-object-storage.appdomain.cloud",
     "sao01-vault": "s3.sao01.cloud-object-storage.appdomain.cloud",
     "sao01-cold": "s3.sao01.cloud-object-storage.appdomain.cloud",
     "sao01-smart": "s3.sao01.cloud-object-storage.appdomain.cloud",
     "seo01-standard": "s3.seo01.cloud-object-storage.appdomain.cloud",
     "seo01-vault": "s3.seo01.cloud-object-storage.appdomain.cloud",
     "seo01-cold": "s3.seo01.cloud-object-storage.appdomain.cloud",
     "seo01-smart": "s3.seo01.cloud-object-storage.appdomain.cloud",
     "sng01-standard": "s3.sng01.cloud-object-storage.appdomain.cloud",
     "sng01-vault": "s3.sng01.cloud-object-storage.appdomain.cloud",
     "sng01-cold": "s3.sng01.cloud-object-storage.appdomain.cloud",
     "sng01-smart": "s3.sng01.cloud-object-storage.appdomain.cloud",
     "tor01-standard": "s3.tor01.cloud-object-storage.appdomain.cloud",
     "tor01-vault": "s3.tor01.cloud-object-storage.appdomain.cloud",
     "tor01-cold": "s3.tor01.cloud-object-storage.appdomain.cloud",
     "tor01-smart": "s3.tor01.cloud-object-storage.appdomain.cloud"}

IBM_CLOUD_DEFAULT_REGION = 'eu-smart'


class IBMCloudConnectionAWS(SignedAWSConnection, BaseS3Connection):
    service_name = 's3'
    version = API_VERSION

    def __init__(self, user_id, key, secure=True, host=None, port=None,
                 url=None, timeout=None, proxy_url=None, token=None,
                 retry_delay=None, backoff=None, **kwargs):

        super(IBMCloudConnectionAWS, self).__init__(user_id, key,
                                                    secure, host,
                                                    port, url,
                                                    timeout,
                                                    proxy_url, token,
                                                    retry_delay,
                                                    backoff,
                                                    4)  # force aws4


class IBMStorageDriver(BaseS3StorageDriver):
    name = 'IBM Cloud Storage'
    website = 'http://cloud.ibm.com/'

    def __init__(self, key, secret=None, secure=True, host=None, port=None,
                 api_version=None, region=IBM_CLOUD_DEFAULT_REGION, **kwargs):

        if host is None:
            raise LibcloudError('host required', driver=self)

        self.name = kwargs.pop('name', None)
        if self.name is None:
            self.name = 'IBM Cloud Object Storage (%s)' % (region)

        self.ex_location_name = region
        self.region_name = region
        self.connectionCls = IBMCloudConnectionAWS
        self.connectionCls.host = host

        super(IBMStorageDriver, self).__init__(key, secret,
                                               secure, host, port,
                                               api_version, region,
                                               **kwargs)


class IBMCloudStorageDriver(IBMStorageDriver):
    name = 'IBM Cloud Storage'
    website = 'http://cloud.ibm.com/'

    def __init__(self, key, secret=None, secure=True, host=None, port=None,
                 api_version=None, region=IBM_CLOUD_DEFAULT_REGION,
                 **kwargs):
        if region not in IBM_CLOUD_HOSTS_BY_REGION:
            raise LibcloudError('Unknown region (%s)' % (region), driver=self)
        if host is None:
            host = IBM_CLOUD_HOSTS_BY_REGION[region]
        kwargs['name'] = 'IBM Cloud Object Storage region (%s)' % region
        super(IBMCloudStorageDriver, self).__init__(key, secret,
                                                    secure, host, port,
                                                    api_version, region,
                                                    **kwargs)
