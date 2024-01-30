#! /usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright 2018 Spotify AB
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

import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name='cassandra-medusa',
    version='0.17.2',
    author='The Last Pickle',
    author_email='medusa@thelastpickle.com',
    url='https://github.com/thelastpickle/cassandra-medusa',
    description='Apache Cassandra backup and restore tool',
    long_description=long_description,
    long_description_content_type="text/markdown",
    license='Apache',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3.7',
        'Topic :: Database',
        'Topic :: System :: Archiving :: Backup'
    ],
    python_requires='>=3.7',
    packages=setuptools.find_packages(),
    install_requires=[
        'python-dateutil<2.8.1,>=2.1',
        'Click>=8.0.1',
        'click-aliases==1.0.1',
        'PyYAML>=5.1',
        'cassandra-driver>=3.27.0',
        'psutil>=5.4.7',
        'ffwd>=0.0.2',
        'lockfile>=0.12.2',
        'pyOpenSSL==22.0.0',
        'cryptography<=35.0,>=2.5',
        'pycryptodome>=3.9.9',
        'retrying>=1.3.3',
        'parallel-ssh==2.2.0',
        'ssh2-python==1.0.0',
        'ssh-python>=0.8.0',
        'requests==2.22.0',
        'protobuf==4.24.3',
        'grpcio==1.58.0',
        'grpcio-health-checking==1.58.0',
        'grpcio-tools==1.58.0',
        'gevent',
        'greenlet',
        'fasteners==0.16',
        'datadog',
        'botocore>=1.13.27',
        'dnspython>=2.2.1',
        'asyncio==3.4.3',
        'aiohttp==3.8.5',
        'boto3>=1.28.38',
        'gcloud-aio-storage==8.3.0',
        'azure-core==1.29.4',
        'azure-identity==1.14.0',
        'azure-storage-blob==12.17.0'
    ],
    entry_points={
        'console_scripts': [
            'medusa=medusa.medusacli:cli',
        ]},
    scripts=['bin/medusa-wrapper'],
    data_files=[('/etc/medusa', ['medusa-example.ini'])]
)
