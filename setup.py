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
    version='0.11.3',
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
        'Programming Language :: Python :: 3.5',
        'Topic :: Database',
        'Topic :: System :: Archiving :: Backup'
    ],
    python_requires='>=3.5',
    packages=setuptools.find_packages(),
    install_requires=[
        'python-dateutil<2.8.1,>=2.1',
        'Click>=8.0.1',
        'click-aliases>=1.0.1',
        'PyYAML>=5.1',
        'cassandra-driver>=3.25.0',
        'psutil>=5.4.7',
        'ffwd>=0.0.2',
        'apache-libcloud<3.4.0,>=3.3.0',
        'lockfile>=0.12.2',
        'cryptography<=3.3.2,>=2.5',
        'pycryptodome>=3.9.9',
        'retrying>=1.3.3',
        'parallel-ssh==2.2.0',
        'ssh2-python==0.22.0',
        'ssh-python>=0.6.0',
        'requests==2.22.0',
        'protobuf>=3.12.0',
        'grpcio>=1.29.0',
        'grpcio-health-checking>=1.29.0',
        'grpcio-tools>=1.29.0',
        'gevent',
        'greenlet',
        'fasteners==0.16'
    ],
    extras_require={
        'S3': ["awscli>=1.16.291"],
        'GCS': ["google-cloud-storage>=1.7.0"],
        'AZURE': ["azure-cli>=2.24.0"]
    },
    entry_points={
        'console_scripts': [
            'medusa=medusa.medusacli:cli',
        ]},
    scripts=['bin/medusa-wrapper'],
    data_files=[('/etc/medusa', ['medusa-example.ini'])]
)
