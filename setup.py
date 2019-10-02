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
    name='medusa',
    version='0.1.0',
    author='Spotify',
    author_email='data-bye@spotify.com',
    url='https://github.com/spotify/cassandra-medusa',
    description='Prototype',
    long_description=long_description,
    long_description_content_type="text/markdown",
    license='Apache',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3.4',
        'Topic :: Database',
        'Topic :: System :: Archiving :: Backup'
    ],
    python_requires='>=3.4',
    packages=setuptools.find_packages(),
    install_requires=[
        'Click==6.7',
        'PyYAML==3.10',
        'google-cloud-storage==1.7.0',
        'cassandra-driver==3.14.0',
        'paramiko==2.6.0',
        'psutil==5.4.7',
        'ffwd==0.0.2',
        'apache-libcloud==2.4.0',
        'python-dateutil==2.8.0',
        'lockfile==0.12.2',
        'pycrypto==2.6.1',
        'retrying==1.3.3'
    ],
    entry_points={
        'console_scripts': [
            'medusa=medusa.medusacli:cli',
        ]},
    scripts=['bin/medusa-wrapper'],
    data_files=[('/etc/medusa', ['medusa-example.ini'])]
)
