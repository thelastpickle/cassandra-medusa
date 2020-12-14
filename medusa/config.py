# -*- coding: utf-8 -*-
# Copyright 2019 Spotify AB. All rights reserved.
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

import collections
import configparser
import logging
import os
import pathlib
import socket
import sys

import medusa.storage
import medusa.cassandra_utils
from medusa.utils import evaluate_boolean

StorageConfig = collections.namedtuple(
    'StorageConfig',
    ['bucket_name', 'key_file', 'prefix', 'fqdn', 'host_file_separator', 'storage_provider',
     'base_path', 'max_backup_age', 'max_backup_count', 'api_profile', 'transfer_max_bandwidth',
     'concurrent_transfers', 'multi_part_upload_threshold', 'host', 'region', 'port', 'secure', 'aws_cli_path']
)

CassandraConfig = collections.namedtuple(
    'CassandraConfig',
    ['start_cmd', 'stop_cmd', 'config_file', 'cql_username', 'cql_password', 'check_running', 'is_ccm',
     'sstableloader_bin', 'nodetool_username', 'nodetool_password', 'nodetool_password_file_path', 'nodetool_host',
     'nodetool_port', 'certfile', 'usercert', 'userkey', 'sstableloader_ts', 'sstableloader_tspw',
     'sstableloader_ks', 'sstableloader_kspw', 'nodetool_ssl', 'resolve_ip_addresses']
)

SSHConfig = collections.namedtuple(
    'SSHConfig',
    ['username', 'key_file', 'port']
)

ChecksConfig = collections.namedtuple(
    'ChecksConfig',
    ['health_check', 'query', 'expected_rows', 'expected_result']
)

MonitoringConfig = collections.namedtuple(
    'MonitoringConfig',
    ['monitoring_provider']
)

MedusaConfig = collections.namedtuple(
    'MedusaConfig',
    ['storage', 'cassandra', 'ssh', 'checks', 'monitoring', 'logging', 'grpc', 'kubernetes']
)

LoggingConfig = collections.namedtuple(
    'LoggingConfig',
    ['enabled', 'file', 'format', 'level', 'maxBytes', 'backupCount']
)

GrpcConfig = collections.namedtuple(
    'GrpcConfig',
    ['enabled']
)

KubernetesConfig = collections.namedtuple(
    'KubernetesConfig',
    ['enabled', 'cassandra_url', 'use_mgmt_api']
)

DEFAULT_CONFIGURATION_PATH = pathlib.Path('/etc/medusa/medusa.ini')


def load_config(args, config_file):
    config = configparser.ConfigParser(interpolation=None)

    # Set defaults

    config['storage'] = {
        'host_file_separator': ',',
        'max_backup_age': 0,
        'max_backup_count': 0,
        'api_profile': 'default',
        'transfer_max_bandwidth': '50MB/s',
        'concurrent_transfers': 1,
        'multi_part_upload_threshold': 100 * 1024 * 1024,
        'secure': True,
        'aws_cli_path': 'aws',
        'fqdn': socket.getfqdn(),
        'region': 'default',
    }

    config['logging'] = {
        'enabled': 'false',
        'file': 'medusa.log',
        'level': 'INFO',
        'format': '[%(asctime)s] %(levelname)s: %(message)s',
        'maxBytes': 20000000,
        'backupCount': 50,
    }

    config['cassandra'] = {
        'config_file': medusa.cassandra_utils.CassandraConfigReader.DEFAULT_CASSANDRA_CONFIG,
        'start_cmd': 'sudo service cassandra start',
        'stop_cmd': 'sudo service cassandra stop',
        'check_running': 'nodetool version',
        'is_ccm': 0,
        'sstableloader_bin': 'sstableloader',
        'resolve_ip_addresses': True
    }

    config['ssh'] = {
        'username': os.environ.get('USER') or '',
        'key_file': '',
        'port': 22
    }

    config['checks'] = {
        'health_check': 'cql',
        'query': '',
        'expected_rows': '0',
        'expected_result': ''
    }

    config['monitoring'] = {
        'monitoring_provider': 'None'
    }

    config['grpc'] = {
        'enabled': False,
    }

    config['kubernetes'] = {
        'enabled': False,
        'cassandra_url': 'None',
        'use_mgmt_api': False
    }

    if config_file:
        logging.debug('Loading configuration from {}'.format(config_file))
        config.read_file(config_file.open())
    elif DEFAULT_CONFIGURATION_PATH.exists():
        logging.debug('Loading configuration from {}'.format(DEFAULT_CONFIGURATION_PATH))
        config.read_file(DEFAULT_CONFIGURATION_PATH.open())
    else:
        logging.error(
            'No configuration file provided via CLI, nor no default file found in {}'.format(DEFAULT_CONFIGURATION_PATH)
        )
        sys.exit(1)

    config.read_dict({'storage': {
        key: value
        for key, value in _zip_fields_with_arg_values(StorageConfig._fields, args)
        if value is not None
    }})

    config.read_dict({'logging': {
        key: value
        for key, value in _zip_fields_with_arg_values(LoggingConfig._fields, args)
        if value is not None
    }})

    config.read_dict({'ssh': {
        key: value
        for key, value in _zip_fields_with_arg_values(SSHConfig._fields, args)
        if value is not None
    }})

    config.read_dict({'checks': {
        key: value
        for key, value in _zip_fields_with_arg_values(ChecksConfig._fields, args)
        if value is not None
    }})

    config.read_dict({'monitoring': {
        key: value
        for key, value in _zip_fields_with_arg_values(MonitoringConfig._fields, args)
        if value is not None
    }})

    config.read_dict({'grpc': {
        key: value
        for key, value in _zip_fields_with_arg_values(GrpcConfig._fields, args)
        if value is not None
    }})

    config.read_dict({'kubernetes': {
        key: value
        for key, value in _zip_fields_with_arg_values(KubernetesConfig._fields, args)
        if value is not None
    }})

    resolve_ip_addresses = evaluate_boolean(config['cassandra']['resolve_ip_addresses'])
    config.set('cassandra', 'resolve_ip_addresses', 'True' if resolve_ip_addresses else 'False')
    if config['storage']['fqdn'] == socket.getfqdn() and not resolve_ip_addresses:
        # Use the ip address instead of the fqdn when DNS resolving is turned off
        config['storage']['fqdn'] = socket.gethostbyname(socket.getfqdn())

    if "CQL_USERNAME" in os.environ:
        config['cassandra']['cql_username'] = os.environ["CQL_USERNAME"]
    if "CQL_PASSWORD" in os.environ:
        config['cassandra']['cql_password'] = os.environ["CQL_PASSWORD"]

    medusa_config = MedusaConfig(
        storage=_namedtuple_from_dict(StorageConfig, config['storage']),
        cassandra=_namedtuple_from_dict(CassandraConfig, config['cassandra']),
        ssh=_namedtuple_from_dict(SSHConfig, config['ssh']),
        checks=_namedtuple_from_dict(ChecksConfig, config['checks']),
        monitoring=_namedtuple_from_dict(MonitoringConfig, config['monitoring']),
        logging=_namedtuple_from_dict(LoggingConfig, config['logging']),
        grpc=_namedtuple_from_dict(GrpcConfig, config['grpc']),
        kubernetes=_namedtuple_from_dict(KubernetesConfig, config['kubernetes'])
    )

    for field in ['bucket_name', 'storage_provider']:
        if getattr(medusa_config.storage, field) is None:
            logging.error('Required configuration "{}" is missing in [storage] section.'.format(field))
            sys.exit(2)

    for field in ['start_cmd', 'stop_cmd']:
        if getattr(medusa_config.cassandra, field) is None:
            logging.error('Required configuration "{}" is missing in [cassandra] section.'.format(field))
            sys.exit(2)

    for field in ['username', 'key_file']:
        if getattr(medusa_config.ssh, field) is None:
            logging.error('Required configuration "{}" is missing in [ssh] section.'.format(field))
            sys.exit(2)

    return medusa_config


def _zip_fields_with_arg_values(fields, args):
    return [(field, args[field] if (field in args) else None) for field in fields]


def _namedtuple_from_dict(cls, data):
    return cls(**{
        field: data.get(field)
        for field in cls._fields
    })
