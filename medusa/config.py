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

import medusa.cassandra_utils
import medusa.storage
from medusa.utils import evaluate_boolean

StorageConfig = collections.namedtuple(
    'StorageConfig',
    ['bucket_name', 'key_file', 'prefix', 'fqdn', 'host_file_separator', 'storage_provider',
     'base_path', 'max_backup_age', 'max_backup_count', 'api_profile', 'transfer_max_bandwidth',
     'concurrent_transfers', 'multi_part_upload_threshold', 'host', 'region', 'port', 'secure', 'aws_cli_path',
     'backup_grace_period_in_days']
)

CassandraConfig = collections.namedtuple(
    'CassandraConfig',
    ['start_cmd', 'stop_cmd', 'config_file', 'cql_username', 'cql_password', 'check_running', 'is_ccm',
     'sstableloader_bin', 'nodetool_username', 'nodetool_password', 'nodetool_password_file_path', 'nodetool_host',
     'nodetool_port', 'certfile', 'usercert', 'userkey', 'sstableloader_ts', 'sstableloader_tspw',
     'sstableloader_ks', 'sstableloader_kspw', 'nodetool_ssl', 'resolve_ip_addresses', 'use_sudo']
)

SSHConfig = collections.namedtuple(
    'SSHConfig',
    ['username', 'key_file', 'port', 'cert_file']
)

ChecksConfig = collections.namedtuple(
    'ChecksConfig',
    ['health_check', 'query', 'expected_rows', 'expected_result', 'enable_md5_checks']
)

MonitoringConfig = collections.namedtuple(
    'MonitoringConfig',
    ['monitoring_provider']
)

MedusaConfig = collections.namedtuple(
    'MedusaConfig',
    [
        'file_path',  # Store a specific config file path, None for default file
        'storage', 'cassandra', 'ssh', 'checks', 'monitoring', 'logging', 'grpc', 'kubernetes'
    ]
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

CONFIG_SECTIONS = {
    'storage': StorageConfig,
    'cassandra': CassandraConfig,
    'ssh': SSHConfig,
    'checks': ChecksConfig,
    'monitoring': MonitoringConfig,
    'logging': LoggingConfig,
    'grpc': GrpcConfig,
    'kubernetes': KubernetesConfig
}

DEFAULT_CONFIGURATION_PATH = pathlib.Path('/etc/medusa/medusa.ini')


def _build_default_config():
    """Build a INI config parser with default values

    :return ConfigParser: default configuration
    """
    config = configparser.ConfigParser(interpolation=None)

    config['storage'] = {
        'host_file_separator': ',',
        'max_backup_age': '0',
        'max_backup_count': '0',
        'api_profile': 'default',
        'transfer_max_bandwidth': '50MB/s',
        'concurrent_transfers': '1',
        'multi_part_upload_threshold': str(100 * 1024 * 1024),
        'secure': 'True',
        'aws_cli_path': 'aws',
        'fqdn': socket.getfqdn(),
        'region': 'default',
        'backup_grace_period_in_days': 10,
    }

    config['logging'] = {
        'enabled': 'false',
        'file': 'medusa.log',
        'level': 'INFO',
        'format': '[%(asctime)s] %(levelname)s: %(message)s',
        'maxBytes': '20000000',
        'backupCount': '50',
    }

    config['cassandra'] = {
        'config_file': medusa.cassandra_utils.CassandraConfigReader.DEFAULT_CASSANDRA_CONFIG,
        'start_cmd': 'sudo service cassandra start',
        'stop_cmd': 'sudo service cassandra stop',
        'check_running': 'nodetool version',
        'is_ccm': '0',
        'sstableloader_bin': 'sstableloader',
        'resolve_ip_addresses': 'True',
        'use_sudo': 'True',
    }

    config['ssh'] = {
        'username': os.environ.get('USER') or '',
        'key_file': '',
        'port': '22',
        'cert_file': ''
    }

    config['checks'] = {
        'health_check': 'cql',
        'query': '',
        'expected_rows': '0',
        'expected_result': '',
        'enable_md5_checks': 'false'
    }

    config['monitoring'] = {
        'monitoring_provider': 'None'
    }

    config['grpc'] = {
        'enabled': 'False',
    }

    config['kubernetes'] = {
        'enabled': 'False',
        'cassandra_url': 'None',
        'use_mgmt_api': 'False'
    }
    return config


def parse_config(args, config_file):
    """Parse a medusa.ini file and allow to override settings from command line

    :param dict args: settings override. Higher priority than settings defined in medusa.ini
    :param pathlib.Path config_file: path to medusa.ini file
    :return: None
    """
    config = _build_default_config()

    if config_file is None and not DEFAULT_CONFIGURATION_PATH.exists():
        logging.error(
            'No configuration file provided via CLI, nor no default file found in {}'.format(DEFAULT_CONFIGURATION_PATH)
        )
        sys.exit(1)

    actual_config_file = DEFAULT_CONFIGURATION_PATH if config_file is None else config_file
    logging.debug('Loading configuration from {}'.format(actual_config_file))
    with actual_config_file.open() as f:
        config.read_file(f)

    # Override config file settings with command line options
    for config_section in config.keys():
        # Default section is not used in medusa.ini
        if config_section == 'DEFAULT':
            continue
        settings = CONFIG_SECTIONS[config_section]._fields
        config.read_dict({config_section: {
            key: value
            for key, value in _zip_fields_with_arg_values(settings, args)
            if value is not None
        }})

    if evaluate_boolean(config['kubernetes']['enabled']):
        if evaluate_boolean(config['cassandra']['use_sudo']):
            logging.warning('Forcing use_sudo to False because Kubernetes mode is enabled')
        config['cassandra']['use_sudo'] = 'False'

    resolve_ip_addresses = evaluate_boolean(config['cassandra']['resolve_ip_addresses'])
    config.set('cassandra', 'resolve_ip_addresses', 'True' if resolve_ip_addresses else 'False')
    if config['storage']['fqdn'] == socket.getfqdn() and not resolve_ip_addresses:
        # Use the ip address instead of the fqdn when DNS resolving is turned off
        config['storage']['fqdn'] = socket.gethostbyname(socket.getfqdn())

    if "CQL_USERNAME" in os.environ:
        config['cassandra']['cql_username'] = os.environ["CQL_USERNAME"]
    if "CQL_PASSWORD" in os.environ:
        config['cassandra']['cql_password'] = os.environ["CQL_PASSWORD"]

    return config


def load_config(args, config_file):
    """Load configuration from a medusa.ini file

    :param dict args: settings override. Higher priority than settings defined in medusa.ini
    :param pathlib.Path config_file: path to a medusa.ini file or None if default path should be used
    :return MedusaConfig: Medusa configuration
    """
    config = parse_config(args, config_file)

    medusa_config = MedusaConfig(
        file_path=config_file,
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
