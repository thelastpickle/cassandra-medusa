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
from medusa.network.hostname_resolver import HostnameResolver

StorageConfig = collections.namedtuple(
    'StorageConfig',
    ['bucket_name', 'key_file', 'prefix', 'fqdn', 'host_file_separator', 'storage_provider', 'storage_class',
     'base_path', 'max_backup_age', 'max_backup_count', 'api_profile', 'transfer_max_bandwidth',
     'concurrent_transfers', 'multi_part_upload_threshold', 'multipart_chunksize', 'host', 'region', 'port', 'secure',
     'ssl_verify', 'aws_cli_path', 'kms_id', 'sse_c_key', 'backup_grace_period_in_days', 'use_sudo_for_restore',
     'k8s_mode', 'read_timeout', 's3_addressing_style']
)

CassandraConfig = collections.namedtuple(
    'CassandraConfig',
    ['start_cmd', 'stop_cmd', 'config_file', 'cql_username', 'cql_password', 'check_running', 'is_ccm',
     'sstableloader_bin', 'nodetool_username', 'nodetool_password', 'nodetool_password_file_path', 'nodetool_host',
     'nodetool_executable', 'nodetool_port', 'certfile', 'usercert', 'userkey', 'sstableloader_ts',
     'sstableloader_tspw', 'sstableloader_ks', 'sstableloader_kspw', 'nodetool_ssl', 'resolve_ip_addresses', 'use_sudo',
     'nodetool_flags', 'cql_k8s_secrets_path', 'nodetool_k8s_secrets_path']
)

SSHConfig = collections.namedtuple(
    'SSHConfig',
    ['username', 'key_file', 'port', 'cert_file', 'use_pty', 'keepalive_seconds', 'login_shell']
)

ChecksConfig = collections.namedtuple(
    'ChecksConfig',
    ['health_check', 'query', 'expected_rows', 'expected_result', 'enable_md5_checks']
)

MonitoringConfig = collections.namedtuple(
    'MonitoringConfig',
    ['monitoring_provider', 'send_backup_name_tag']
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
    ['enabled', 'max_send_message_length', 'max_receive_message_length', 'port']
)

KubernetesConfig = collections.namedtuple(
    'KubernetesConfig',
    ['enabled', 'cassandra_url', 'use_mgmt_api', 'ca_cert', 'tls_cert', 'tls_key']
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
DEFAULT_GRPC_PORT = 50051


def _build_default_config():
    """Build a INI config parser with default values

    :return ConfigParser: default configuration
    """
    config = configparser.ConfigParser(interpolation=None)

    config['storage'] = {
        'host_file_separator': ',',
        'max_backup_age': '0',
        'max_backup_count': '0',
        'api_profile': '',
        'transfer_max_bandwidth': '50MB/s',
        'concurrent_transfers': '1',
        'multi_part_upload_threshold': str(20 * 1024 * 1024),
        'secure': 'True',
        'ssl_verify': 'False',      # False until we work out how to specify custom certs
        'aws_cli_path': 'aws',
        'fqdn': socket.getfqdn(),
        'region': 'default',
        'backup_grace_period_in_days': 10,
        'use_sudo_for_restore': 'True',
        'multipart_chunksize': '50MB',
        's3_addressing_style': 'auto',
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
        'nodetool_executable': 'nodetool',
        'nodetool_flags': '-Dcom.sun.jndi.rmiURLParsing=legacy'
    }

    config['ssh'] = {
        'username': os.environ.get('USER') or '',
        'key_file': '',
        'port': '22',
        'cert_file': '',
        'use_pty': 'False',
        'keepalive_seconds': '60',
        'login_shell': 'False'
    }

    config['checks'] = {
        'health_check': 'cql',
        'query': '',
        'expected_rows': '0',
        'expected_result': '',
        'enable_md5_checks': 'false'
    }

    config['monitoring'] = {
        'monitoring_provider': 'None',
        'send_backup_name_tag': 'False'
    }

    config['grpc'] = {
        'enabled': 'False',
        'max_send_message_length': '536870912',
        'max_receive_message_length': '134217728',
        'port': f'{DEFAULT_GRPC_PORT}'
    }

    config['kubernetes'] = {
        'enabled': 'False',
        'cassandra_url': 'None',
        'use_mgmt_api': 'False',
        'ca_cert': '',
        'tls_cert': '',
        'tls_key': ''
    }
    return config


def parse_config(args, config_file):
    """Parse a medusa.ini file and allow to override settings from command line

    :param dict args: settings override. Higher priority than settings defined in medusa.ini
    :param pathlib.Path config_file: path to medusa.ini file
    :return: None
    """
    config = _build_default_config()

    _load_config_file(config, config_file)
    _override_config_with_args(config, args)
    _handle_k8s_and_grpc_settings(config, args)
    _handle_env_vars(config)
    _handle_k8s_secrets(config)
    _handle_hostname_resolution(config)

    return config


def _load_config_file(config, config_file):
    """Load configuration from file, handling default path if needed."""
    if config_file is None and not DEFAULT_CONFIGURATION_PATH.exists():
        logging.error(
            'No configuration file provided via CLI, nor no default file found in {}'.format(DEFAULT_CONFIGURATION_PATH)
        )
        sys.exit(1)

    actual_config_file = DEFAULT_CONFIGURATION_PATH if config_file is None else config_file
    logging.debug('Loading configuration from {}'.format(actual_config_file))
    with actual_config_file.open() as f:
        config.read_file(f)


def _override_config_with_args(config, args):
    """Override config with command line arguments."""
    for config_section in config.keys():
        if config_section == 'DEFAULT':
            continue
        settings = CONFIG_SECTIONS[config_section]._fields
        config.read_dict({config_section: {
            key: value
            for key, value in _zip_fields_with_arg_values(settings, args)
            if value is not None
        }})


def _handle_k8s_and_grpc_settings(config, args):
    """Handle Kubernetes and gRPC specific settings."""
    k8s_enabled = evaluate_boolean(config['kubernetes']['enabled'])
    if args.get('k8s_enabled', 'False') == 'True' or k8s_enabled:
        config.set('kubernetes', 'enabled', 'True')
    grpc_enabled = evaluate_boolean(config['grpc']['enabled'])
    if args.get('grpc_enabled', "False") == 'True' or grpc_enabled:
        config.set('grpc', 'enabled', 'True')

    if evaluate_boolean(config['kubernetes']['enabled']):
        if evaluate_boolean(config['cassandra']['use_sudo']):
            logging.warning('Forcing use_sudo to False because Kubernetes mode is enabled')
        config['cassandra']['use_sudo'] = 'False'
        config['storage']['use_sudo_for_restore'] = 'False'
        if "POD_IP" in os.environ:
            config['storage']['fqdn'] = os.environ["POD_IP"]


def _handle_env_vars(config):
    """Handle environment variable overrides."""
    for config_property in ['cql_username', 'cql_password']:
        config_property_upper_old = config_property.upper()
        config_property_upper_new = "MEDUSA_{}".format(config_property.upper())
        if config_property_upper_old in os.environ:
            config['cassandra'][config_property] = os.environ[config_property_upper_old]
            logging.warning('The {} environment variable is deprecated and has been replaced by the {} variable'
                            .format(config_property_upper_old, config_property_upper_new))
        if config_property_upper_new in os.environ:
            config['cassandra'][config_property] = os.environ[config_property_upper_new]

    for config_property in [
        'nodetool_username',
        'nodetool_password',
        'sstableloader_tspw',
        'sstableloader_kspw',
        'resolve_ip_addresses',
        'cql_k8s_secrets_path',
        'nodetool_k8s_secrets_path'
    ]:
        config_property_upper = "MEDUSA_{}".format(config_property.upper())
        if config_property_upper in os.environ:
            config.set('cassandra', config_property, os.environ[config_property_upper])


def _handle_k8s_secrets(config):
    """Handle Kubernetes secrets configuration."""
    if config.has_option('cassandra', 'cql_k8s_secrets_path'):
        cql_k8s_secrets_path = config.get('cassandra', 'cql_k8s_secrets_path')
        if cql_k8s_secrets_path:
            logging.debug('Using cql_k8s_secrets_path (path="{}")'.format(cql_k8s_secrets_path))
            cql_k8s_username, cql_k8s_password = _load_k8s_secrets(cql_k8s_secrets_path)
            config.set('cassandra', 'cql_username', cql_k8s_username)
            config.set('cassandra', 'cql_password', cql_k8s_password)

    if config.has_option('cassandra', 'nodetool_k8s_secrets_path'):
        nodetool_k8s_secrets_path = config.get('cassandra', 'nodetool_k8s_secrets_path')
        if nodetool_k8s_secrets_path:
            logging.debug('Using nodetool_k8s_secrets_path (path="{}")'.format(nodetool_k8s_secrets_path))
            nodetool_k8s_username, nodetool_k8s_password = _load_k8s_secrets(nodetool_k8s_secrets_path)
            config.set('cassandra', 'nodetool_username', nodetool_k8s_username)
            config.set('cassandra', 'nodetool_password', nodetool_k8s_password)


def _handle_hostname_resolution(config):
    """Handle hostname resolution settings."""
    resolve_ip_addresses = config['cassandra']['resolve_ip_addresses']
    kubernetes_enabled = evaluate_boolean(config['kubernetes']['enabled'])
    hostname_resolver = HostnameResolver(resolve_ip_addresses, kubernetes_enabled)

    if config['storage']['fqdn'] == socket.getfqdn() and not resolve_ip_addresses:
        config['storage']['fqdn'] = socket.gethostbyname(socket.getfqdn())
    elif config['storage']['fqdn'] == socket.getfqdn():
        config.set('storage', 'fqdn', hostname_resolver.resolve_fqdn())

    config.set('storage', 'k8s_mode', str(kubernetes_enabled))
    config.set('cassandra', 'resolve_ip_addresses', 'True'
               if evaluate_boolean(config['cassandra']['resolve_ip_addresses']) else 'False')


def _load_k8s_secrets(k8s_secrets_path):
    """Load username and password from files following the k8s secrets convention.

    :param str k8s_secrets_path: folder path containing the secrets
    :return str, str: username and password contained in files
    """
    # By default, username and password are available in path/username and path/password.
    # They could be in other places if overridden, this is not supported for now. Refs:
    # https://kubernetes.io/docs/concepts/configuration/secret/#using-secrets-as-files-from-a-pod
    # https://kubernetes.io/docs/concepts/configuration/secret/#consuming-secret-values-from-volumes
    k8s_username_file = os.path.join(k8s_secrets_path, 'username')
    logging.debug('Loading k8s username from "{}"'.format(k8s_username_file))
    with open(k8s_username_file, 'r') as f:
        k8s_username = f.read().strip()
    k8s_password_file = os.path.join(k8s_secrets_path, 'password')
    logging.debug('Loading k8s password from "{}"'.format(k8s_password_file))
    with open(k8s_password_file, 'r') as f:
        k8s_password = f.read().strip()
    return k8s_username, k8s_password


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

    for field in ['bucket_name', 'prefix']:
        value = getattr(medusa_config.storage, field)
        if value is not None and '/' in value:
            logging.error('Required configuration "{}" cannot contain a slash ("/")'.format(field))
            sys.exit(2)

    return medusa_config


def _zip_fields_with_arg_values(fields, args):
    return [(field, args[field] if (field in args) else None) for field in fields]


def _namedtuple_from_dict(cls, data):
    return cls(**{
        field: data.get(field)
        for field in cls._fields
    })
