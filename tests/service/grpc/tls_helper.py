# -*- coding: utf-8 -*-
# Copyright 2020- Datastax, Inc. All rights reserved.
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

"""Generates a throwaway CA, a server certificate for 127.0.0.1 and a client certificate for TLS tests."""

import collections
import datetime
import ipaddress
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

TlsFiles = collections.namedtuple('TlsFiles', ['ca', 'server_cert', 'server_key', 'client_cert', 'client_key'])


def _name(common_name):
    return x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)])


def _write_key(key, path):
    path.write_bytes(key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                                       serialization.NoEncryption()))


def _write_cert(cert, path):
    path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))


def _issue(common_name, issuer_name, issuer_key, usage, san=None):
    key = ec.generate_private_key(ec.SECP256R1())
    now = datetime.datetime.now(datetime.timezone.utc)
    builder = (x509.CertificateBuilder()
               .subject_name(_name(common_name)).issuer_name(issuer_name)
               .public_key(key.public_key()).serial_number(x509.random_serial_number())
               .not_valid_before(now - datetime.timedelta(minutes=5))
               .not_valid_after(now + datetime.timedelta(days=1))
               .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
               .add_extension(x509.ExtendedKeyUsage([usage]), critical=False))
    if san is not None:
        builder = builder.add_extension(x509.SubjectAlternativeName(san), critical=False)
    return key, builder.sign(issuer_key, hashes.SHA256())


def make_tls_files(directory):
    directory = Path(directory)
    now = datetime.datetime.now(datetime.timezone.utc)
    ca_key = ec.generate_private_key(ec.SECP256R1())
    ca_name = _name('medusa-test-ca')
    ca_cert = (x509.CertificateBuilder()
               .subject_name(ca_name).issuer_name(ca_name)
               .public_key(ca_key.public_key()).serial_number(x509.random_serial_number())
               .not_valid_before(now - datetime.timedelta(minutes=5))
               .not_valid_after(now + datetime.timedelta(days=1))
               .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
               .add_extension(x509.KeyUsage(digital_signature=True, key_cert_sign=True, crl_sign=True,
                                            content_commitment=False, key_encipherment=False,
                                            data_encipherment=False, key_agreement=False, encipher_only=False,
                                            decipher_only=False), critical=True)
               .add_extension(x509.SubjectKeyIdentifier.from_public_key(ca_key.public_key()), critical=False)
               .sign(ca_key, hashes.SHA256()))
    server_key, server_cert = _issue('127.0.0.1', ca_name, ca_key, ExtendedKeyUsageOID.SERVER_AUTH,
                                     san=[x509.IPAddress(ipaddress.ip_address('127.0.0.1'))])
    client_key, client_cert = _issue('scraper', ca_name, ca_key, ExtendedKeyUsageOID.CLIENT_AUTH)

    files = TlsFiles(*(directory / n for n in ('ca.pem', 'server.crt', 'server.key', 'client.crt', 'client.key')))
    _write_cert(ca_cert, files.ca)
    _write_cert(server_cert, files.server_cert)
    _write_key(server_key, files.server_key)
    _write_cert(client_cert, files.client_cert)
    _write_key(client_key, files.client_key)
    return files
