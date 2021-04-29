# -*- coding: utf-8 -*-
# Copyright 2021- Datastax, Inc. All rights reserved.
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
from cassandra.util import Version


class HostMan:
    DEFAULT_RELEASE_VERSION = "3.11.9"
    __instance = None

    @staticmethod
    def set_release_version(version):
        if not version:
            raise RuntimeError('No version supplied.')

        if not HostMan.__instance:
            HostMan()

        HostMan.__instance.__release_version = Version(version)

    @staticmethod
    def get_release_version():
        if not HostMan.__instance:
            raise RuntimeError('Release version must be set before getting')
        return HostMan.__instance.__release_version

    def __init__(self):
        if HostMan.__instance:
            raise RuntimeError('Unable to re-init HostMan')
        HostMan.__instance = self

    # Reset the instance
    @staticmethod
    def reset():
        HostMan.__instance = None
