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


class HostMan:
    __host_releases = dict()

    @staticmethod
    def set_release_version(host, release_version):

        if host and release_version and host not in HostMan.__host_releases:
            HostMan.__host_releases[host] = release_version

    @staticmethod
    def get_release_version(host):

        if not host:
            return None

        if host in HostMan.__host_releases:
            return HostMan.__host_releases[host]

        return None
