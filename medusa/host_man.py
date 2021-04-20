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
    def get_release_version(host):
        if not host or not host.host_id or not host.release_version:
            return None

        if host.host_id not in HostMan.__host_releases:
            HostMan.__host_releases[host.host_id] = host.release_version

        return HostMan.__host_releases[host.host_id]
