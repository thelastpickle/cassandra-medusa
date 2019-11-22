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

#!/usr/bin/env bash
set -x
# Keep the following rm for the sake of running the integration tests in CI
rm -Rf .python-version
export LOCAL_JMX=yes
export PYTHONWARNINGS="ignore"
pip3 install -r requirements.txt
pip3 install -r requirements-test.txt
cd tests/integration
if [ -z "$1" ]
then
    PYTHONPATH=../.. behave --stop --no-capture-stderr --no-capture
else
    PYTHONPATH=../.. PYTHONPATH=../.. behave --tags=$1 --stop --no-capture-stderr --no-capture
fi
