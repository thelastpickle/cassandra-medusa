#! /bin/sh

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

if test -f pid; then
    # We can't wait for things that aren't our children. Loop and sleep. :-(
    while ! test -f status; do
        sleep 10s
    done
    exit $(cat status)
fi
if [ -f /etc/default/cassandra-medusa ]; then
    . /etc/default/cassandra-medusa
fi
$@ >stdout 2>stderr &
echo $! >pid
wait $!
STATUS=$?
echo ${STATUS} >status
exit $(cat status)
