<!--
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
-->

# Developer Setup

This document describes how to setup Medusa straight from Github.

## Requirements

### Ubuntu

For debian users, install the required Debian packages:

```
sudo apt-get install -y debhelper python3 dh-virtualenv libffi-dev libssl-dev python3-dev libxml2-dev libxslt-dev build-essential python3-pip
```

There is a problem with setuptools. The required package has lower apt priority. Needs manual intervention.

```
# manually, aptitude, say no until it proposes installing the right version
sudo aptitude install -y python3-setuptools=20.7.0-1
```

**Alternatve:** we had good result with just running this instead of the previous command.

```
sudo pip3 install setuptools --upgrade
```

### Other OSes

Install Python 3.6+, pip3 and virtualenv through your favorite method.

## Create a virtualenv for Medusa

Run the following command to create the Python virtual environment in the current directory:

```
virtualenv medusa
```

Activate the virtual environment:

```
source medusa/bin/activate
```


Then, install the python packages. It will take some time to build the Cassandra driver:

```
sudo pip3 install -r requirements.txt
sudo pip3 install -r requirements-test.txt
```

Finally, install Medusa from source:

````
sudo pip3 install git+https://you@github.com/spotify/medusa.git@branch --upgrade
```
