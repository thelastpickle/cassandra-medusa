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

It is tested on a fresh installation of debian 13 and ubuntu 24.04 for medusa 0.26.

For exact tool versions used in CI see:
https://github.com/thelastpickle/cassandra-medusa/blob/master/.github/workflows/ci.yml

## Installation for Debian/Ubuntu

This setup guide is tested on Debian/Ubuntu systems. For other Linux distributions or macOS, you may need to adjust package manager commands accordingly.

## System Dependencies

**System packages (needed to build Python and native wheels):**

```bash
sudo apt update
sudo apt install -y build-essential libssl-dev \
    libxml2-dev  libffi-dev  libxslt-dev zlib1g-dev \
    xz-utils git curl

# Optional lib for installing python with pyenv
sudo apt install -y libncursesw5-dev libbz2-dev \
    libreadline-dev libsqlite3-dev liblzma-dev
```

**File descriptor limits (temporary + persistent):**

```bash
ulimit -n 8192
echo "$USER soft nofile 4096" | sudo tee -a /etc/security/limits.conf
echo "$USER hard nofile 8192" | sudo tee -a /etc/security/limits.conf
```


## System installation

**Medusa requires Python 3.9.2 or higher (up to Python 3.12):**

```bash
curl https://pyenv.run | bash
# add pyenv init lines to ~/.bashrc (the installer prints them)
PATH="$HOME/.pyenv/bin/:$PATH"
cat << 'EOF' >> ~/.bashrc

PATH="$HOME/.pyenv/bin/:$PATH"

export PYENV_ROOT="$HOME/.pyenv"
[[ -d $PYENV_ROOT/bin ]] && export PATH="$PYENV_ROOT/bin:$PATH"
eval "$(pyenv init - bash)"
# Load pyenv-virtualenv automatically
eval "$(pyenv virtualenv-init -)"

EOF
source ~/.bashrc
```

**Install Poetry 2.2.1:**

Poetry is used for dependency management and virtual environment handling:

```bash
curl -sSL https://install.python-poetry.org | python3 - --version 2.2.1
export PATH="$HOME/.local/bin:$PATH"
echo "" >> "$HOME/.bashrc"
echo 'PATH="$HOME/.local/bin/:$PATH"' >> "$HOME/.bashrc"

poetry --version
```

**Java setup:**

Java is required for Cassandra and CCM. We'll install OpenJDK 8 and 11 since different Cassandra versions have different Java requirements.


```bash
sudo apt install -y zip unzip

curl -s "https://get.sdkman.io" | bash
source "/home/ubuntu/.sdkman/bin/sdkman-init.sh"

sdk install java 11.0.28-tem
sdk use java 11.0.28-tem
sdk default java 11.0.28-tem
```

Check if all is good with:

```bash
java --version
echo $JAVA_HOME
```

## Setup python env

Create the python env for cassandra-medusa

```bash
pyenv install 3.10.19
pyenv virtualenv 3.10.19 venv-medusa
```

Need poetry project file in cassandra-medusa repo:

```bash
git clone https://github.com/thelastpickle/cassandra-medusa.git
cd cassandra-medusa
pyenv virtualenv 3.10.19 venv-medusa
echo "venv-medusa" > .python-version
```

Install libs for cassandra-medusa

```bash
# For debian only:
pip install keyring secretstorage
# Install Test helpers
poetry install
poetry run pip install git+https://github.com/riptano/ccm.git
cd .
ccm
```

If `ccm` application is not working, restart your shell and go in cassandra-medusa folder.  

## Test installation

```bash
# Check Python syntax errors or undefined names
poetry run flake8 . --count --select=E9,F63,F7,F82 --show-source --statistics
# An other check
poetry run flake8 . --count --exit-zero --max-complexity=10 --statistics --ignore=W503
# Run unit test
poetry run tox

# run integration tests (local/minio/gcs/s3 etc.)
./run_integration_tests.sh --cassandra-version=4.1.9
```
