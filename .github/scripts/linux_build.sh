#!/bin/bash
#
# Copyright (c) YugaByte, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
# in compliance with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License
# is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
# or implied.  See the License for the specific language governing permissions and limitations
# under the License.

build_dir_in_container=/opt/cdcsdk-server
docker run -t \
  --cap-add=SYS_PTRACE \
  -e DOCKER_IMAGE \
  -e YB_VERSION_TO_TEST_AGAINST \
  -e PKG_VERSION \
  "-w=$build_dir_in_container" \
  --privileged \
  --mount type=bind,source="$PWD",target="$build_dir_in_container" \
  -v /var/run/docker.sock:/var/run/docker.sock \
  "$DOCKER_IMAGE" \
  bash -c '
    set -euo pipefail -x
    export YUGABYTE_SRC=/home/yugabyte
    YB_VERSION=${YB_VERSION_TO_TEST_AGAINST[0]%-*}
    # Install dependent packages
    yum -y -q install java-11-openjdk-devel
    alternatives --set java java-11-openjdk.x86_64
    ./.github/scripts/install_yugabyte.sh ${YB_VERSION_TO_TEST_AGAINST} ${YUGABYTE_SRC}
    if [[ ! -f "/usr/bin/python" ]]; then
        ln -s /usr/bin/python3 /usr/bin/python
    fi
    export PATH=$YUGABYTE_SRC/yugabyte-$YB_VERSION/bin:/usr/local/bin:$PATH
    yugabyted start --advertise_address $(hostname -i)
    # Run tests
    mvn clean integration-test -PreleaseTests 
    SHORT_COMMIT=$(git rev-parse --short HEAD)
    cd cdcsdk-server/cdcsdk-server-dist/target
    mv cdcsdk-server-dist-${PKG_VERSION}.tar.gz cdcsdk-server-dist-${PKG_VERSION}-${SHORT_COMMIT}.tar.gz
    sha1sum cdcsdk-server-dist-${PKG_VERSION}-${SHORT_COMMIT}.tar.gz > cdcsdk-server-dist-${PKG_VERSION}-${SHORT_COMMIT}.tar.gz.sha
    md5sum cdcsdk-server-dist-${PKG_VERSION}-${SHORT_COMMIT}.tar.gz > cdcsdk-server-dist-${PKG_VERSION}-${SHORT_COMMIT}.tar.gz.md5
  '
