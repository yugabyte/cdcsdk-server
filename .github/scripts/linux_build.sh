#!/usr/bin/env bash
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

set -eo pipefail
build_dir_in_container=/opt/cdcsdk-server
docker run -t \
  -e DOCKER_IMAGE \
  -e YB_VERSION_TO_TEST_AGAINST \
  -e PKG_VERSION \
  -e JENKINS_AGENT_IP \
  "-w=$build_dir_in_container" \
  --privileged \
  --mount type=bind,source="$PWD",target="$build_dir_in_container" \
  -v /var/run/docker.sock:/var/run/docker.sock \
  "$DOCKER_IMAGE" \
  bash -c '
    set -ex
    YUGABYTE_SRC=/home/yugabyte
    ./.github/scripts/install_start_yugabyte.sh ${YB_VERSION_TO_TEST_AGAINST} ${YUGABYTE_SRC}
    ./.github/scripts/build_test_cdcsdk.sh ${PKG_VERSION}
  '
