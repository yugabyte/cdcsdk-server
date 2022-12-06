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

set -exo pipefail

show_help() {
cat <<-EOT
Usage: ${0##*/} <PKG_VERSION>
EOT
}

PKG_VERSION=$1

if [[ "${PKG_VERSION}x" == "x" ]]; then
    show_help
    exit 1
fi

mvn clean package -DskipTests -DskipITs
SHORT_COMMIT=$(git rev-parse --short HEAD)
cd cdcsdk-server/cdcsdk-server-dist/target
mv cdcsdk-server-dist-${PKG_VERSION}.tar.gz cdcsdk-server-dist-${PKG_VERSION}-${SHORT_COMMIT}.tar.gz
sha1sum cdcsdk-server-dist-${PKG_VERSION}-${SHORT_COMMIT}.tar.gz > cdcsdk-server-dist-${PKG_VERSION}-${SHORT_COMMIT}.tar.gz.sha
md5sum cdcsdk-server-dist-${PKG_VERSION}-${SHORT_COMMIT}.tar.gz > cdcsdk-server-dist-${PKG_VERSION}-${SHORT_COMMIT}.tar.gz.md5
