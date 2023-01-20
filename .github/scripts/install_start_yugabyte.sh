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

export ARCHITECTURE=`uname -m`

if [[ $ARCHITECTURE != "x86_64" && $ARCHITECTURE != "aarch64" ]]; then
  echo "Arch $ARCHITECTURE not yet supported"
  exit 1
fi

show_help() {
cat <<-EOT
Usage: ${0##*/} <YB_VERSION>
EOT
}

YB_VERSION=$1

if [[ "${YB_VERSION}x" == "x" ]]; then
  show_help
  exit 1
fi

YUGABYTE_SRC=${2:-/home/yugabyte}

YB_BUILD_NUMBER=$(curl -sk http://release.dev.yugabyte.com/releases/latest?version=${YB_VERSION})
YB_VERSION_BUILD="${YB_VERSION}-b${YB_BUILD_NUMBER}"

S3_URL="s3://releases.yugabyte.com/${YB_VERSION_BUILD}/yugabyte-${YB_VERSION_BUILD}-centos-${ARCHITECTURE}.tar.gz"
file="yugabyte-${YB_VERSION_BUILD}-centos-${ARCHITECTURE}.tar.gz"

rm -rf $YUGABYTE_SRC
mkdir -p $YUGABYTE_SRC
rm -rf "yugabyte-${YB_VERSION_BUILD}-centos-${ARCHITECTURE}.tar.gz"
aws s3 cp $S3_URL $file
tar -xf $file -C $YUGABYTE_SRC
rm -rf $file
$YUGABYTE_SRC/yugabyte-$YB_VERSION/bin/post_install.sh >/dev/null 2>&1
mkdir -p $YUGABYTE_SRC/build/latest
ln -s $YUGABYTE_SRC/yugabyte-$YB_VERSION/bin $YUGABYTE_SRC/bin
ln -s $YUGABYTE_SRC/yugabyte-$YB_VERSION/bin $YUGABYTE_SRC/build/latest/bin
ln -s $YUGABYTE_SRC/yugabyte-$YB_VERSION/postgres $YUGABYTE_SRC/build/latest/postgres

if [[ ! -f "/usr/bin/python" ]]; then
    ln -s /usr/bin/python3 /usr/bin/python
fi
export PATH=$YUGABYTE_SRC/yugabyte-$YB_VERSION/bin:/usr/local/bin:$PATH

if [[  `yugabyted status | grep 'not running\|Stopped'` ]] ; then
  yugabyted start --advertise_address $(hostname -i)
fi
