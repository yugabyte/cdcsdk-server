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

. /etc/os-release

export ARCHITECTURE=`uname -m`
export MOS=`uname|tr '[:upper:]' '[:lower:]'`

if [[ $ARCHITECTURE != "x86_64" && $ARCHITECTURE != "aarch64" ]]; then
  echo "Arch $ARCHITECTURE not yet supported"
  exit 1
fi

if [[ "${ID_LIKE:-}" == *rhel* ]]; then
  yum -y -q install java-11-openjdk-devel
  alternatives --set java java-11-openjdk.x86_64
else
  echo "OS not supported"
  exit 1
fi

if [[ $ARCHITECTURE == "aarch64" ]]; then
  # For aarch64 yugabyte build we have el8 in build name
  # e.g: yugabyte-2.15.1.0-b175-el8-aarch64.tar.gz & yugabyte-2.15.1.0-b175-linux-x86_64.tar.gz
  export MOS="el8"
fi

show_help() {
cat <<-EOT
Usage: ${0##*/} <YB_VERSION_BUILD>
EOT
}

YB_VERSION_BUILD=$1

if [[ "${YB_VERSION_BUILD}x" == "x" ]]; then
  show_help
  exit 1
fi

YUGABYTE_SRC=${2:-/home/yugabyte}

YB_VERSION=`echo $YB_VERSION_BUILD| awk -F'-' '{print $1}'`
URL="https://downloads.yugabyte.com/releases/$YB_VERSION/yugabyte-$YB_VERSION_BUILD-$MOS-$ARCHITECTURE.tar.gz"

rm -rf $YUGABYTE_SRC
mkdir -p $YUGABYTE_SRC
rm -rf "yugabyte-$YB_VERSION_BUILD-$MOS-$ARCHITECTURE.tar.gz"
wget -q $URL
file=yugabyte-$YB_VERSION_BUILD-$MOS-$ARCHITECTURE.tar.gz
tar -xf $file -C $YUGABYTE_SRC
rm -rf "yugabyte-$YB_VERSION_BUILD-$MOS-$ARCHITECTURE.tar.gz"
$YUGABYTE_SRC/yugabyte-$YB_VERSION/bin/post_install.sh >/dev/null 2>&1
mkdir -p $YUGABYTE_SRC/build/latest
ln -s $YUGABYTE_SRC/yugabyte-$YB_VERSION/bin $YUGABYTE_SRC/bin
ln -s $YUGABYTE_SRC/yugabyte-$YB_VERSION/bin $YUGABYTE_SRC/build/latest/bin
ln -s $YUGABYTE_SRC/yugabyte-$YB_VERSION/postgres $YUGABYTE_SRC/build/latest/postgres

if [[ ! -f "/usr/bin/python" ]]; then
    ln -s /usr/bin/python3 /usr/bin/python
fi
export PATH=$YUGABYTE_SRC/yugabyte-$YB_VERSION/bin:/usr/local/bin:$PATH

if [[  `yugabyted status | grep 'not running'` ]] ; then
  yugabyted start --advertise_address $(hostname -i)
fi
