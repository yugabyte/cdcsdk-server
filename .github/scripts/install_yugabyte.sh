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

set -eo pipefail
export YBC_HOME_DIR=`dirname $(which $0)`
source $YBC_HOME_DIR/common.sh

YB_VERSION_BUILD=$1
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