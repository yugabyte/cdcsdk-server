#!/bin/bash
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