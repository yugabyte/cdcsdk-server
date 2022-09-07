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
#

export ARCHITECTURE=`uname -m`
export MOS=`uname|tr '[:upper:]' '[:lower:]'`

detect_os() {
  is_linux=false
  case $MOS in
    darwin) export BUILD_PLATFORM="macos" ;;
    linux) is_linux=true ;;
    *)
      echo "Unknown operating system: $MOS"
      exit 1
  esac

  if "$is_linux"; then
    if [[ -f /etc/issue ]] && grep Ubuntu /etc/issue >/dev/null; then
      BUILD_PLATFORM="ubuntu"
    elif [[ -f /etc/redhat-release ]] && grep CentOS /etc/redhat-release > /dev/null; then
      BUILD_PLATFORM="centos"
    elif [[ -f /etc/redhat-release ]] && grep AlmaLinux /etc/redhat-release > /dev/null; then
      BUILD_PLATFORM="almalinux"
    fi
  fi
}

detect_os

if [[ $ARCHITECTURE != "x86_64" && $ARCHITECTURE != "aarch64" ]]; then
  echo "Arch $ARCHITECTURE not yet supported"
  exit 1
fi

if [[ $ARCHITECTURE == "aarch64" ]]; then
  export MOS="el8"
fi
