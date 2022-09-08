#!/bin/bash
build_dir_in_container=/opt/cdcsdk-server
docker run -t \
  --cap-add=SYS_PTRACE \
  -e DOCKER_IMAGE \
  -e YB_VERSION_TO_TEST_AGAINST \
  "-w=$build_dir_in_container" \
  --privileged \
  --mount type=bind,source="$PWD",target="$build_dir_in_container" \
  "$DOCKER_IMAGE" \
  bash -c '
    set -euo pipefail -x
    export YUGABYTE_SRC=/home/yugabyte
    export PATH=/usr/local/bin:$PATH
    ./install_yugabyte.sh ${YB_VERSION_TO_TEST_AGAINST} ${YUGABYTE_SRC}
    sudo $YUGABYTE_SRC/yugabyte-$YB_VERSION/bin/yugabyted start --advertise_address $(hostname -i)