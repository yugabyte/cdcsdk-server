#!/bin/bash
build_dir_in_container=/opt/cdcsdk-server
docker run -it \
  --cap-add=SYS_PTRACE \
  -e DOCKER_IMAGE \
  -e YB_VERSION_TO_TEST_AGAINST \
  "-w=$build_dir_in_container" \
  --privileged \
  --mount type=bind,source="$PWD",target="$build_dir_in_container" \
  -v /var/run/docker.sock:/var/run/docker.sock \
  "$DOCKER_IMAGE" \
  bash
  bash -c '
    set -euo pipefail -x
    export YUGABYTE_SRC=/home/yugabyte
    YB_VERSION=`echo ${YB_VERSION_TO_TEST_AGAINST}| awk -F'-' '{print $1}'`
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
    mvn clean integration-test -PreleaseTests -Dit.test=MultiOpsPostgresSinkConsumerIT
    '