# CDCSDK Documentation

- [CDCSDK Documentation](#cdcsdk-documentation)
  - [Installation](#installation)
    - [Unpack and Run Instructions.](#unpack-and-run-instructions)
  - [Configuration](#configuration)
    - [Configuration using Environment Variables](#configuration-using-environment-variables)
  - [Health Checks](#health-checks)
    - [Running the health check](#running-the-health-check)

## Installation

CDCSDK Server distribution archives are available in [Github Releases](https://github.com/yugabyte/cdcsdk-server/releases) of the project.
Each of the releases has a tar.gz labelled as CDCSDK Server.

The archive has the following layout:

```
  cdcsdk-server
  |--CHANGELOG.md
  |-- conf
  |-- CONTRIBUTE.md
  |-- COPYRIGHT.txt
  |-- debezium-server-1.9.2.Final-runner.jar
  |-- lib
  |-- LICENSE-3rd-PARTIES.txt
  |-- LICENSE.txt
  |-- README.md
  |-- run.sh
```


### Unpack and Run Instructions.

    export CDCSDK_VERSION=<x.y.z>
    wget https://github.com/yugabyte/cdcsdk-server/releases/download/v${CDCSDK_VERSION}/cdcsdk-server-dist-${CDCSDK_VERSION}.tar.gz

    # OR Using gh cli

    gh release download v{CDCSDK_VERSION} -A tar.gz --repo yugabyte/cdcsdk-server

    tar xvf cdcsdk-server-dist-${CDCSDK_VERSION}.tar.gz
    cd cdcsdk-server && mkdir data

    # Configure the application. Check next section
    touch conf/application.properties

    # Run the application
    ./run.sh

## Configuration

The main configuration file is conf/application.properties. There are multiple sections configured:
* `debezium.source` is for source connector configuration; each instance of Debezium Server runs exactly one connector
* `debezium.sink` is for the sink system configuration
* `debezium.format` is for the output serialization format configuration
* `debezium.transforms` is for the configuration of message transformations

An example configuration file can look like so:

```
debezium.sink.type=kafka
debezium.sink.kafka.producer.bootstrap.servers=127.0.0.1:9092
debezium.sink.kafka.producer.key.serializer=org.apache.kafka.common.serialization.StringSerializer
debezium.sink.kafka.producer.value.serializer=org.apache.kafka.common.serialization.StringSerializer
debezium.source.connector.class=io.debezium.connector.yugabytedb.YugabyteDBConnector
debezium.source.offset.storage.file.filename=data/offsets.dat
debezium.source.offset.flush.interval.ms=0
debezium.source.database.hostname=127.0.0.1
debezium.source.database.port=5433
debezium.source.database.user=yugabyte
debezium.source.database.password=yugabyte
debezium.source.database.dbname=yugabyte
debezium.source.database.server.name=dbserver1
debezium.source.database.streamid=de362081fa864e94b35fcac6005e7cd9
debezium.source.table.include.list=public.test
debezium.source.database.master.addresses=127.0.0.1:7100
debezium.source.snapshot.mode=never
```

For detailed documentation of the configuration, check [debezium docs](https://debezium.io/documentation/reference/stable/operations/debezium-server.html#_sink_configuration)

### Configuration using Environment Variables

Configuration using environment variables maybe useful when running in containers. The rule of thumb
is to convert the keys to UPPER CASE and replace `.` with `_`. For example, `debezium.source.database.port`
has to be changed to `DEBEZIUM_SOURCE_DATABASE_PORT`

## Health Checks

CDCSDK Server exposes a simple health check REST API. Currently the health check only ensures that the
server is up and running.

### Running the health check

The following REST endpoints are exposed:

* `/q/health/live` - The application is up and running.

* `/q/health/ready` - The application is ready to serve requests.


All of the health REST endpoints return a simple JSON object with two fields:

status — the overall result of all the health check procedures

checks — an array of individual checks

The general status of the health check is computed as a logical AND of all the declared health check procedures.
The checks array is currently empty as we have not specified any health check procedure yet.


Example output:

```
curl http://localhost:8080/q/health/live

{
    "status": "UP",
    "checks": [
        {
            "name": "debezium",
            "status": "UP"
        }
    ]
}

curl http://localhost:8080/q/health/ready

{
    "status": "UP",
    "checks": [
    ]
}
```