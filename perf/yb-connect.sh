#!/bin/bash
set -xe
FILE=$1

if test -f "$FILE"; then
    export $(grep -v '^#' $FILE | xargs)
fi

export PATH=$PATH:$YBPATH

payload() {
    cat << EOF
{
    "name": "yb-connector-${TOPIC_PREFIX}-${FQDN[0]}-${FQDN[1]}",
    "config": {
        "connector.class":"io.debezium.connector.yugabytedb.YugabyteDBConnector",    
        "tasks.max": "${THREADS}",
        "topics": "${TOPIC_PREFIX}.${table}",
        "database.hostname":"${PGHOST}",
        "database.port":"5433",
        "database.master.addresses": "${PGHOST}:7100",
        "database.user": "yugabyte",
        "database.password": "yugabyte",
        "database.dbname" : "yugabyte",
        "database.server.name": "${TOPIC_PREFIX}",
        "table.include.list":"${TABLES}",
        "database.streamid":"${CDC_SDK_STREAM_ID}",
        "key.converter.schemas.enable":"false",
        "value.converter.schemas.enable":"false",
        "transforms":"unwrap",
        "transforms.unwrap.type":"io.debezium.connector.yugabytedb.transforms.YBExtractNewRecordState",
        "snapshot.mode":"initial"
    }
}
EOF
}

IFS=',' read -ra TABLE_ARRAY <<< "$TABLES"
for table in "${TABLE_ARRAY[@]}"; do
    echo $table
    IFS='.' read -ra FQDN <<< "$table"
    curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" localhost:8083/connectors/ -d "$(payload)"


done
