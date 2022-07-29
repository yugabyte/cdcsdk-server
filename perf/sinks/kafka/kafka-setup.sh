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
    "name": "jdbc-sink-${TOPIC_PREFIX}-${FQDN[0]}-${FQDN[1]}",
    "config": {
        "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",    
        "tasks.max": "3",
        "topics": "${TOPIC_PREFIX}.${table}",
        "dialect.name": "PostgreSqlDatabaseDialect",
        "table.name.format": "${FQDN[1]}",    
        "connection.url": "jdbc:postgresql://pg:5432/postgres?user=postgres&password=postgres&sslMode=require",    
        "transforms": "unwrap",    
        "transforms.unwrap.type": "io.debezium.connector.yugabytedb.transforms.YBExtractNewRecordState",   
        "transforms.unwrap.drop.tombstones": "false",
        "auto.create": "true",   
        "insert.mode": "upsert",    
        "pk.fields": "id",    
        "pk.mode": "record_key",   
        "delete.enabled": "true",
        "auto.evolve":"true"
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
