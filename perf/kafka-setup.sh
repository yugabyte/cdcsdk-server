set -ex

curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" localhost:8083/connectors/ -d '{
  "name": "jdbc-sink-1",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",    
     "tasks.max": "3",
      "topics": "dbserver1.public.api_apple_store_receipt_log",
      "dialect.name": "PostgreSqlDatabaseDialect",
      "table.name.format": "api_apple_store_receipt_log",    
      "connection.url": "jdbc:postgresql://pg:5432/postgres?user=postgres&password=postgres&sslMode=require",    
      "transforms": "unwrap",    
      "transforms.unwrap.type": "io.debezium.connector.yugabytedb.transforms.YBExtractNewRecordState",   
      "transforms.unwrap.drop.tombstones": "false",
      "auto.create": "true",   
      "insert.mode": "upsert",    
      "pk.fields": "k",    
      "pk.mode": "record_key",   
      "delete.enabled": "true",
      "auto.evolve":"true"
   }
}'

curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" localhost:8083/connectors/ -d '{
  "name": "jdbc-sink-2",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",    
     "tasks.max": "3",
      "topics": "dbserver1.public.api_sub_user_packages",
      "dialect.name": "PostgreSqlDatabaseDialect",
      "table.name.format": "api_sub_user_packages",    
      "connection.url": "jdbc:postgresql://pg:5432/postgres?user=postgres&password=postgres&sslMode=require",    
      "transforms": "unwrap",    
      "transforms.unwrap.type": "io.debezium.connector.yugabytedb.transforms.YBExtractNewRecordState",   
      "transforms.unwrap.drop.tombstones": "false",
      "auto.create": "true",   
      "insert.mode": "upsert",    
      "pk.fields": "k",    
      "pk.mode": "record_key",   
      "delete.enabled": "true",
      "auto.evolve":"true",
      "transforms": "unwrap,ts_createddate,ts_enddate,ts_pspmodifieddate,ts_updateddate,ts_startdate",
      "transforms.ts_createddate.type": "org.apache.kafka.connect.transforms.TimestampConverter$Value",
      "transforms.ts_createddate.target.type": "Timestamp",
      "transforms.ts_createddate.field": "createddate",
      "transforms.ts_createddate.format": "YYYY-MM-dd'\''T'\''HH:mm:ss'\''Z'\''",
      "transforms.ts_enddate.type": "org.apache.kafka.connect.transforms.TimestampConverter$Value",
      "transforms.ts_enddate.target.type": "Timestamp",
      "transforms.ts_enddate.field": "enddate",
      "transforms.ts_enddate.format": "YYYY-MM-dd'\''T'\''HH:mm:ss'\''Z'\''",
      "transforms.ts_pspmodifieddate.type": "org.apache.kafka.connect.transforms.TimestampConverter$Value",
      "transforms.ts_pspmodifieddate.target.type": "Timestamp",
      "transforms.ts_pspmodifieddate.field": "pspmodifieddate",
      "transforms.ts_pspmodifieddate.format": "YYYY-MM-dd'\''T'\''HH:mm:ss'\''Z'\''",
      "transforms.ts_updateddate.type": "org.apache.kafka.connect.transforms.TimestampConverter$Value",
      "transforms.ts_updateddate.target.type": "Timestamp",
      "transforms.ts_updateddate.field": "updateddate",
      "transforms.ts_updateddate.format": "YYYY-MM-dd'\''T'\''HH:mm:ss'\''Z'\''",
      "transforms.ts_startdate.type": "org.apache.kafka.connect.transforms.TimestampConverter$Value",
      "transforms.ts_startdate.target.type": "Timestamp",
      "transforms.ts_startdate.field": "startdate",
      "transforms.ts_startdate.format": "YYYY-MM-dd'\''T'\''HH:mm:ss'\''Z'\''"    
   }
}'

curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" localhost:8083/connectors/ -d '{
  "name": "jdbc-sink-3",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",    
     "tasks.max": "3",
      "topics": "dbserver1.public.api_oauth_access_token",
      "dialect.name": "PostgreSqlDatabaseDialect",
      "table.name.format": "api_oauth_access_token",    
      "connection.url": "jdbc:postgresql://pg:5432/postgres?user=postgres&password=postgres&sslMode=require",    
      "transforms": "unwrap",    
      "transforms.unwrap.type": "io.debezium.connector.yugabytedb.transforms.YBExtractNewRecordState",   
      "transforms.unwrap.drop.tombstones": "false",
      "auto.create": "true",   
      "insert.mode": "upsert",    
      "pk.fields": "k",    
      "pk.mode": "record_key",   
      "delete.enabled": "true",
      "auto.evolve":"true",
      "transforms": "unwrap,ts_expires_in",
      "transforms.ts_expires_in.type": "org.apache.kafka.connect.transforms.TimestampConverter$Value",
      "transforms.ts_expires_in.target.type": "Timestamp",
      "transforms.ts_expires_in.field": "expires_in",
      "transforms.ts_expires_in.format": "YYYY-MM-dd'\''T'\''HH:mm:ss'\''Z'\''"
   }
}'


curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" localhost:8083/connectors/ -d '{
  "name": "jdbc-sink-4",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",    
     "tasks.max": "3",
      "topics": "dbserver1.public.api_sub_apple_orig_transactions",
      "dialect.name": "PostgreSqlDatabaseDialect",
      "table.name.format": "api_sub_apple_orig_transactions",    
      "connection.url": "jdbc:postgresql://pg:5432/postgres?user=postgres&password=postgres&sslMode=require",    
      "transforms": "unwrap",    
      "transforms.unwrap.type": "io.debezium.connector.yugabytedb.transforms.YBExtractNewRecordState",   
      "transforms.unwrap.drop.tombstones": "false",
      "auto.create": "true",   
      "insert.mode": "upsert",    
      "pk.fields": "k",    
      "pk.mode": "record_key",   
      "delete.enabled": "true",
      "auto.evolve":"true",
      "transforms": "unwrap,ts_createddate,ts_lastcheckeddate",
      "transforms.ts_createddate.type": "org.apache.kafka.connect.transforms.TimestampConverter$Value",
      "transforms.ts_createddate.target.type": "Timestamp",
      "transforms.ts_createddate.field": "createddate",
      "transforms.ts_createddate.format": "YYYY-MM-dd'\''T'\''HH:mm:ss'\''Z'\''",
      "transforms.ts_lastcheckeddate.type": "org.apache.kafka.connect.transforms.TimestampConverter$Value",
      "transforms.ts_lastcheckeddate.target.type": "Timestamp",
      "transforms.ts_lastcheckeddate.field": "lastcheckeddate",
      "transforms.ts_lastcheckeddate.format": "YYYY-MM-dd'\''T'\''HH:mm:ss'\''Z'\''"
   }
}'

curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" localhost:8083/connectors/ -d '{
  "name": "jdbc-sink-5",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",    
     "tasks.max": "3",
      "topics": "dbserver1.public.api_sub_recurly_notify_log",
      "dialect.name": "PostgreSqlDatabaseDialect",
      "table.name.format": "api_sub_recurly_notify_log",    
      "connection.url": "jdbc:postgresql://pg:5432/postgres?user=postgres&password=postgres&sslMode=require",    
      "transforms": "unwrap",    
      "transforms.unwrap.type": "io.debezium.connector.yugabytedb.transforms.YBExtractNewRecordState",   
      "transforms.unwrap.drop.tombstones": "false",
      "auto.create": "true",   
      "insert.mode": "upsert",    
      "pk.fields": "k",    
      "pk.mode": "record_key",   
      "delete.enabled": "true",
      "auto.evolve":"true",
      "transforms": "unwrap,ts_ingestdate",
      "transforms.ts_ingestdate.type": "org.apache.kafka.connect.transforms.TimestampConverter$Value",
      "transforms.ts_ingestdate.target.type": "Timestamp",
      "transforms.ts_ingestdate.field": "ingestdate",
      "transforms.ts_ingestdate.format": "YYYY-MM-dd'\''T'\''HH:mm:ss'\''Z'\''"
   }
}'


curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" localhost:8083/connectors/ -d '{
  "name": "jdbc-sink-6",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",    
     "tasks.max": "3",
      "topics": "dbserver1.public.api_user_device",
      "dialect.name": "PostgreSqlDatabaseDialect",
      "table.name.format": "api_user_device",    
      "connection.url": "jdbc:postgresql://pg:5432/postgres?user=postgres&password=postgres&sslMode=require",    
      "transforms": "unwrap",    
      "transforms.unwrap.type": "io.debezium.connector.yugabytedb.transforms.YBExtractNewRecordState",   
      "transforms.unwrap.drop.tombstones": "false",
      "auto.create": "true",   
      "insert.mode": "upsert",    
      "pk.fields": "k",    
      "pk.mode": "record_key",   
      "delete.enabled": "true",
      "auto.evolve":"true",
      "transforms": "unwrap,ts_createddate,ts_lastupdate",
      "transforms.ts_createddate.type": "org.apache.kafka.connect.transforms.TimestampConverter$Value",
      "transforms.ts_createddate.target.type": "Timestamp",
      "transforms.ts_createddate.field": "createddate",
      "transforms.ts_createddate.format": "YYYY-MM-dd'\''T'\''HH:mm:ss'\''Z'\''",
      "transforms.ts_lastupdate.type": "org.apache.kafka.connect.transforms.TimestampConverter$Value",
      "transforms.ts_lastupdate.target.type": "Timestamp",
      "transforms.ts_lastupdate.field": "lastupdate",
      "transforms.ts_lastupdate.format": "YYYY-MM-dd'\''T'\''HH:mm:ss'\''Z'\''"
   }
}'


curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" localhost:8083/connectors/ -d '{
  "name": "jdbc-sink-7",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",    
     "tasks.max": "3",
      "topics": "dbserver1.public.api_user_partner_transaction_log",
      "dialect.name": "PostgreSqlDatabaseDialect",
      "table.name.format": "api_user_partner_transaction_log",    
      "connection.url": "jdbc:postgresql://pg:5432/postgres?user=postgres&password=postgres&sslMode=require",    
      "transforms": "unwrap",    
      "transforms.unwrap.type": "io.debezium.connector.yugabytedb.transforms.YBExtractNewRecordState",   
      "transforms.unwrap.drop.tombstones": "false",
      "auto.create": "true",   
      "insert.mode": "upsert",    
      "pk.fields": "k",    
      "pk.mode": "record_key",   
      "delete.enabled": "true",
      "auto.evolve":"true"
   }
}'


curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" localhost:8083/connectors/ -d '{
  "name": "jdbc-sink-8",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",    
     "tasks.max": "3",
      "topics": "dbserver1.public.api_watch_list",
      "dialect.name": "PostgreSqlDatabaseDialect",
      "table.name.format": "api_watch_list",    
      "connection.url": "jdbc:postgresql://pg:5432/postgres?user=postgres&password=postgres&sslMode=require",    
      "transforms": "unwrap",    
      "transforms.unwrap.type": "io.debezium.connector.yugabytedb.transforms.YBExtractNewRecordState",   
      "transforms.unwrap.drop.tombstones": "false",
      "auto.create": "true",   
      "insert.mode": "upsert",    
      "pk.fields": "k",    
      "pk.mode": "record_key",   
      "delete.enabled": "true",
      "auto.evolve":"true",
      "transforms": "unwrap,ts_createddate,ts_updateddate",
      "transforms.ts_createddate.type": "org.apache.kafka.connect.transforms.TimestampConverter$Value",
      "transforms.ts_createddate.target.type": "Timestamp",
      "transforms.ts_createddate.field": "createddate",
      "transforms.ts_createddate.format": "YYYY-MM-dd'\''T'\''HH:mm:ss'\''Z'\''",
      "transforms.ts_updateddate.type": "org.apache.kafka.connect.transforms.TimestampConverter$Value",
      "transforms.ts_updateddate.target.type": "Timestamp",
      "transforms.ts_updateddate.field": "updateddate",
      "transforms.ts_updateddate.format": "YYYY-MM-dd'\''T'\''HH:mm:ss'\''Z'\''"
   }
}'


curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" localhost:8083/connectors/ -d '{
  "name": "jdbc-sink-9",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",    
     "tasks.max": "3",
      "topics": "dbserver1.public.user_profile",
      "dialect.name": "PostgreSqlDatabaseDialect",
      "table.name.format": "user_profile",    
      "connection.url": "jdbc:postgresql://pg:5432/postgres?user=postgres&password=postgres&sslMode=require",    
      "transforms": "unwrap",    
      "transforms.unwrap.type": "io.debezium.connector.yugabytedb.transforms.YBExtractNewRecordState",   
      "transforms.unwrap.drop.tombstones": "false",
      "auto.create": "true",   
      "insert.mode": "upsert",    
      "pk.fields": "k",    
      "pk.mode": "record_key",   
      "delete.enabled": "true",
      "auto.evolve":"true",
      "transforms": "unwrap,ts_createddate,ts_updateddate",
      "transforms.ts_createddate.type": "org.apache.kafka.connect.transforms.TimestampConverter$Value",
      "transforms.ts_createddate.target.type": "Timestamp",
      "transforms.ts_createddate.field": "createddate",
      "transforms.ts_createddate.format": "YYYY-MM-dd'\''T'\''HH:mm:ss'\''Z'\''",
      "transforms.ts_updateddate.type": "org.apache.kafka.connect.transforms.TimestampConverter$Value",
      "transforms.ts_updateddate.target.type": "Timestamp",
      "transforms.ts_updateddate.field": "updateddate",
      "transforms.ts_updateddate.format": "YYYY-MM-dd'\''T'\''HH:mm:ss'\''Z'\''"
   }
}'

             
             
