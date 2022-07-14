#!/bin/bash
set -x
set -e 

FILE=$1

if test -f "$FILE"; then
    export $(grep -v '^#' $FILE | xargs)
fi

export PATH=$PATH:$YBPATH

ysqlsh -f ysql_drop_tables.sql
if [[ -n "$var" ]]; then
    yb-admin --master_addresses $MASTER_ADDRESSES delete_change_data_stream $CDC_SDK_STREAM_ID
fi

ysqlsh -f ysql_schema.sql 
yb-admin create_change_data_stream ysql.yugabyte --master_addresses $MASTER_ADDRESSES

