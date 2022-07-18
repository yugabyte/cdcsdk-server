#!/bin/bash
set -x
set -e 

FILE=$1

if test -f "$FILE"; then
    export $(grep -v '^#' $FILE | xargs)
fi

export PATH=$PATH:$YBPATH

ysqlsh -f workloads/${WORKLOAD}/drop_schema.sql
if [[ -n "$CDC_SDK_STREAM_ID" ]]; then
    yb-admin --master_addresses $MASTER_ADDRESSES delete_change_data_stream $CDC_SDK_STREAM_ID
fi

ysqlsh -f workloads/${WORKLOAD}/schema.sql 
yb-admin create_change_data_stream ysql.yugabyte --master_addresses $MASTER_ADDRESSES

