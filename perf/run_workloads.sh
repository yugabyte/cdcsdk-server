#!/bin/bash
set -ex

declare -a branches=("api_sub_apple_orig_transactions"
                     "test-api_apple_store_receipt_log" 
                     "test-api_oauth_access_token"
                     "test-api_sub_recurly_notify_log" 
                     "test-api_sub_user_packages"
                     "test-api_user_device" 
                     "test-api_user_partner_transaction_log"
                     "test-api_watch_list" 
                     "test-user_profile")

declare -a tables=("api_sub_apple_orig_transactions"
                     "api_apple_store_receipt_log" 
                     "api_oauth_access_token"
                     "api_sub_recurly_notify_log" 
                     "api_sub_user_packages"
                     "api_user_device" 
                     "api_user_partner_transaction_log"
                     "api_watch_list" 
                     "user_profile")


FILE=$1
if test -f "$FILE"; then
    export $(grep -v '^#' $FILE | xargs)
fi

export PATH=$PATH:$YBPATH
for t in "${tables[@]}"
do
    ysqlsh -c "TRUNCATE TABLE $t"
done

TARGET_DIR=/tmp/workloads
cd $WORKING_DIR

pid_list=""

for i in "${branches[@]}"
do
    java -jar $TARGET_DIR/$i.jar --workload SqlInserts --nodes $PGHOST:5433 \
    --num_writes 100000000 --num_unique_keys 100000000 --num_threads_write 1 \
    --num_reads 0 --default_postgres_database yugabyte &
    pid_list="$pid_list $!"     
done

( trap exit SIGINT ; read -r -d '' _ </dev/tty ) ## wait for Ctrl-C

# Start killing background processes from list
for p in $pid_list
do
        echo killing $p. Process is still there.
        ps | grep $p
        kill $p
        ps | grep $p
        echo Just ran "'"ps"'" command, $dPid must not show again.
done
