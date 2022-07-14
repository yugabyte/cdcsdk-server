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

WORKING_DIR=/tmp/sample_apps
TARGET_DIR=/tmp/workloads

mkdir -p $WORKING_DIR
mkdir -p $TARGET_DIR
cd $WORKING_DIR

[[ -d yb-sample-apps ]] || git clone https://github.com/suranjan/yb-sample-apps
cd  yb-sample-apps


for i in "${branches[@]}"
do
    git checkout $i
    mvn clean -DskipTests -DskipDockerBuild package
    cp target/yb-sample-apps.jar $TARGET_DIR/$i.jar
done
