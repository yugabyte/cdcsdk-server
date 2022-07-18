#!/bin/bash
set -xe
FILE=$1

if test -f "$FILE"; then
    export $(grep -v '^#' $FILE | xargs)
fi

export PATH=$PATH:$YBPATH


ysqlsh -c "TRUNCATE TABLE host CASCADE"

cwd=$(dirname "$0")

# Insert data to create fake hosts
INSERT_HOSTS="INSERT INTO host SELECT id, 'host_' || id::TEXT AS name,
random_json(ARRAY['building','rack'],1,20) AS LOCATION FROM
generate_series(1,10) AS id;"

ysqlsh -c "${INSERT_HOSTS}"

# insert ~1.3 million records for the last 3 months
for START in `seq 1 1 180`
do
    pid_list=()
    for HOUR in `seq 0 1 23`
    do
        INSERT_TIMESERIES="INSERT INTO host_data(date,host_id,cpu,tempc,status) SELECT date, host_id,
          random_between(5,100,3) AS cpu, random_between(28,90) AS tempc,
          random_text(20,75) AS status FROM generate_series('2022-07-01'::date +
          INTERVAL '${START} days ${HOUR} hours','2022-07-01'::Date + INTERVAL
          '${START} days $((${HOUR}+1)) hours', INTERVAL '15 seconds') AS date, generate_series(1,10) AS host_id;"
        echo "$INSERT_TIMESERIES"
        ysqlsh -c "$INSERT_TIMESERIES" &
        pid_list[${HOUR}]=$!
    done
    for p in ${pid_list[*]}; do
        wait $p
    done
done

    
