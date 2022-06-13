/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.kinesis;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;

import com.yugabyte.cdcsdk.server.TestConfigSource;

public class KinesisTestConfigSource extends TestConfigSource {

    public static final String KINESIS_REGION = "eu-central-1";

    public KinesisTestConfigSource() {
        Map<String, String> kinesisTest = new HashMap<>();

        kinesisTest.put("cdcsdk.sink.type", "kinesis");
        kinesisTest.put("cdcsdk.sink.kinesis.region", KINESIS_REGION);
        kinesisTest.put("cdcsdk.source.connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        kinesisTest.put("cdcsdk.source." + StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG,
                OFFSET_STORE_PATH.toAbsolutePath().toString());
        kinesisTest.put("cdcsdk.source.offset.flush.interval.ms", "0");
        kinesisTest.put("cdcsdk.source.database.server.name", "testc");
        kinesisTest.put("cdcsdk.source.schema.include.list", "inventory");
        kinesisTest.put("cdcsdk.source.table.include.list", "inventory.customers");

        config = kinesisTest;
    }
}
