/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.pubsub;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;

import com.yugabyte.cdcsdk.server.TestConfigSource;

public class PubSubTestConfigSource extends TestConfigSource {

    public PubSubTestConfigSource() {
        Map<String, String> pubsubTest = new HashMap<>();

        pubsubTest.put("cdcsdk.sink.type", "pubsub");
        pubsubTest.put("cdcsdk.source.connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        pubsubTest.put("cdcsdk.source." + StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG,
                OFFSET_STORE_PATH.toAbsolutePath().toString());
        pubsubTest.put("cdcsdk.source.offset.flush.interval.ms", "0");
        pubsubTest.put("cdcsdk.source.database.server.name", "testc");
        pubsubTest.put("cdcsdk.source.schema.include.list", "inventory");
        pubsubTest.put("cdcsdk.source.table.include.list", "inventory.customers");

        config = pubsubTest;
    }
}