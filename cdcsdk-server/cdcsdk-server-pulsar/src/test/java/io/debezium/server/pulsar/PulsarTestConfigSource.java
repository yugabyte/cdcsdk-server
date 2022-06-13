/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.pulsar;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;

import com.yugabyte.cdcsdk.server.TestConfigSource;

public class PulsarTestConfigSource extends TestConfigSource {

    public PulsarTestConfigSource() {
        Map<String, String> pulsarTest = new HashMap<>();

        pulsarTest.put("cdcsdk.sink.type", "pulsar");
        pulsarTest.put("cdcsdk.source.connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        pulsarTest.put("cdcsdk.source." + StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG,
                OFFSET_STORE_PATH.toAbsolutePath().toString());
        pulsarTest.put("cdcsdk.source.offset.flush.interval.ms", "0");
        pulsarTest.put("cdcsdk.source.database.server.name", "testc");
        pulsarTest.put("cdcsdk.source.schema.include.list", "inventory");
        pulsarTest.put("cdcsdk.source.table.include.list", "inventory.customers,inventory.nokey");

        config = pulsarTest;
    }
}
