/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.nats.streaming;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;

import com.yugabyte.cdcsdk.server.TestConfigSource;

public class NatsStreamingTestConfigSource extends TestConfigSource {

    public NatsStreamingTestConfigSource() {
        Map<String, String> natsStreamingTest = new HashMap<>();

        natsStreamingTest.put("cdcsdk.sink.type", "nats-streaming");
        natsStreamingTest.put("cdcsdk.sink.nats-streaming.url",
                NatsStreamingTestResourceLifecycleManager.getNatsStreamingContainerUrl());
        natsStreamingTest.put("cdcsdk.sink.nats-streaming.cluster.id", "cdcsdk");
        natsStreamingTest.put("cdcsdk.sink.nats-streaming.client.id", "cdcsdk-sink");
        natsStreamingTest.put("cdcsdk.source.connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        natsStreamingTest.put("cdcsdk.source.database.server.name", "testc");
        natsStreamingTest.put("cdcsdk.source.schema.include.list", "inventory");
        natsStreamingTest.put("cdcsdk.source.table.include.list", "inventory.customers");
        natsStreamingTest.put("cdcsdk.source." + StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG,
                OFFSET_STORE_PATH.toAbsolutePath().toString());
        natsStreamingTest.put("cdcsdk.source.offset.flush.interval.ms", "0");

        config = natsStreamingTest;
    }
}
