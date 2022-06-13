/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.kafka;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;

import com.yugabyte.cdcsdk.server.TestConfigSource;

public class KafkaTestConfigSource extends TestConfigSource {

    public KafkaTestConfigSource() {
        final Map<String, String> kafkaConfig = new HashMap<>();

        kafkaConfig.put("cdcsdk.sink.type", "kafka");
        kafkaConfig.put("cdcsdk.sink.kafka.producer.bootstrap.servers",
                KafkaTestResourceLifecycleManager.getBootstrapServers());
        kafkaConfig.put("cdcsdk.sink.kafka.producer.key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        kafkaConfig.put("cdcsdk.sink.kafka.producer.value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        kafkaConfig.put("debezium.source.connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        kafkaConfig.put("cdcsdk.source." + StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG,
                OFFSET_STORE_PATH.toAbsolutePath().toString());

        kafkaConfig.put("cdcsdk.source.offset.flush.interval.ms", "0");
        kafkaConfig.put("cdcsdk.source.database.server.name", "testc");
        kafkaConfig.put("cdcsdk.source.schema.include.list", "inventory");
        kafkaConfig.put("cdcsdk.source.table.include.list", "inventory.customers");

        config = kafkaConfig;
    }
}
