/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.yugabyte.cdcsdk.sink.cloudstorage;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;

import io.debezium.server.TestConfigSource;

public class FileTestConfigSource extends TestConfigSource {

    public FileTestConfigSource() {
        Map<String, String> fileTest = new HashMap<>();

        fileTest.put("debezium.sink.type", "file");
        fileTest.put("cdcsdk.sink.storage.basedir", "/tmp/fileIT_test");
        fileTest.put("cdcsdk.sink.storage.pattern", "stream_{EPOCH}");
        fileTest.put("cdcsdk.sink.storage.flushRecords", "1");
        fileTest.put("debezium.format.value", "json"); // Need to explicitly pass in the cloudevents format

        fileTest.put("debezium.source.connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        fileTest.put("debezium.source." + StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, OFFSET_STORE_PATH.toAbsolutePath().toString());
        fileTest.put("debezium.source.offset.flush.interval.ms", "0");
        fileTest.put("debezium.source.database.server.name", "testc");
        fileTest.put("debezium.source.schema.include.list", "inventory");
        fileTest.put("debezium.source.table.include.list", "inventory.customers");

        config = fileTest;
    }

    @Override
    public int getOrdinal() {
        // Configuration property precedence is based on ordinal values and since we override the
        // properties in TestConfigSource, we should give this a higher priority.
        return super.getOrdinal() + 1;
    }
}
