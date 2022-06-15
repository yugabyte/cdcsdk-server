/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.yugabyte.cdcsdk.server;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.eclipse.microprofile.config.spi.ConfigSource;

import io.debezium.data.Json;
import io.debezium.util.Testing;

/**
 * A config source used during tests. Amended/overridden by values exposed from
 * test lifecycle listeners.
 */
public class TestConfigSource implements ConfigSource {

    public static final String OFFSETS_FILE = "file-connector-offsets.txt";
    public static final Path OFFSET_STORE_PATH = Testing.Files.createTestingPath(OFFSETS_FILE).toAbsolutePath();
    public static final Path TEST_FILE_PATH = Testing.Files.createTestingPath("file-connector-input.txt")
            .toAbsolutePath();

    final Map<String, String> integrationTest = new HashMap<>();
    final Map<String, String> kinesisTest = new HashMap<>();
    final Map<String, String> pubsubTest = new HashMap<>();
    final Map<String, String> unitTest = new HashMap<>();
    protected Map<String, String> config;

    public TestConfigSource() {
        integrationTest.put("cdcsdk.sink.type", "test");
        integrationTest.put("cdcsdk.source.connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        integrationTest.put("cdcsdk.source." + StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG,
                OFFSET_STORE_PATH.toAbsolutePath().toString());
        integrationTest.put("cdcsdk.source.offset.flush.interval.ms", "0");
        integrationTest.put("cdcsdk.source.database.server.name", "testc");
        integrationTest.put("cdcsdk.source.schema.include.list", "inventory");
        integrationTest.put("cdcsdk.source.table.include.list", "inventory.customers");

        String format = System.getProperty("test.apicurio.converter.format");
        String formatKey = System.getProperty("cdcsdk.format.key");
        String formatValue = System.getProperty("cdcsdk.format.value");

        if (format != null && format.length() != 0) {
            integrationTest.put("cdcsdk.format.key", format);
            integrationTest.put("cdcsdk.format.value", format);
        }
        else {
            formatKey = (formatKey != null) ? formatKey : Json.class.getSimpleName().toLowerCase();
            formatValue = (formatValue != null) ? formatValue : Json.class.getSimpleName().toLowerCase();
            integrationTest.put("cdcsdk.format.key", formatKey);
            integrationTest.put("cdcsdk.format.value", formatValue);
        }

        unitTest.put("cdcsdk.sink.type", "test");
        unitTest.put("cdcsdk.source.connector.class", "org.apache.kafka.connect.file.FileStreamSourceConnector");
        unitTest.put("cdcsdk.source." + StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG,
                OFFSET_STORE_PATH.toAbsolutePath().toString());
        unitTest.put("cdcsdk.source.offset.flush.interval.ms", "0");
        unitTest.put("cdcsdk.source.file", TEST_FILE_PATH.toAbsolutePath().toString());
        unitTest.put("cdcsdk.source.topic", "topicX");
        unitTest.put("cdcsdk.server.format.schemas.enable", "true");
        unitTest.put("cdcsdk.server.format.value.schemas.enable", "false");
        unitTest.put("cdcsdk.server.transforms", "hoist");
        unitTest.put("cdcsdk.server.transforms.hoist.type", "org.apache.kafka.connect.transforms.HoistField$Value");
        unitTest.put("cdcsdk.server.transforms.hoist.field", "line");

        // DBZ-2622 For testing properties passed via smallrye/microprofile environment
        // variables
        unitTest.put("CDCSDK_SOURCE_TABLE_WHITELIST", "public.table_name");
        unitTest.put("cdcsdk_source_offset_flush_interval_ms_Test", "0");
        unitTest.put("cdcsdk.source.snapshot.select.statement.overrides.public.table_name",
                "SELECT * FROM table_name WHERE 1>2");
        unitTest.put("cdcsdk.source.database.allowPublicKeyRetrieval", "true");

        if (isItTest()) {
            config = integrationTest;
        }
        else {
            config = unitTest;
        }
    }

    public static boolean isItTest() {
        return "IT".equals(System.getProperty("test.type"));
    }

    @Override
    public Map<String, String> getProperties() {
        return config;
    }

    @Override
    public String getValue(String propertyName) {
        return config.get(propertyName);
    }

    @Override
    public String getName() {
        return "test";
    }

    @Override
    public Set<String> getPropertyNames() {
        return config.keySet();
    }

    public static int waitForSeconds() {
        return 60;
    }
}