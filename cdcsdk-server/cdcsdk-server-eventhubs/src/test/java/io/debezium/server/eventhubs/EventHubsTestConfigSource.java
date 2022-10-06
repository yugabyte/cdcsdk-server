/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.eventhubs;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;

import com.yugabyte.cdcsdk.server.TestConfigSource;

public class EventHubsTestConfigSource extends TestConfigSource {

    static final String EVENTHUBS_CONNECTION_STRING_SYSTEM_PROPERTY_NAME = "eventhubs.connection.string";
    static final String EVENTHUBS_NAME_SYSTEM_PROPERTY_NAME = "eventhubs.hub.name";
    static final String CONNECTION_STRING_FORMAT = "%s;EntityPath=%s";

    public EventHubsTestConfigSource() {
        Map<String, String> eventHubsTest = new HashMap<>();

        // event hubs sink config
        eventHubsTest.put("cdcsdk.sink.type", "eventhubs");
        eventHubsTest.put("cdcsdk.sink.eventhubs.connectionstring", getEventHubsConnectionString());
        eventHubsTest.put("cdcsdk.sink.eventhubs.hubname", getEventHubsName());

        // postgresql source config

        eventHubsTest.put("cdcsdk.source.connector.class", "io.debezium.connector.postgresql.PostgresConnector");

        eventHubsTest.put("cdcsdk.source." + StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG,
                OFFSET_STORE_PATH.toAbsolutePath().toString());
        eventHubsTest.put("cdcsdk.source.offset.flush.interval.ms", "0");
        eventHubsTest.put("cdcsdk.source.database.server.name", "testc");
        eventHubsTest.put("cdcsdk.source.schema.include.list", "inventory");
        eventHubsTest.put("cdcsdk.source.table.include.list", "inventory.customers");

        config = eventHubsTest;
    }

    public static String getEventHubsConnectionString() {
        return System.getProperty(EVENTHUBS_CONNECTION_STRING_SYSTEM_PROPERTY_NAME);
    }

    public static String getEventHubsName() {
        return System.getProperty(EVENTHUBS_NAME_SYSTEM_PROPERTY_NAME);
    }
}
