/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.http;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;

import com.yugabyte.cdcsdk.server.TestConfigSource;

public class HttpTestConfigSource extends TestConfigSource {

    public HttpTestConfigSource() {
        Map<String, String> httpTest = new HashMap<>();

        httpTest.put("cdcsdk.sink.type", "http");
        httpTest.put("cdcsdk.format.value", "json"); // Need to explicitly pass in the cloudevents format

        httpTest.put("cdcsdk.source.connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        httpTest.put("cdcsdk.source." + StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG,
                OFFSET_STORE_PATH.toAbsolutePath().toString());
        httpTest.put("cdcsdk.source.offset.flush.interval.ms", "0");
        httpTest.put("cdcsdk.source.database.server.name", "testc");
        httpTest.put("cdcsdk.source.schema.include.list", "inventory");
        httpTest.put("cdcsdk.source.table.include.list", "inventory.customers");

        config = httpTest;
    }

    @Override
    public int getOrdinal() {
        // Configuration property precedence is based on ordinal values and since we override the
        // properties in TestConfigSource, we should give this a higher priority.
        return super.getOrdinal() + 1;
    }
}
