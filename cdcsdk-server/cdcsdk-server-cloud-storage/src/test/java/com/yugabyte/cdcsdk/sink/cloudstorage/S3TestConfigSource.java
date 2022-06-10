package com.yugabyte.cdcsdk.sink.cloudstorage;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;

import io.debezium.server.TestConfigSource;

public class S3TestConfigSource extends TestConfigSource {

    public S3TestConfigSource() {
        Map<String, String> s3Test = new HashMap<>();

        s3Test.put("debezium.sink.type", "s3");
        s3Test.put("cdcsdk.sink.storage.s3.bucket.name", "cdcsdk-test");
        s3Test.put("cdcsdk.sink.storage.s3.region", "us-west-2");
        s3Test.put("cdcsdk.sink.storage.basedir", "S3ConsumerIT/");
        s3Test.put("cdcsdk.sink.storage.pattern", "stream_{EPOCH}");
        s3Test.put("cdcsdk.sink.storage.flushRecords", "4");
        s3Test.put("debezium.format.value", "json"); // Need to explicitly pass in the cloudevents format

        s3Test.put("debezium.source.connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        s3Test.put("debezium.source." + StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG,
                OFFSET_STORE_PATH.toAbsolutePath().toString());
        s3Test.put("debezium.source.offset.flush.interval.ms", "0");
        s3Test.put("debezium.source.database.server.name", "testc");
        s3Test.put("debezium.source.schema.include.list", "inventory");
        s3Test.put("debezium.source.table.include.list", "inventory.customers");
        s3Test.put("quarkus.log.level", "debug");

        config = s3Test;
    }

    public Map<String, String> getMapSubset(String prefix) {
        Map<String, String> subsetMap = new HashMap<>();

        config.forEach((k, v) -> {
            if (k.startsWith(prefix)) {
                subsetMap.put(k.substring(prefix.length()), v);
            }
        });

        return subsetMap;
    }

    @Override
    public int getOrdinal() {
        // Configuration property precedence is based on ordinal values and since we
        // override the
        // properties in TestConfigSource, we should give this a higher priority.
        return super.getOrdinal() + 1;
    }
}
