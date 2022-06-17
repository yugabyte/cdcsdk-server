package com.yugabyte.cdcsdk.sink.s3;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;

import com.yugabyte.cdcsdk.server.TestConfigSource;

public class S3TestConfigSource extends TestConfigSource {

    public S3TestConfigSource() {
        Map<String, String> s3Test = new HashMap<>();

        s3Test.put("cdcsdk.sink.type", "s3");
        s3Test.put("cdcsdk.sink.s3.bucket.name", "cdcsdk-test");
        s3Test.put("cdcsdk.sink.s3.region", "us-west-2");
        s3Test.put("cdcsdk.sink.s3.basedir", "S3ConsumerIT/");
        s3Test.put("cdcsdk.sink.s3.pattern", "stream_{EPOCH}");
        s3Test.put("cdcsdk.sink.s3.flush.records", "4");
        s3Test.put("cdcsdk.server.transforms", "FLATTEN");

        s3Test.put("cdcsdk.source.connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        s3Test.put("cdcsdk.source." + StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG,
                OFFSET_STORE_PATH.toAbsolutePath().toString());
        s3Test.put("cdcsdk.source.offset.flush.interval.ms", "0");
        s3Test.put("cdcsdk.source.database.server.name", "testc");
        s3Test.put("cdcsdk.source.schema.include.list", "inventory");
        s3Test.put("cdcsdk.source.table.include.list", "inventory.customers");
        s3Test.put("quarkus.log.level", "trace");

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
