package com.yugabyte.cdcsdk.testing;

import java.util.HashMap;
import java.util.Map;

import com.yugabyte.cdcsdk.server.TestConfigSource;

public class S3TestConfigSourceRel extends TestConfigSource {
    public S3TestConfigSourceRel() {
        Map<String, String> s3Test = new HashMap<>();
        String dbStreamId = "";
        System.out.println("Going to create a stream ID");
        try {
            dbStreamId = TestHelper.getNewDbStreamId("yugabyte");
        }
        catch (Exception e) {
            System.out.println("Exception thrown while creating stream ID: " + e);
        }
        System.out.println("Created stream ID: " + dbStreamId);

        s3Test.put("cdcsdk.sink.type", "s3");
        s3Test.put("cdcsdk.sink.s3.bucket.name", "cdcsdk-test");
        s3Test.put("cdcsdk.sink.s3.region", "us-west-2");
        s3Test.put("cdcsdk.sink.s3.basedir", "S3ConsumerIT/");
        s3Test.put("cdcsdk.sink.s3.pattern", "stream_{EPOCH}");
        s3Test.put("cdcsdk.sink.s3.flushRecords", "3");
        s3Test.put("cdcsdk.server.transforms", "FLATTEN");

        s3Test.put("cdcsdk.source.connector.class", "io.debezium.connector.yugabytedb.YugabyteDBConnector");
        // s3Test.put("cdcsdk.source." + StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG,
        // OFFSET_STORE_PATH.toAbsolutePath().toString());
        s3Test.put("cdcsdk.source.offset.flush.interval.ms", "0");
        s3Test.put("cdcsdk.source.database.hostname", "127.0.0.1");
        s3Test.put("cdcsdk.source.database.port", "5433");
        s3Test.put("cdcsdk.source.database.user", "yugabyte");
        s3Test.put("cdcsdk.source.database.password", "yugabyte");
        s3Test.put("cdcsdk.source.database.dbname", "yugabyte");
        s3Test.put("cdcsdk.source.database.streamid", dbStreamId);
        s3Test.put("cdcsdk.source.database.master.addresses", "127.0.0.1:7100");
        s3Test.put("cdcsdk.source.snapshot.mode", "never");
        s3Test.put("cdcsdk.source.database.server.name", "testc");
        s3Test.put("cdcsdk.source.schema.include.list", "public");
        s3Test.put("cdcsdk.source.table.include.list", "public.test_table");
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
