package com.yugabyte.cdcsdk.testing;

import java.util.HashMap;
import java.util.Map;

import com.yugabyte.cdcsdk.server.TestConfigSource;

public class S3TestConfigSourceRel extends TestConfigSource {
    public S3TestConfigSourceRel() {
        Map<String, String> s3Test = new HashMap<>();

        s3Test.put("cdcsdk.sink.type", "s3");
        s3Test.put("cdcsdk.sink.s3.bucket.name", "cdcsdk-test");
        s3Test.put("cdcsdk.sink.s3.region", "us-west-2");
        s3Test.put("cdcsdk.sink.s3.basedir", "S3ConsumerIT/");
        s3Test.put("cdcsdk.sink.s3.pattern", "stream_{EPOCH}");
        s3Test.put("cdcsdk.sink.s3.flushRecords", "3");
        s3Test.put("cdcsdk.server.transforms", "FLATTEN");
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
