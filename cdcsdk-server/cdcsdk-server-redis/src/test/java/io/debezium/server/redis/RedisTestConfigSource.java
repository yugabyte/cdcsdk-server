/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.redis;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;

import com.yugabyte.cdcsdk.server.TestConfigSource;

public class RedisTestConfigSource extends TestConfigSource {

    public RedisTestConfigSource() {
        Map<String, String> redisTest = new HashMap<>();

        redisTest.put("cdcsdk.sink.type", "redis");
        redisTest.put("cdcsdk.sink.redis.address", RedisTestResourceLifecycleManager.getRedisContainerAddress());
        redisTest.put("cdcsdk.source.connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        redisTest.put("cdcsdk.source." + StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG,
                OFFSET_STORE_PATH.toAbsolutePath().toString());
        redisTest.put("cdcsdk.source.offset.flush.interval.ms", "0");
        redisTest.put("cdcsdk.source.database.server.name", "testc");
        redisTest.put("cdcsdk.source.schema.include.list", "inventory");
        redisTest.put("cdcsdk.source.table.include.list", "inventory.customers");

        config = redisTest;
    }
}
