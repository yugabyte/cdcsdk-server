/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package com.yugabyte.cdcsdk.testing;

import static org.junit.jupiter.api.Assertions.*;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yugabyte.cdcsdk.sink.s3.FlushingChangeConsumer;
import com.yugabyte.cdcsdk.sink.s3.S3Storage;
import com.yugabyte.cdcsdk.sink.s3.config.S3SinkConnectorConfig;
import com.yugabyte.cdcsdk.sink.s3.util.S3Utils;

/**
 * Release test that verifies basic reading from PostgreSQL database and
 * writing to S3
 *
 * @author Rajat Venkatesh
 */

public class S3ConsumerRelIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3ConsumerRelIT.class);

    private S3SinkConnectorConfig s3Config;
    private ConfigSourceS3 testConfig;
    private S3Storage storage;

    @BeforeEach
    public void createTable() throws Exception {
        String createTableSql = "CREATE TABLE IF NOT EXISTS test_table (id INT, name VARCHAR(255))";
        TestHelper.execute(createTableSql);
    }

    @AfterEach
    public void dropTable() throws Exception {
        String dropTableSql = "DROP TABLE IF EXISTS test_table";
        TestHelper.execute(dropTableSql);
    }

    private String getBaseDir() {
        return testConfig.getValue("cdcsdk.sink.storage.basedir");
    }

    @Test
    public void testSample() {
        System.out.println("Running testSample()");
        assertEquals(0, 0);
    }

    @Test
    public void testAutomationOfS3Assertions() throws Exception {
        // Assuming that the table is created at this point with the schema
        // {id int primary key, first_name varchar(30), last_name varchar(50), days_worked double precision}
        // CREATE TABLE IF NOT EXISTS test_table (id int primary key, first_name varchar(30), last_name varchar(50), days_worked double precision);
        testConfig = new ConfigSourceS3();
        s3Config = new S3SinkConnectorConfig(testConfig.getMapSubset(FlushingChangeConsumer.PROP_SINK_PREFIX));

        // todo vaibhav: add configuration from a resource file if possible
        storage = new S3Storage(s3Config, "");

        AmazonS3 s3Client = storage.client();

        if (!storage.bucketExists()) {
            System.out.println("The bucket doesn't exist because it has not been created yet lol");
        }

        for (int i = 0; i < 5; ++i) {
            String insertSql = String.format("INSERT INTO test_table VALUES (%d, %s, %s, %f);", i, "first_" + i, "last_" + i, 23.45);
            TestHelper.execute(insertSql);
        }

        System.out.println("Waiting for sometime for the data to be pushed to S3...");
        Thread.sleep(5000);

        List<String> expected_data = List.of(
                "{\"id\":\"0\",\"first_name\":\"first_0\",\"last_name\":\"last_0\",\"days_worked\":\"23.45\"}",
                "{\"id\":\"1\",\"first_name\":\"first_1\",\"last_name\":\"last_1\",\"days_worked\":\"23.45\"}",
                "{\"id\":\"2\",\"first_name\":\"first_2\",\"last_name\":\"last_2\",\"days_worked\":\"23.45\"}",
                "{\"id\":\"3\",\"first_name\":\"first_3\",\"last_name\":\"last_3\",\"days_worked\":\"23.45\"}",
                "{\"id\":\"4\",\"first_name\":\"first_4\",\"last_name\":\"last_4\",\"days_worked\":\"23.45\"}");

        Iterator<String> expected = expected_data.iterator();

        List<String> fileNames = S3Utils.getDirectoryFiles(s3Client, s3Config.getBucketName(),
                this.getBaseDir());
        List<String> allLines = new ArrayList<>();

        for (String file : fileNames) {
            S3Object object = s3Client.getObject(new GetObjectRequest(s3Config.getBucketName(), file));
            InputStream objectData = object.getObjectContent();
            BufferedReader reader = new BufferedReader(new InputStreamReader(objectData));
            String line = reader.readLine();
            while (line != null) {
                allLines.add(line);
                line = reader.readLine();
            }
            // Process the objectData stream.
            objectData.close();
        }

        for (String line : allLines) {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode node = mapper.readTree(line);
            assertEquals(mapper.readTree(expected.next()), node);
        }
    }

    private class ConfigSourceS3 {
        Map<String, String> s3Test = new HashMap<>();

        public ConfigSourceS3() {
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
            s3Test.put("cdcsdk.sink.s3.flushRecords", "5");
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
        }

        public Map<String, String> getMapSubset(String prefix) {
            Map<String, String> subsetMap = new HashMap<>();

            s3Test.forEach((k, v) -> {
                if (k.startsWith(prefix)) {
                    subsetMap.put(k.substring(prefix.length()), v);
                }
            });

            return subsetMap;
        }

        public String getValue(String key) {
            return s3Test.get(key);
        }
    }
}
