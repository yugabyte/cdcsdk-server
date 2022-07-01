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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.YugabyteYSQLContainer;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yugabyte.cdcsdk.sink.s3.S3ChangeConsumer;
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

    private static YugabyteYSQLContainer ybContainer;

    private S3SinkConnectorConfig s3Config;
    private ConfigSourceS3 testConfig;
    private S3Storage storage;

    @BeforeAll
    public static void beforeClass() throws Exception {
        // This function assumes that we have yugabyted running locally

        // Commenting out the below code for now because of tserver crash observed inside container
        /*
         * System.out.println("Getting the container...");
         * ybContainer = TestHelper.getYbContainer();
         * ybContainer.start();
         * 
         * System.out.println("Setting the hosts and ports values...");
         * // TestHelper.setHost(ybContainer.getContainerInfo().getNetworkSettings().getNetworks().entrySet().stream().findFirst().get().getValue().getIpAddress());
         * // System.out.println("Setting IP as: " + InetAddress.getLocalHost().getHostAddress());
         * TestHelper.setHost(InetAddress.getLocalHost().getHostAddress());
         * TestHelper.setYsqlPort(ybContainer.getMappedPort(5433));
         * TestHelper.setMasterPort(ybContainer.getMappedPort(7100));
         */
    }

    @BeforeEach
    public void createTable() throws Exception {
        // String createTableSql = "CREATE TABLE IF NOT EXISTS test_table (id INT, name VARCHAR(255))";
        // TestHelper.execute(createTableSql);
    }

    @AfterEach
    public void dropTable() throws Exception {
        // String dropTableSql = "DROP TABLE IF EXISTS test_table";
        // TestHelper.execute(dropTableSql);
    }

    private String getBaseDir() {
        return testConfig.getValue("cdcsdk.sink.storage.basedir");
    }

    private void clearBucket(String bucketName, String prefix) {
        AmazonS3 S3Client = storage.client();
        List<String> files = S3Utils.getDirectoryFiles(S3Client, bucketName, prefix);
        for (String file : files) {
            S3Client.deleteObject(bucketName, file);
        }
    }

    // @Disabled
    // @Test
    public void testAutomationOfS3Assertions() throws Exception {
        // Assuming that the table is created at this point with the schema
        // {id int primary key, first_name varchar(30), last_name varchar(50), days_worked double precision}
        // CREATE TABLE IF NOT EXISTS test_table (id int primary key, first_name varchar(30), last_name varchar(50), days_worked double precision);
        TestHelper.execute("CREATE TABLE IF NOT EXISTS test_table (id int primary key, first_name varchar(30), last_name varchar(50), days_worked double precision);");

        testConfig = new ConfigSourceS3();
        s3Config = new S3SinkConnectorConfig(testConfig.getMapSubset(S3ChangeConsumer.PROP_SINK_PREFIX));

        // todo vaibhav: add configuration from a resource file if possible
        storage = new S3Storage(s3Config, "");

        AmazonS3 s3Client = storage.client();

        if (!storage.bucketExists()) {
            throw new RuntimeException("The bucket you are trying to access doesn't exist...");
        }

        int recordsInserted = 5;
        for (int i = 0; i < recordsInserted; ++i) {
            String insertSql = String.format("INSERT INTO test_table VALUES (%d, '%s', '%s', %f);", i, "first_" + i, "last_" + i, 23.45);
            TestHelper.execute(insertSql);
        }

        System.out.println("Waiting for sometime for the data to be pushed to S3...");
        Thread.sleep(5000);

        List<String> expected_data = List.of(
                "{\"id\":{\"value\":0,\"set\":true},\"first_name\":{\"value\":\"first_0\",\"set\":true},\"last_name\":{\"value\":\"last_0\",\"set\":true},\"days_worked\":{\"value\":23.45,\"set\":true}}",
                "{\"id\":{\"value\":1,\"set\":true},\"first_name\":{\"value\":\"first_1\",\"set\":true},\"last_name\":{\"value\":\"last_1\",\"set\":true},\"days_worked\":{\"value\":23.45,\"set\":true}}",
                "{\"id\":{\"value\":2,\"set\":true},\"first_name\":{\"value\":\"first_2\",\"set\":true},\"last_name\":{\"value\":\"last_2\",\"set\":true},\"days_worked\":{\"value\":23.45,\"set\":true}}",
                "{\"id\":{\"value\":3,\"set\":true},\"first_name\":{\"value\":\"first_3\",\"set\":true},\"last_name\":{\"value\":\"last_3\",\"set\":true},\"days_worked\":{\"value\":23.45,\"set\":true}}",
                "{\"id\":{\"value\":4,\"set\":true},\"first_name\":{\"value\":\"first_4\",\"set\":true},\"last_name\":{\"value\":\"last_4\",\"set\":true},\"days_worked\":{\"value\":23.45,\"set\":true}}");

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

        int recordsAsserted = 0;
        for (String line : allLines) {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode node = mapper.readTree(line);
            assertEquals(mapper.readTree(expected.next()), node);
            ++recordsAsserted;
            if (recordsAsserted == recordsInserted) {
                break;
            }
        }

        clearBucket(s3Config.getBucketName(), getBaseDir());
    }

    @Test
    public void testAutomation() throws Exception {
        System.out.println("Running the test testAutomation...");

        // At this point in code, we know that the table exists already so it's safe to get a CDCSDK server instance
        GenericContainer<?> cdcContainer = TestHelper.getCdcsdkContainer();
        cdcContainer.start();

        System.out.println("Waiting for 20s after starting the container");
        Thread.sleep(20000);

        int recordsInserted = 5;
        System.out.println("Going to insert records...");
        for (int i = 0; i < recordsInserted; ++i) {
            String insertSql = String.format("INSERT INTO test_table VALUES (%d, '%s', '%s', %f);", i, "first_" + i, "last_" + i, 23.45);
            TestHelper.execute(insertSql);
        }

        System.out.println("Waiting for 30s to atleast let the container push values to the s3bucket");
        Thread.sleep(30000);

        Path path = Paths.get("/home/ec2-user/test-log.txt");

        Files.writeString(path, cdcContainer.getLogs(), StandardCharsets.UTF_8);
        // System.out.println("Wait for 5 minutes to analyze the docker logs");
        // Thread.sleep(600000);
        System.out.println("Ending the test");
    }

    private class ConfigSourceS3 {
        Map<String, String> s3Test = new HashMap<>();

        public ConfigSourceS3() {
            s3Test.put("cdcsdk.sink.type", "s3");
            s3Test.put("cdcsdk.sink.s3.bucket.name", "cdcsdk-test");
            s3Test.put("cdcsdk.sink.s3.region", "us-west-2");
            s3Test.put("cdcsdk.sink.s3.basedir", "S3ConsumerIT/");
            s3Test.put("cdcsdk.sink.s3.pattern", "stream_12345");
            s3Test.put("cdcsdk.sink.s3.flushRecords", "5");
            s3Test.put("cdcsdk.server.transforms", "FLATTEN");
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
