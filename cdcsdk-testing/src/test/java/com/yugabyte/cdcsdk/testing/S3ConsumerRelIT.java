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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yugabyte.cdcsdk.sink.s3.S3ChangeConsumer;
import com.yugabyte.cdcsdk.sink.s3.S3Storage;
import com.yugabyte.cdcsdk.sink.s3.config.S3SinkConnectorConfig;
import com.yugabyte.cdcsdk.sink.s3.util.S3Utils;
import com.yugabyte.cdcsdk.testing.util.CdcsdkTestBase;
import com.yugabyte.cdcsdk.testing.util.UtilStrings;

/**
 * Release test that verifies basic reading from a YugabyteDB database and
 * writing to S3
 *
 * @author Rajat Venkatesh
 */

public class S3ConsumerRelIT extends CdcsdkTestBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3ConsumerRelIT.class);

    private S3SinkConnectorConfig s3Config;
    private ConfigSourceS3 testConfig;
    private S3Storage storage;

    @BeforeAll
    public static void beforeClass() throws Exception {
        initializeContainers();
    }

    @BeforeEach
    public void beforeEachTest() throws Exception {
        ybHelper.execute(UtilStrings.getCreateTableYBStmt(DEFAULT_TABLE_NAME));
    }

    @AfterEach
    public void dropTable() throws Exception {
        ybHelper.execute(UtilStrings.getDropTableStmt(DEFAULT_TABLE_NAME));
        clearBucket(s3Config.getBucketName(), getBaseDir());
    }

    @Test
    public void automationOfS3Assertions() throws Exception {
        testConfig = new ConfigSourceS3();
        s3Config = new S3SinkConnectorConfig(testConfig.getMapSubset(S3ChangeConsumer.PROP_S3_PREFIX));

        // At this point in code, we know that the table exists already so it's safe to get a CDCSDK server instance
        cdcsdkContainer = TestHelper.getCdcsdkContainerForS3Sink(ybHelper, "public." + DEFAULT_TABLE_NAME);
        cdcsdkContainer.withNetwork(containerNetwork);
        cdcsdkContainer.start();

        assertTrue(cdcsdkContainer.isRunning());

        storage = new S3Storage(s3Config, "");

        AmazonS3 s3Client = storage.client();

        if (!storage.bucketExists()) {
            throw new RuntimeException("The bucket you are trying to access doesn't exist...");
        }

        int recordsInserted = 5;
        for (int i = 0; i < recordsInserted; ++i) {
            ybHelper.execute(UtilStrings.getInsertStmt(DEFAULT_TABLE_NAME, i, "first_" + i, "last_" + i, 23.45));
        }

        // Wait for sometime for the data to be pushed to S3
        S3Utils.waitForFilesInDirectory(storage.client(), s3Config.getBucketName(),
                this.getBaseDir(), 1, 60);

        List<String> expected_data = List.of(
                "{\"id\":0,\"first_name\":\"first_0\",\"last_name\":\"last_0\",\"days_worked\":23.45}",
                "{\"id\":1,\"first_name\":\"first_1\",\"last_name\":\"last_1\",\"days_worked\":23.45}",
                "{\"id\":2,\"first_name\":\"first_2\",\"last_name\":\"last_2\",\"days_worked\":23.45}",
                "{\"id\":3,\"first_name\":\"first_3\",\"last_name\":\"last_3\",\"days_worked\":23.45}",
                "{\"id\":4,\"first_name\":\"first_4\",\"last_name\":\"last_4\",\"days_worked\":23.45}");

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
        assertEquals(expected_data.size(), allLines.size());

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

        // Kill the cdcsdk-server container and then drop the table before ending the test
        cdcsdkContainer.stop();
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
