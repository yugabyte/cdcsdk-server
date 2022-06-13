/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package com.yugabyte.cdcsdk.sink.s3;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.yugabyte.cdcsdk.server.ServerApp;
import com.yugabyte.cdcsdk.sink.s3.config.S3SinkConnectorConfig;
import com.yugabyte.cdcsdk.sink.s3.util.S3Utils;

import io.debezium.testing.testcontainers.PostgresTestResourceLifecycleManager;
import io.debezium.util.Testing;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;

/**
 * Integration test that verifies basic reading from PostgreSQL database and
 * writing to S3
 *
 * @author Rajat Venkatesh
 */

@QuarkusTest
@QuarkusTestResource(PostgresTestResourceLifecycleManager.class)
public class S3ConsumerIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3ConsumerIT.class);

    // local dir configs
    private static final String TEST_RESOURCES_PATH = "src/test/resources/";
    private static final String TEST_DOWNLOAD_PATH = TEST_RESOURCES_PATH + "downloaded-files/";
    private static final long S3_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(60);

    @Inject
    ServerApp server;

    static S3Storage storage;
    static S3SinkConnectorConfig s3Config;
    static S3TestConfigSource testConfig;

    private static String getBaseDir() {
        return testConfig.getValue("cdcsdk.sink.storage.basedir");
    }

    @BeforeAll
    public static void setupClient() throws Exception {
        testConfig = new S3TestConfigSource();
        s3Config = new S3SinkConnectorConfig(
                testConfig.getMapSubset(FlushingChangeConsumer.PROP_PREFIX));

        storage = new S3Storage(s3Config, "");
        if (storage.bucketExists()) {
            clearBucket(s3Config.getBucketName(), S3ConsumerIT.getBaseDir());
        }
        else {
            throw new Exception("S3 Bucket does not exist: " + s3Config.getBucketName());
        }
        LOGGER.info("Test Client has been setup");
    }

    /**
     * Clear the given S3 bucket. Removes the contents, keeps the bucket.
     *
     * @param bucketName the name of the bucket to clear.
     */
    private static void clearBucket(String bucketName, String prefix) {
        AmazonS3 S3Client = storage.client();
        List<String> files = S3Utils.getDirectoryFiles(S3Client, bucketName, prefix);
        for (String file : files) {
            S3Client.deleteObject(bucketName, file);
        }
    }

    @AfterEach
    public void after() throws Exception {
        // delete the downloaded test file folder
        FileUtils.deleteDirectory(new File(TEST_DOWNLOAD_PATH));
        // clear for next test
        clearBucket(s3Config.getBucketName(), S3ConsumerIT.getBaseDir());
        // wait for bucket to clear
        S3Utils.waitForFilesInDirectory(storage.client(), s3Config.getBucketName(), S3ConsumerIT.getBaseDir(), 0,
                S3_TIMEOUT_MS);
    }

    @Test
    public void testS3() throws IOException, InterruptedException {
        Testing.Print.enable();
        System.out.println(storage);
        System.out.println(s3Config);
        S3Utils.waitForFilesInDirectory(storage.client(), s3Config.getBucketName(), S3ConsumerIT.getBaseDir(), 1, 60);

        // Testing payload section
        /*
         * "payload": {
         * "before": null,
         * "after": {
         * "id": 1004,
         * "first_name": "Anne",
         * "last_name": "Kretchmar",
         * "email": "annek@noanswer.org"
         * },
         * "source": {
         * "version": "1.7.0.Final",
         * "connector": "postgresql",
         * "name": "testc",
         * "ts_ms": 1654157318401,
         * "snapshot": "last",
         * "db": "postgres",
         * "sequence": "[null,\"36167792\"]",
         * "schema": "inventory",
         * "table": "customers",
         * "txId": 761,
         * "lsn": 36167792,
         * "xmin": null
         * },
         * "op": "r",
         * "ts_ms": 1654157318401,
         * "transaction": null
         * }
         */

        List<String> expected_data = List.of(
                "{\"id\":1001,\"first_name\":\"Sally\",\"last_name\":\"Thomas\",\"email\":\"sally.thomas@acme.com\"}",
                "{\"id\":1002,\"first_name\":\"George\",\"last_name\":\"Bailey\",\"email\":\"gbailey@foobar.com\"}",
                "{\"id\":1003,\"first_name\":\"Edward\",\"last_name\":\"Walker\",\"email\":\"ed@walker.com\"}",
                "{\"id\":1004,\"first_name\":\"Anne\",\"last_name\":\"Kretchmar\",\"email\":\"annek@noanswer.org\"}");

        Iterator<String> expected = expected_data.iterator();

        AmazonS3 s3Client = storage.client();

        List<String> fileNames = S3Utils.getDirectoryFiles(s3Client, s3Config.getBucketName(),
                S3ConsumerIT.getBaseDir());
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
            JsonNode after = node.get("payload").get("after");
            assertEquals(mapper.readTree(expected.next()), after);
        }
    }
}
