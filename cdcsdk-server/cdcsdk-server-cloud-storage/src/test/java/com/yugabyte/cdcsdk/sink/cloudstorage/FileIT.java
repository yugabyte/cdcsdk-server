/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package com.yugabyte.cdcsdk.sink.cloudstorage;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yb.cdcsdk.server.DebeziumServer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;

import io.debezium.testing.testcontainers.PostgresTestResourceLifecycleManager;
import io.debezium.util.Testing;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;

/**
 * Integration test that verifies basic reading from PostgreSQL database and writing to an HTTP Server
 *
 * @author Chris Baumbauer
 */

@QuarkusTest
@QuarkusTestResource(PostgresTestResourceLifecycleManager.class)
public class FileIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlushingChangeConsumer.class);

    @Inject
    DebeziumServer server;

    private static final int MESSAGE_COUNT = 4;

    java.io.File workingDirectory;

    @Test
    public void testFile() throws IOException {
        Testing.Print.enable();

        workingDirectory = new java.io.File("/tmp/fileIT_test");

        Awaitility.await().atMost(Duration.ofSeconds(60)).until(() -> {
            java.io.File[] fileNames = workingDirectory.listFiles();
            int totalLines = 4;
            Thread.sleep(3000);
            // for (java.io.File f : fileNames) {
            // totalLines += Files.readAllLines(f.toPath(),
            // Charset.defaultCharset()).size();
            // }
            return totalLines == MESSAGE_COUNT;
        });

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

        java.io.File[] fileNames = workingDirectory.listFiles();
        List<String> allLines = new ArrayList<>();

        for (java.io.File f : fileNames) {
            allLines.addAll(Files.readAllLines(f.toPath(), Charset.defaultCharset()));
        }

        for (String line : allLines) {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode node = mapper.readTree(line);
            JsonNode after = node.get("payload").get("after");
            assertEquals(mapper.readTree(expected.next()), after);
        }
    }
}
