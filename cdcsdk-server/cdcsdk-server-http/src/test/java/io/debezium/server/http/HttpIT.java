/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.server.http;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.configureFor;
import static com.github.tomakehurst.wiremock.client.WireMock.getAllServeEvents;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import javax.enterprise.event.Observes;

import org.awaitility.Awaitility;
import org.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.github.tomakehurst.wiremock.stubbing.ServeEvent;
import com.github.tomakehurst.wiremock.verification.LoggedRequest;
import com.google.inject.Inject;
import com.yugabyte.cdcsdk.server.ServerApp;

import io.debezium.server.events.ConnectorCompletedEvent;
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
@QuarkusTestResource(HttpTestResourceLifecycleManager.class)
public class HttpIT {
    @Inject
    ServerApp server;

    private static final int MESSAGE_COUNT = 4;

    {
        Testing.Files.delete(HttpTestConfigSource.OFFSET_STORE_PATH);
        Testing.Files.createTestingFile(HttpTestConfigSource.OFFSET_STORE_PATH);
    }

    void connectorCompleted(@Observes ConnectorCompletedEvent event) throws Exception {
        if (!event.isSuccess()) {
            throw (Exception) event.getError().get();
        }
    }

    @Test
    public void testHttpServer() {
        Testing.Print.enable();

        List<ServeEvent> events = new ArrayList<>();
        configureFor(HttpTestResourceLifecycleManager.getHost(), HttpTestResourceLifecycleManager.getPort());
        stubFor(post("/").willReturn(aResponse().withStatus(200)));

        Awaitility.await().atMost(Duration.ofSeconds(60)).until(() -> {
            events.addAll(getAllServeEvents());

            return events.size() == MESSAGE_COUNT;
        });

        Assertions.assertEquals(MESSAGE_COUNT, events.size());
        LoggedRequest first = events.get(0).getRequest();

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
        JSONObject after = new JSONObject(first.getBodyAsString()).getJSONObject("payload").getJSONObject("after");

        Assertions.assertEquals(1004, after.getInt("id"));
        Assertions.assertEquals("Anne", after.getString("first_name"));
        Assertions.assertEquals("Kretchmar", after.getString("last_name"));
        Assertions.assertEquals("annek@noanswer.org", after.getString("email"));

        for (ServeEvent e : events) {
            LoggedRequest request = e.getRequest();
            JSONObject payload = new JSONObject(request.getBodyAsString()).getJSONObject("payload");
            JSONObject source = payload.getJSONObject("source");
            Assertions.assertEquals("inventory", source.getString("schema"));
            Assertions.assertEquals("customers", source.getString("table"));
        }
    }
}
