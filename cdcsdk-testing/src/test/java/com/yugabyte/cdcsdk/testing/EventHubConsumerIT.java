/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package com.yugabyte.cdcsdk.testing;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventHubConsumerAsyncClient;
import com.yugabyte.cdcsdk.testing.util.CdcsdkTestBase;
import com.yugabyte.cdcsdk.testing.util.UtilStrings;

import reactor.core.Disposable;

/**
 * Release test that verifies inserts, updates and deletes on a YugabyteDB
 * database and
 * writing to Azure Event Hub
 *
 * @author Sumukh Phalgaonkar
 */


public class EventHubConsumerIT extends CdcsdkTestBase {
    private static final Duration OPERATION_TIMEOUT = Duration.ofSeconds(60);
    private static final int NUMBER_OF_EVENTS = 3;
    private static String connectionString;
    private static String hubName;

    @BeforeAll
    public static void beforeClass() throws Exception {
        initializeContainers();

        initHelpers(true, false, false);

    }

    @BeforeEach
    public void beforeEachTest() throws Exception {
        ybHelper.execute(UtilStrings.getCreateTableYBStmt(DEFAULT_TABLE_NAME));

        connectionString = System.getenv("EVENTHUB_CONNECTIONSTRING");
        hubName = System.getenv("EVENTHUB_HUBNAME");

        cdcsdkContainer = TestHelper.getCdcsdkContainerForEventHubSink(ybHelper, "public." + DEFAULT_TABLE_NAME, connectionString, hubName)
                .withCreateContainerCmdModifier(cmd -> cmd.withUser(System.getenv("USERID")));
        cdcsdkContainer.withNetwork(containerNetwork);

        cdcsdkContainer.start();

        assertTrue(cdcsdkContainer.isRunning());

    }

    @AfterEach
    public void dropTable() throws Exception {
        ybHelper.execute(UtilStrings.getDropTableStmt(DEFAULT_TABLE_NAME));
    }

    @Test
    public void automationOfEventHubAssertions() throws Exception {
        CountDownLatch countDownLatch = new CountDownLatch(NUMBER_OF_EVENTS);
        int recordsInserted = 1;
        List<String> expected_data = List.of(
                "{\"id\":0,\"first_name\":\"first_0\",\"last_name\":\"last_0\",\"days_worked\":23.45,\"__deleted\":\"false\"}",
                "{\"id\":0,\"days_worked\":25.0,\"__deleted\":\"false\"}",
                "{\"id\":0,\"__deleted\":\"true\"}");

        List<String> actual_data = new ArrayList<String>();

        EventHubConsumerAsyncClient consumer = new EventHubClientBuilder()
                .connectionString(connectionString)
                .consumerGroup(EventHubClientBuilder.DEFAULT_CONSUMER_GROUP_NAME)
                .buildAsyncConsumerClient();

        Disposable subscription = consumer.receive(false)
                .subscribe(partitionEvent -> {
                    EventData event = partitionEvent.getData();
                    String contents = new String(event.getBody(), UTF_8);
                    actual_data.add(contents);

                    countDownLatch.countDown();
                },
                        error -> {
                            System.err.println("Error occurred while consuming events: " + error);

                            // Count down until 0, so the main thread does not keep waiting for events.
                            while (countDownLatch.getCount() > 0) {
                                countDownLatch.countDown();
                            }
                        }, () -> {
                            System.out.println("Finished reading events.");
                        });

        for (int i = 0; i < recordsInserted; ++i) {
            ybHelper.execute(UtilStrings.getInsertStmt(DEFAULT_TABLE_NAME, i, "first_" + i, "last_" + i, 23.45));
        }
        ybHelper.execute(UtilStrings.getUpdateStmt(DEFAULT_TABLE_NAME, 0, 25));

        ybHelper.execute(UtilStrings.getDeleteStmt(DEFAULT_TABLE_NAME, 0));

        try {
            // We wait for all the events to be received before continuing.
            boolean isSuccessful = countDownLatch.await(OPERATION_TIMEOUT.getSeconds(), TimeUnit.SECONDS);
            if (!isSuccessful) {
                System.err.printf("Did not complete successfully. There are: %s events left.%n",
                        countDownLatch.getCount());
            }
        }
        finally {
            // Dispose and close of all the resources we've created.
            subscription.dispose();
            consumer.close();
        }

        assertTrue(validateRecords(expected_data, actual_data));
    }
}
