/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package com.yugabyte.cdcsdk.testing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.BindMode;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import com.yugabyte.cdcsdk.testing.util.CdcsdkTestBase;
import com.yugabyte.cdcsdk.testing.util.UtilStrings;

/**
 * Release test that verifies inserts, updates and deletes on a YugabyteDB
 * database and
 * writing to Google Pub/Sub
 *
 * @author Sumukh Phalgaonkar
 */

public class PubSubConsumerIT extends CdcsdkTestBase {
    // We are using project id ="yugabyte";
    private static String projectId = System.getenv("GCLOUD_PROJECT");
    // We are using subscription id"dbserver1.public.test_table-sub";
    private static String subscriptionId = System.getenv("SUBSCRIPTION_ID");

    @BeforeAll
    public static void beforeClass() throws Exception {
        initializeContainers();

        initHelpers(true, false, false);
    }

    @BeforeEach
    public void beforeEachTest() throws Exception {
        ybHelper.execute(UtilStrings.getCreateTableYBStmt(DEFAULT_TABLE_NAME));
    }

    @AfterEach
    public void dropTable() throws Exception {
        ybHelper.execute(UtilStrings.getDropTableStmt(DEFAULT_TABLE_NAME));
    }

    @Test
    public void automationOfPubSubAssertions() throws Exception {

        cdcsdkContainer = TestHelper
                .getCdcsdkContainerForPubSubSink(ybHelper, projectId, "public." + DEFAULT_TABLE_NAME)
                .withCreateContainerCmdModifier(cmd -> cmd.withUser(System.getenv("USERID")));
        cdcsdkContainer.withNetwork(containerNetwork);

        cdcsdkContainer.withFileSystemBind(System.getenv("GOOGLE_APPLICATION_CREDENTIALS"),
                "/tmp/keys/application_default_credentials.json", BindMode.READ_ONLY);
        cdcsdkContainer.addEnv("GOOGLE_APPLICATION_CREDENTIALS", "/tmp/keys/application_default_credentials.json");
        cdcsdkContainer.start();

        assertTrue(cdcsdkContainer.isRunning());

    }

    @AfterEach
    public void dropTable() throws Exception {
        ybHelper.execute(UtilStrings.getDropTableStmt(DEFAULT_TABLE_NAME));
    }

    @Test
    public void automationOfPubSubAssertions() throws Exception {

        int recordsInserted = 1;
        for (int i = 0; i < recordsInserted; ++i) {
            ybHelper.execute(UtilStrings.getInsertStmt(DEFAULT_TABLE_NAME, i, "first_" + i, "last_" + i, 23.45));
        }
        ybHelper.execute(UtilStrings.getUpdateStmt(DEFAULT_TABLE_NAME, 0, 25));

        ybHelper.execute(UtilStrings.getDeleteStmt(DEFAULT_TABLE_NAME, 0));

        List<String> expected_data = List.of(
                "{\"id\":0,\"first_name\":\"first_0\",\"last_name\":\"last_0\",\"days_worked\":23.45,\"__deleted\":\"false\"}",
                "{\"id\":0,\"days_worked\":25.0,\"__deleted\":\"false\"}",
                "{\"id\":0,\"__deleted\":\"true\"}");
        Iterator<String> expected = expected_data.iterator();

        List<String> actual_data = new ArrayList<>();

        ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(projectId, subscriptionId);

        // Instantiate an asynchronous message receiver.
        MessageReceiver receiver = (PubsubMessage message, AckReplyConsumer consumer) -> {
            // Handle incoming message, then ack the received message.
            actual_data.add(message.getData().toStringUtf8());
            consumer.ack();
        };

        Subscriber subscriber = null;
        try {
            subscriber = Subscriber.newBuilder(subscriptionName, receiver).build();
            // Start the subscriber.
            subscriber.startAsync().awaitRunning();
            // Allow the subscriber to run for 30s unless an unrecoverable error occurs.
            subscriber.awaitTerminated(30, TimeUnit.SECONDS);
        }
        catch (TimeoutException timeoutException) {
            // Shut down the subscriber after 30s. Stop receiving messages.
            subscriber.stopAsync();
        }

        assertEquals(expected_data.size(), actual_data.size());

        for (String s : actual_data) {
            assertEquals(s, expected.next());
        }

    }

}
