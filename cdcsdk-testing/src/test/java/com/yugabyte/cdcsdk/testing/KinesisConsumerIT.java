/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package com.yugabyte.cdcsdk.testing;

import static org.junit.jupiter.api.Assertions.*;

import java.io.UnsupportedEncodingException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.BindMode;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import com.yugabyte.cdcsdk.testing.util.CdcsdkTestBase;
import com.yugabyte.cdcsdk.testing.util.UtilStrings;

/**
 * Release test that verifies inserts, updates and deletes on a YugabyteDB
 * database and
 * writing to Amazon Kinesis
 *
 * @author Sumukh Phalgaonkar
 */

public class KinesisConsumerIT extends CdcsdkTestBase {

    private static String stream_name = "dbserver1.public.test_table";

    @BeforeAll
    public static void beforeClass() throws Exception {
        initializeContainers();

        initHelpers(true, false, false);
    }

    @BeforeEach
    public void beforeEachTest() throws Exception {
        ybHelper.execute(UtilStrings.getCreateTableYBStmt(DEFAULT_TABLE_NAME));

        cdcsdkContainer = TestHelper.getCdcsdkContainerForKinesisSink(ybHelper, "public." + DEFAULT_TABLE_NAME)
                .withCreateContainerCmdModifier(cmd -> cmd.withUser(System.getenv("USERID")));
        cdcsdkContainer.withNetwork(containerNetwork);

        cdcsdkContainer.withFileSystemBind(System.getenv("AWS_SHARED_CREDENTIALS_FILE"),
                "/home/jboss/.aws/credentials", BindMode.READ_ONLY);
        cdcsdkContainer.start();

        assertTrue(cdcsdkContainer.isRunning());

    }

    @AfterEach
    public void dropTable() throws Exception {
        ybHelper.execute(UtilStrings.getDropTableStmt(DEFAULT_TABLE_NAME));
    }

    @Test
    public void automationOfKinesisAssertions() throws Exception {

        int recordsInserted = 1;
        String uuid = getUUID().substring(0, 8);
        for (int i = 0; i < recordsInserted; ++i) {
            ybHelper.execute(UtilStrings.getInsertStmt(DEFAULT_TABLE_NAME, i, uuid, "last_" + i, 23.45));
        }
        ybHelper.execute(UtilStrings.getUpdateStmt(DEFAULT_TABLE_NAME, 0, 25));

        ybHelper.execute(UtilStrings.getDeleteStmt(DEFAULT_TABLE_NAME, 0));

        Awaitility.await().atMost(Duration.ofSeconds(10));

        List<String> expected_data = List.of(
                "{\"id\":0,\"first_name\":\"" + uuid
                        + "\",\"last_name\":\"last_0\",\"days_worked\":23.45,\"__deleted\":\"false\"}",
                "{\"id\":0,\"days_worked\":25.0,\"__deleted\":\"false\"}",
                "{\"id\":0,\"__deleted\":\"true\"}");

        List<String> actual_data = new ArrayList<>();

        AWSCredentials awsCredentials = new BasicSessionCredentials(System.getenv("AWS_ACCESS_KEY_ID"),
                System.getenv("AWS_SECRET_ACCESS_KEY"),
                System.getenv("AWS_SESSION_TOKEN"));

        AmazonKinesisClient client = new AmazonKinesisClient(awsCredentials);

        client.setEndpoint("https://kinesis.ap-south-1.amazonaws.com/");

        final int INTERVAL = 2000;
        ;

        List<Shard> initialShardData = client.describeStream(stream_name).getStreamDescription().getShards();

        // Getting shardIterators (at beginning sequence number) for reach shard
        List<String> initialShardIterators = initialShardData.stream()
                .map(s -> client.getShardIterator(new GetShardIteratorRequest()
                        .withStreamName(stream_name)
                        .withShardId(s.getShardId())
                        .withStartingSequenceNumber(s.getSequenceNumberRange().getStartingSequenceNumber())
                        .withShardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER))
                        .getShardIterator())
                .collect(Collectors.toList());

        String shardIterator = initialShardIterators.get(0);

        // Continuously read data records from a shard
        while (true) {
            GetRecordsRequest getRecordsRequest = new GetRecordsRequest();
            getRecordsRequest.setShardIterator(shardIterator);
            getRecordsRequest.setLimit(25);

            GetRecordsResult recordResult = client.getRecords(getRecordsRequest);

            recordResult.getRecords().forEach(record -> {
                try {
                    String rec = new String(record.getData().array(), "UTF-8");
                    actual_data.add(rec);
                }
                catch (UnsupportedEncodingException e) {
                    System.out.println("Could not decode message from Kinesis stream result");
                    e.printStackTrace();
                }
            });

            if (recordResult.getRecords().size() == 0)
                break;
            try {
                Thread.sleep(INTERVAL);
            }
            catch (InterruptedException exception) {
                System.out.println("Receving InterruptedException. Exiting ...");
                return;
            }
            shardIterator = recordResult.getNextShardIterator();
        }

        assertTrue(validateRecords(expected_data, actual_data));

    }

    public String getUUID() {
        UUID uuid = UUID.randomUUID();
        return uuid.toString();
    }

    public Boolean validateRecords(List<String> expectedData, List<String> actualData) {
        Boolean result = true;
        for (String expected : expectedData) {
            Boolean match = false;
            for (String actual : actualData) {
                match = expected.equals(actual);
                if (match)
                    break;
            }
            result = result && match;
        }
        return result;
    }

}
