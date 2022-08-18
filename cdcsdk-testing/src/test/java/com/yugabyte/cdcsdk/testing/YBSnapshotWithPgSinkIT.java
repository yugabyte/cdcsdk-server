package com.yugabyte.cdcsdk.testing;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.yugabyte.cdcsdk.testing.util.CdcsdkTestBase;
import com.yugabyte.cdcsdk.testing.util.UtilStrings;

import io.debezium.testing.testcontainers.ConnectorConfiguration;

public class YBSnapshotWithPgSinkIT extends CdcsdkTestBase {
    private static final String SINK_CONNECTOR_NAME = "jdbc-sink-pg";

    private static ConnectorConfiguration sinkConfig;

    @BeforeAll
    public static void beforeAll() throws Exception {
        initializeContainers();

        kafkaContainer.start();
        kafkaConnectContainer.start();
        postgresContainer.start();
        Awaitility.await()
                .atMost(Duration.ofSeconds(20))
                .until(() -> postgresContainer.isRunning());

        initHelpers();
    }

    @BeforeEach
    public void beforeEachTest() throws Exception {
        ybHelper.execute(UtilStrings.getCreateTableYBStmt(DEFAULT_TABLE_NAME, 10));

        // Deploy the PG sink connector
        sinkConfig = pgHelper.getJdbcSinkConfiguration(postgresContainer, "id");
        kafkaConnectContainer.registerConnector(SINK_CONNECTOR_NAME, sinkConfig);
    }

    @AfterEach
    public void afterEachTest() throws Exception {
        // Assuming that CDCSDK Server container is started in each test, we will stop/kill it here
        cdcsdkContainer.stop();

        // Deregister the sinkConnector
        kafkaConnectContainer.deleteConnector(SINK_CONNECTOR_NAME);

        // Drop the source and sink tables now
        dropTablesAfterEachTest(DEFAULT_TABLE_NAME);

        // Delete the topic in Kafka as well so that it does not contain irrelevant data for other tests
        kafkaHelper.deleteTopicInKafka(ybHelper.getKafkaTopicName());
    }

    @AfterAll
    public static void afterAll() throws Exception {
        // Stop the running containers
        postgresContainer.stop();
        kafkaConnectContainer.stop();
        kafkaContainer.stop();
    }

    @Test
    public void verifyBasicSnapshotStreaming() throws Exception {
        int rowsToBeInserted = 100;
        // Insert data to the source table
        TestHelper.insertRowsInSourceTable(ybHelper, DEFAULT_TABLE_NAME, rowsToBeInserted);

        // Start the CDCSDKContainer with initial snapshot mode
        cdcsdkContainer = kafkaHelper.getCdcsdkContainer(ybHelper, "public." + DEFAULT_TABLE_NAME, 10, "initial");
        cdcsdkContainer.withNetwork(containerNetwork);
        cdcsdkContainer.start();

        pgHelper.waitTillRecordsAreVerified(rowsToBeInserted, 10000);

        pgHelper.assertRecordCountInPostgres(rowsToBeInserted);
    }

    @Test
    public void performStreamingAfterSnapshotAndPerformInsUpdDelOps() throws Exception {
        int rowsToBeInserted = 100;
        // Insert data to the source table
        TestHelper.insertRowsInSourceTable(ybHelper, DEFAULT_TABLE_NAME, rowsToBeInserted);

        // Start the CDCSDKContainer with initial snapshot mode
        cdcsdkContainer = kafkaHelper.getCdcsdkContainer(ybHelper, "public." + DEFAULT_TABLE_NAME, 10, "initial");
        cdcsdkContainer.withNetwork(containerNetwork);
        cdcsdkContainer.start();

        pgHelper.waitTillRecordsAreVerified(rowsToBeInserted, 10000);

        // Now stop the cdcsdk container and start it again with never snapshot mode
        cdcsdkContainer.stop();

        // Change the snapshot mode and start the container
        cdcsdkContainer.withEnv("CDCSDK_SOURCE_SNAPSHOT_MODE", "never");
        cdcsdkContainer.start();

        for (int i = 100; i < 150; ++i) {
            ybHelper.execute(UtilStrings.getInsertStmt(DEFAULT_TABLE_NAME, i, "first_" + i, "last_" + i, 23.45));
        }

        pgHelper.waitTillRecordsAreVerified(150, 10000);
        pgHelper.assertRecordCountInPostgres(150);

        ybHelper.execute("UPDATE " + DEFAULT_TABLE_NAME + " SET first_name = 'Vaibhav' WHERE id >= 100 AND id <= 149;");

        Awaitility.await()
                .atMost(Duration.ofSeconds(5))
                .until(() -> {
                    ResultSet rSet = pgHelper.executeAndGetResultSet("SELECT COUNT(*) FROM test_table WHERE first_name='Vaibhav';");
                    rSet.next();
                    return rSet.getInt(1) == 50;
                });

        // Check the results in Postgres
        ResultSet rs = pgHelper.executeAndGetResultSet(String.format("SELECT * FROM %s WHERE id >= 100 and id <= 149 ORDER BY id;", DEFAULT_TABLE_NAME));
        int ind = 100;
        while (rs.next()) {
            TestHelper.assertValuesInResultSet(rs, ind, "Vaibhav", "last_" + ind, 23.45);
            ++ind;
        }

        ybHelper.execute("DELETE FROM " + DEFAULT_TABLE_NAME + ";");

        // Wait till all the records are deleted from the sink
        pgHelper.waitTillRecordsAreVerified(0, 10000);
    }

    @Test
    public void restartCdcsdkServerWhileSnapshotInProgress() throws Exception {
        int rowsToBeInserted = 500;
        TestHelper.insertRowsInSourceTable(ybHelper, DEFAULT_TABLE_NAME, rowsToBeInserted);

        // Get the cdcsdk container instance
        cdcsdkContainer = kafkaHelper.getCdcsdkContainer(ybHelper, "public." + DEFAULT_TABLE_NAME, 10, "initial");
        cdcsdkContainer.withNetwork(containerNetwork);
        cdcsdkContainer.start();

        // Wait for at least 1000 rows to be replicated across postgres sink and then restart the cdcsdk server
        // Doing the above would effectively mean that the server will take the snapshot again
        // Ignore the exceptions of type SQLException which may be thrown if the table does not exist
        Awaitility.await()
                .atMost(Duration.ofSeconds(20))
                .ignoreExceptionsInstanceOf(SQLException.class)
                .until(() -> {
                    ResultSet rSet = pgHelper.executeAndGetResultSet("SELECT COUNT(*) FROM " + DEFAULT_TABLE_NAME + ";");
                    rSet.next();
                    return rSet.getInt(1) >= 100;
                });

        // Restart the cdcsdk container
        cdcsdkContainer.stop();
        cdcsdkContainer.start();

        // Wait for all the other records to be replicated after the restart
        pgHelper.waitTillRecordsAreVerified(rowsToBeInserted, 30000);

        // Verify the record count once
        pgHelper.assertRecordCountInPostgres(rowsToBeInserted);
    }

    @Test
    public void restartSnapshotOnceCompleted() throws Exception {
        int rowsToBeInserted = 100;
        TestHelper.insertRowsInSourceTable(ybHelper, DEFAULT_TABLE_NAME, rowsToBeInserted);

        // Start the cdcsdk container
        cdcsdkContainer = kafkaHelper.getCdcsdkContainer(ybHelper, "public." + DEFAULT_TABLE_NAME, 10, "initial");
        cdcsdkContainer.withNetwork(containerNetwork);
        cdcsdkContainer.start();

        // Wait for the rows to be replicated across Postgres
        pgHelper.waitTillRecordsAreVerified(rowsToBeInserted, 10000);

        // Verify the count
        pgHelper.assertRecordCountInPostgres(rowsToBeInserted);

        // Restart the container - the container will take snapshot again
        cdcsdkContainer.stop();
        cdcsdkContainer.start();

        // Check record count in postgres - nothing should change at this step
        pgHelper.assertRecordCountInPostgres(rowsToBeInserted);
    }

    @Test
    public void performOpsWhileCdcsdkServerIsStopped() throws Exception {
        int rowsToBeInserted = 5000;
        TestHelper.insertRowsInSourceTable(ybHelper, DEFAULT_TABLE_NAME, rowsToBeInserted);

        // Start the cdcsdk container
        cdcsdkContainer = kafkaHelper.getCdcsdkContainer(ybHelper, "public." + DEFAULT_TABLE_NAME, 10, "initial");
        cdcsdkContainer.withNetwork(containerNetwork);
        cdcsdkContainer.start();

        // Wait till there are some records in sink so we can kill the cdcsdk container in the middle
        // of the snapshot
        Awaitility.await()
                .atMost(Duration.ofSeconds(20))
                .ignoreExceptionsInstanceOf(SQLException.class)
                .until(() -> {
                    ResultSet rSet = pgHelper.executeAndGetResultSet("SELECT COUNT(*) FROM " + DEFAULT_TABLE_NAME + ";");
                    rSet.next();
                    return rSet.getInt(1) >= 1000;
                });

        // Stop the container and insert some more rows
        cdcsdkContainer.stop();

        for (int i = -100; i < 0; ++i) {
            ybHelper.execute(UtilStrings.getInsertStmt(DEFAULT_TABLE_NAME, i, "first_" + i, "last_" + i, 23.45));
        }

        // When the container is started, it would take a snapshot again
        // TODO: this snapshot may contain some rows which were inserted while the cdcsdk server was stopped
        cdcsdkContainer.start();

        Awaitility.await()
                .atMost(Duration.ofSeconds(20))
                .ignoreExceptionsInstanceOf(SQLException.class)
                .until(() -> {
                    ResultSet rSet = pgHelper.executeAndGetResultSet("SELECT COUNT(*) FROM " + DEFAULT_TABLE_NAME + ";");
                    rSet.next();
                    return rSet.getInt(1) >= rowsToBeInserted;
                });

        // Check final record count in postgres
        pgHelper.assertRecordCountInPostgres(rowsToBeInserted + 100 /* records inserted while cdcsdk container was stopped */);
    }
}
