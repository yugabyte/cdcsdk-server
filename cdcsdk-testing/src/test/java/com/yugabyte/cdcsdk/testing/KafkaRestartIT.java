package com.yugabyte.cdcsdk.testing;

import java.time.Duration;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.yugabyte.cdcsdk.testing.util.CdcsdkTestBase;
import com.yugabyte.cdcsdk.testing.util.UtilStrings;

import io.debezium.testing.testcontainers.ConnectorConfiguration;

/**
 * Release tests that verify the integrity of the pipeline in case Kafka restarts or goes down
 * for any reason
 * 
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class KafkaRestartIT extends CdcsdkTestBase {
    private static final String SINK_CONNECTOR_NAME = "jdbc-sink";

    private static final String CONNECTOR_URI = "http://localhost:%d/connectors/";

    private static ConnectorConfiguration sinkConfig;

    @BeforeAll
    public static void beforeAll() throws Exception {
        initializeContainers();

        kafkaConnectContainer.withEnv("CONNECT_SESSION_TIMEOUT_MS", "120000");
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
        // Create table in source database
        ybHelper.execute(UtilStrings.getCreateTableYBStmt(DEFAULT_TABLE_NAME, 10));

        // Start the cdcsdk container
        cdcsdkContainer = kafkaHelper.getCdcsdkContainer(ybHelper, "public." + DEFAULT_TABLE_NAME, 10);
        cdcsdkContainer.withNetwork(containerNetwork);
        cdcsdkContainer.start();

        // Register the sink connector
        sinkConfig = pgHelper.getJdbcSinkConfiguration(postgresContainer, "id");
        int mappedPort = TestHelper.getContainerMappedPortFor(kafkaConnectContainer, 8083);
        TestHelper.registerConnector(String.format(CONNECTOR_URI, mappedPort), SINK_CONNECTOR_NAME, sinkConfig);
    }

    @AfterEach
    public void afterEachTest() throws Exception {
        // Stop the cdcsdk container to prevent further unexpected behaviour
        cdcsdkContainer.stop();

        // Delete the sink connector
        TestHelper.deleteConnector(String.format(CONNECTOR_URI, TestHelper.getContainerMappedPortFor(kafkaConnectContainer, 8083)) + SINK_CONNECTOR_NAME);

        // Delete the Kafka topic so that it can be again created/used by the next test
        kafkaHelper.deleteTopicInKafka(pgHelper.getKafkaTopicName());

        // Drop the tables
        dropTablesAfterEachTest(DEFAULT_TABLE_NAME);
    }

    @AfterAll
    public static void afterAll() throws Exception {
        // Stop the running containers
        postgresContainer.stop();
        kafkaConnectContainer.stop();
        kafkaContainer.stop();
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    public void restartKafkaAssociatedContainersAndValidatePipelineIntegrity(boolean restartKafka) throws Exception {
        int rowsToBeInserted = 5;
        for (int i = 0; i < rowsToBeInserted; ++i) {
            ybHelper.execute(UtilStrings.getInsertStmt(DEFAULT_TABLE_NAME, i, "first_" + i, "last_" + i, 23.45));
        }

        // Wait for records to be replicated across Postgres sink
        pgHelper.waitTillRecordsAreVerified(rowsToBeInserted, 10000);

        // If restartKafka flag is true, then we will restart the kafkaContainer but in case it is
        // false, we will restart the kafkaConnectContainer
        if (restartKafka) {
            restartKafkaContainer();
        }
        else {
            restartKafkaConnectContainer();
        }

        // Insert some more records after the container is restarted
        for (int i = 5; i < 15; ++i) {
            ybHelper.execute(UtilStrings.getInsertStmt(DEFAULT_TABLE_NAME, i, "first_" + i, "last_" + i, 23.45));
        }

        pgHelper.waitTillRecordsAreVerified(15, 30000);

        // Verify the record count in the sink
        pgHelper.assertRecordCountInPostgres(15);
    }

    @Test
    public void insertRecordsWhileKafkaConnectIsDown() throws Exception {
        int rowsToBeInsertedBeforeStopping = 5;
        ybHelper.insertBulk(0, rowsToBeInsertedBeforeStopping);

        // Wait for records to be replicated across Postgres
        pgHelper.waitTillRecordsAreVerified(rowsToBeInsertedBeforeStopping, 10000);

        // Stop the Kafka Connect
        kafkaConnectContainer.getDockerClient().stopContainerCmd(kafkaConnectContainer.getContainerId()).exec();

        // Insert more records - this will make the total records as 15
        ybHelper.insertBulk(rowsToBeInsertedBeforeStopping, 15);

        kafkaConnectContainer.getDockerClient().startContainerCmd(kafkaConnectContainer.getContainerId()).exec();

        pgHelper.waitTillRecordsAreVerified(15, 30000);
    }

    @Test
    public void insertRecordsWhileKafkaIsDown() throws Exception {
        int rowsToBeInsertedBeforeStopping = 5;
        ybHelper.insertBulk(0, rowsToBeInsertedBeforeStopping);

        // Wait for records to be replicated across Postgres
        pgHelper.waitTillRecordsAreVerified(rowsToBeInsertedBeforeStopping, 10000);

        // Stop the Kafka process
        kafkaContainer.getDockerClient().stopContainerCmd(kafkaContainer.getContainerId()).exec();

        // Insert more records - this will make the total records as 15
        ybHelper.insertBulk(rowsToBeInsertedBeforeStopping, 15);

        // Start Kafka process
        kafkaContainer.getDockerClient().startContainerCmd(kafkaContainer.getContainerId()).exec();

        pgHelper.waitTillRecordsAreVerified(15, 30000);
    }

    /**
     * Helper function to restart the Kafka connect container
     * @throws Exception if the container cannot be stopped or started after stopping
     */
    public void restartKafkaConnectContainer() throws Exception {
        kafkaConnectContainer.getDockerClient().stopContainerCmd(kafkaConnectContainer.getContainerId()).exec();
        kafkaConnectContainer.getDockerClient().startContainerCmd(kafkaConnectContainer.getContainerId()).exec();
    }

    /**
     * Helper function to restart the Kafka container
     * @throws Exception if the container cannot be stopped or started after stopping
     */
    public void restartKafkaContainer() throws Exception {
        kafkaContainer.getDockerClient().stopContainerCmd(kafkaContainer.getContainerId()).exec();
        kafkaContainer.getDockerClient().startContainerCmd(kafkaContainer.getContainerId()).exec();
    }
}
