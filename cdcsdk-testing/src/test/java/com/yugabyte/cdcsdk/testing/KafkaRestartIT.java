package com.yugabyte.cdcsdk.testing;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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

/**
 * Release tests that verify the integrity of the pipeline in case Kafka restarts or goes down
 * for any reason
 * 
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class KafkaRestartIT extends CdcsdkTestBase {
    private static final String SINK_CONNECTOR_NAME = "jdbc-sink";

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
        kafkaConnectContainer.registerConnector(SINK_CONNECTOR_NAME, sinkConfig);
    }

    @AfterEach
    public void afterEachTest() throws Exception {
        // Stop the cdcsdk container to prevent further unexpected behaviour
        cdcsdkContainer.stop();

        // Delete the sink connector
        kafkaConnectContainer.deleteConnector(SINK_CONNECTOR_NAME);

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

    @Test
    public void restartKafkaAndVerifyPipelineIntegrity() throws Exception {
        int rowsToBeInserted = 5;
        for (int i = 0; i < rowsToBeInserted; ++i) {
            ybHelper.execute(UtilStrings.getInsertStmt(DEFAULT_TABLE_NAME, i, "first_" + i, "last_" + i, 23.45));
        }

        // Wait for records to be replicated across Postgres sink
        pgHelper.waitTillRecordsAreVerified(rowsToBeInserted, 10000);

        restartKafkaContainer();

        // Insert some more records after Kafka container is restarted
        for (int i = 5; i < 15; ++i) {
            ybHelper.execute(UtilStrings.getInsertStmt(DEFAULT_TABLE_NAME, i, "first_" + i, "last_" + i, 23.45));
        }

        Path kafkaLogPath = Paths.get("/home/ec2-user/kafka-log.txt");
        Path kafkaConnectLogPath = Paths.get("/home/ec2-user/kafka-connect-log.txt");

        Files.writeString(kafkaLogPath, kafkaContainer.getLogs(), StandardCharsets.UTF_8);
        Files.writeString(kafkaConnectLogPath, kafkaConnectContainer.getLogs(), StandardCharsets.UTF_8);
        System.out.println("Wrote the logs to the respective files");
        pgHelper.waitTillRecordsAreVerified(15, 10000);

        // Verify the record count in the sink
        pgHelper.assertRecordCountInPostgres(15);
    }

    /**
     * Helper function to restart the Kafka container
     * @throws Exception if the container cannot be stopped or started after stopping
     */
    public void restartKafkaContainer() throws Exception {
        kafkaContainer.stop();
        kafkaContainer.start();
    }

}