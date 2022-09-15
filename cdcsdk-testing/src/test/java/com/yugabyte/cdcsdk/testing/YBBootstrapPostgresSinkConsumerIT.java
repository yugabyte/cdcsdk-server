package com.yugabyte.cdcsdk.testing;

import static org.junit.jupiter.api.Assertions.assertEquals;

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

/**
 * Release test that verifies reading of multiple combination of operations from 
 * a YugabyteDB database and writing to Kafka and then further to a PostgreSQL sink database 
 *
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class YBBootstrapPostgresSinkConsumerIT extends CdcsdkTestBase {
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
        // Register the sink connector
        sinkConfig = pgHelper.getJdbcSinkConfiguration(postgresContainer, "id");
        kafkaConnectContainer.registerConnector(SINK_CONNECTOR_NAME, sinkConfig);
    }

    @AfterEach
    public void afterEachTest() throws Exception {
        // Delete the sink connector from the Kafka Connect container
        kafkaConnectContainer.deleteConnector(SINK_CONNECTOR_NAME);

        // Delete the topic in Kafka Container
        kafkaHelper.deleteTopicInKafka(ybHelper.getKafkaTopicName());

        // Drop the table in YugabyteDB as well as the sink table in Postgres
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
    public void testSourceBootstrap() throws Exception {
        ybHelper.execute(UtilStrings.getCreateTableYBStmt(DEFAULT_TABLE_NAME));

        // Insert some records before starting the CDCSDK container
        int recordsInsertedBeforeBootstrap = 1000;
        for (int i = 0; i < recordsInsertedBeforeBootstrap; ++i) {
            ybHelper.execute(UtilStrings.getInsertStmt(DEFAULT_TABLE_NAME, i, "first_" + i, "last_" + i, 23.45));
        }

        // Starting the container would now bootstrap the tablets so only the new changes
        // will be propagated
        cdcsdkContainer = kafkaHelper.getCdcsdkContainer(ybHelper, "public." + DEFAULT_TABLE_NAME, 1);
        cdcsdkContainer.withNetwork(containerNetwork);
        cdcsdkContainer.start();

        // Insert more records --> [1000, 1499)
        int recordsInsertedAfterBootstrap = 500;
        for (int i = recordsInsertedBeforeBootstrap; i < recordsInsertedBeforeBootstrap + recordsInsertedAfterBootstrap; ++i) {
            ybHelper.execute(UtilStrings.getInsertStmt(DEFAULT_TABLE_NAME, i, "first_" + i, "last_" + i, 23.45));
        }

        // Wait for records to be replicated across Postgres
        pgHelper.waitTillRecordsAreVerified(recordsInsertedAfterBootstrap, 5000);

        // Assert for record count
        pgHelper.assertRecordCountInPostgres(recordsInsertedAfterBootstrap);

        // Verify the data in Postgres
        ResultSet resultSet = pgHelper.executeAndGetResultSet(String.format("SELECT * FROM %s ORDER BY id;", DEFAULT_TABLE_NAME));

        // The records' id will be starting from 1000
        int ind = 1000;
        while (resultSet.next()) {
            assertValuesInResultSet(resultSet, ind, "first_" + ind, "last_" + ind, 23.45);
            ++ind;
        }

        // Stop the cdcsdkContainer so that it doesn't cause any unexpected crashes
        cdcsdkContainer.stop();
    }

    private void assertValuesInResultSet(ResultSet rs, int idCol, String firstNameCol, String lastNameCol, double daysWorkedCol) throws SQLException {
        assertEquals(idCol, rs.getInt(1));
        assertEquals(firstNameCol, rs.getString(2));
        assertEquals(lastNameCol, rs.getString(3));
        assertEquals(daysWorkedCol, rs.getDouble(4));
    }
}
