package com.yugabyte.cdcsdk.testing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.yugabyte.cdcsdk.testing.util.CdcsdkTestBase;
import com.yugabyte.cdcsdk.testing.util.UtilStrings;

import io.debezium.testing.testcontainers.ConnectorConfiguration;

/**
 * Release test that verifies reading of multiple combination of operations from
 * a YugabyteDB database and writing to Kafka and then further to a PostgreSQL
 * sink database
 *
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class MultiOpsPostgresSinkConsumerIT extends CdcsdkTestBase {
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
        // Create table in the YugabyteDB database
        ybHelper.execute(UtilStrings.getCreateTableYBStmt(DEFAULT_TABLE_NAME, 10));

        // Initiate the cdcsdkContainer
        cdcsdkContainer = kafkaHelper.getCdcsdkContainer(ybHelper, "public." + DEFAULT_TABLE_NAME, 10);
        cdcsdkContainer.withNetwork(containerNetwork);
        cdcsdkContainer.start();

        // Register the sink connector
        sinkConfig = pgHelper.getJdbcSinkConfiguration(postgresContainer, "id");
        kafkaConnectContainer.registerConnector(SINK_CONNECTOR_NAME, sinkConfig);
    }

    @AfterEach
    public void afterEachTest() throws Exception {
        // Stop the cdcsdkContainer so that it doesn't cause any unexpected crashes
        cdcsdkContainer.stop();

        // Delete the sink connector from the Kafka Connect container
        kafkaConnectContainer.deleteConnector(SINK_CONNECTOR_NAME);

        // Delete the topic in Kafka Container
        kafkaHelper.deleteTopicInKafka(pgHelper.getKafkaTopicName());

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
    public void verifyInsertUpdateDeleteOps() throws Exception {
        // At this point in time, all the containers are up and running properly
        ybHelper.execute(UtilStrings.getInsertStmt(DEFAULT_TABLE_NAME, 1, "Vaibhav", "Kushwaha", 23.456));
        ybHelper.execute("UPDATE " + DEFAULT_TABLE_NAME + " SET last_name='Kush' WHERE id = 1;");
        ybHelper.execute("DELETE FROM " + DEFAULT_TABLE_NAME + " WHERE id = 1;");

        // Wait some time for the table to get created in postgres and for replication
        // to complete
        Awaitility.await()
                .atLeast(Duration.ofMillis(20))
                .atMost(Duration.ofMillis(5000))
                .ignoreExceptionsInstanceOf(SQLException.class)
                .until(() -> pgHelper.verifyRecordCount(0));

        pgHelper.assertRecordCountInPostgres(0);

    }

    @Test
    public void verifyUpdatesBeingPropagatedProperly() throws Exception {
        ybHelper.execute(UtilStrings.getInsertStmt(DEFAULT_TABLE_NAME, 1, "Vaibhav", "Kushwaha", 23.456));
        ybHelper.execute("UPDATE " + DEFAULT_TABLE_NAME + " SET last_name='Kush' WHERE id = 1;");

        // Wait some time for the table to get created in postgres and for replication
        // to complete
        Awaitility.await()
                .atLeast(Duration.ofMillis(20))
                .atMost(Duration.ofMillis(5000))
                .ignoreExceptionsInstanceOf(SQLException.class)
                .until(() -> pgHelper.verifyRecordCount(1));

        ResultSet rs = pgHelper.executeAndGetResultSet("SELECT * FROM " + DEFAULT_TABLE_NAME + ";");
        if (rs.next()) {
            // Expect one row with the updated value of last_name
            assertEquals("Kush", rs.getString("last_name"));
        }
        else {
            // Fail in case no ResultSet is retrieved from the query
            fail();
        }
    }

    @Disabled
    @Test
    public void batchInsertsToVerifyIntentsBeingReadProperly() throws Exception {
        int totalRowsToBeInserted = 50;
        try (Connection conn = ybHelper.getConnection()) {
            Statement st = conn.createStatement();
            int ind = 0;
            while (ind < totalRowsToBeInserted) {
                for (int i = ind; i < ind + 5; ++i) {
                    st.addBatch(UtilStrings.getInsertStmt("test_table", i, "first_" + i, "last_" + i, 23.456));
                }
                st.executeBatch();
                ind += 5;

                // Clear batch for next iteration
                st.clearBatch();
            }
        }
        catch (SQLException e) {
            throw e;
        }

        // Wait for records to be replicated across Postgres
        Awaitility.await()
                .atLeast(Duration.ofMillis(20))
                .atMost(Duration.ofMillis(5000))
                .ignoreExceptionsInstanceOf(SQLException.class)
                .until(() -> pgHelper.verifyRecordCount(totalRowsToBeInserted));

        // Assert for the count of the records
        pgHelper.assertRecordCountInPostgres(totalRowsToBeInserted);

        // Now assert for the values in Postgres
        ResultSet resultSet = pgHelper.executeAndGetResultSet("SELECT * FROM " + DEFAULT_TABLE_NAME + " ORDER BY id;");
        int id = 0;
        while (resultSet.next()) {
            assertValuesInResultSet(resultSet, id, "first_" + id, "last_" + id, 23.456);
            ++id;
        }
    }

    @Test
    public void transactionWithHighOperationCount() throws Exception {
        int rowsToBeInserted = 30000;
        ybHelper.execute(
                String.format("INSERT INTO " + DEFAULT_TABLE_NAME + " VALUES (generate_series(1,%d), 'Vaibhav', 'Kushwaha', 23.456);",
                        rowsToBeInserted));

        // Wait for records to be replicated across Postgres
        Awaitility.await()
                .atLeast(Duration.ofMillis(20))
                .atMost(Duration.ofMillis(70000))
                .ignoreExceptionsInstanceOf(SQLException.class)
                .until(() -> pgHelper.verifyRecordCount(rowsToBeInserted));

        // Assert for the count of the records
        pgHelper.assertRecordCountInPostgres(rowsToBeInserted);

        // Now assert for the values in Postgres
        ResultSet resultSet = pgHelper.executeAndGetResultSet("SELECT * FROM " + DEFAULT_TABLE_NAME + " ORDER BY id;");
        int id = 1;
        while (resultSet.next()) {
            assertValuesInResultSet(resultSet, id, "Vaibhav", "Kushwaha", 23.456);
            ++id;
        }
    }

    private void assertValuesInResultSet(ResultSet rs, int idCol, String firstNameCol, String lastNameCol,
                                         double daysWorkedCol)
            throws SQLException {
        assertEquals(idCol, rs.getInt(1));
        assertEquals(firstNameCol, rs.getString(2));
        assertEquals(lastNameCol, rs.getString(3));
        assertEquals(daysWorkedCol, rs.getDouble(4));
    }
}
