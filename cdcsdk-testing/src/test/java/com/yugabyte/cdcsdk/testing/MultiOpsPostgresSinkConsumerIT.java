package com.yugabyte.cdcsdk.testing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.net.InetAddress;
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
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;

import com.yugabyte.cdcsdk.testing.util.KafkaHelper;
import com.yugabyte.cdcsdk.testing.util.PgHelper;
import com.yugabyte.cdcsdk.testing.util.TestImages;
import com.yugabyte.cdcsdk.testing.util.UtilStrings;
import com.yugabyte.cdcsdk.testing.util.YBHelper;

import io.debezium.testing.testcontainers.ConnectorConfiguration;
import io.debezium.testing.testcontainers.DebeziumContainer;

/**
 * Release test that verifies reading of multiple combination of operations from
 * a YugabyteDB database and writing to Kafka and then further to a PostgreSQL
 * sink database
 *
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class MultiOpsPostgresSinkConsumerIT {
    private static final String SINK_CONNECTOR_NAME = "jdbc-sink-pg";

    private static KafkaContainer kafkaContainer;
    private static GenericContainer<?> cdcsdkContainer;
    private static DebeziumContainer kafkaConnectContainer;
    private static PostgreSQLContainer<?> pgContainer;

    private static Network containerNetwork;
    private static ConnectorConfiguration sinkConfig;

    // private static String pgContainerIp;
    private static String hostIp;
    // private static String kafkaContainerIp;

    @BeforeAll
    public static void beforeAll() throws Exception {
        containerNetwork = Network.newNetwork();

        kafkaContainer = new KafkaContainer(TestImages.KAFKA)
                .withNetworkAliases("kafka")
                .withNetwork(containerNetwork);

        pgContainer = new PostgreSQLContainer<>(TestImages.POSTGRES)
                .withPassword("postgres")
                .withUsername("postgres")
                .withExposedPorts(5432)
                .withReuse(true)
                .withNetwork(containerNetwork);

        kafkaConnectContainer = new DebeziumContainer(TestImages.KAFKA_CONNECT)
                .withKafka(kafkaContainer)
                .dependsOn(kafkaContainer)
                .withNetwork(containerNetwork);

        kafkaContainer.start();
        kafkaConnectContainer.start();
        pgContainer.start();
        Awaitility.await()
                .atMost(Duration.ofSeconds(20))
                .until(() -> pgContainer.isRunning());

        KafkaHelper.setBootstrapServers(kafkaContainer.getContainerInfo().getNetworkSettings().getNetworks()
                .entrySet().stream().findFirst().get().getValue().getIpAddress() + ":" + kafkaContainer.KAFKA_PORT);
        PgHelper.setHost(pgContainer.getContainerInfo().getNetworkSettings().getNetworks()
                .entrySet().stream().findFirst().get().getValue().getIpAddress());
        hostIp = InetAddress.getLocalHost().getHostAddress();
        YBHelper.setHost(hostIp);

        // Keeping this for now because it is being passed to the cdcsdk server configs
        TestHelper.setHost(hostIp);
        TestHelper.setBootstrapServerForCdcsdkContainer(kafkaContainer.getNetworkAliases().get(0) + ":9092");
    }

    @BeforeEach
    public void beforeEachTest() throws Exception {
        // Create table in the YugabyteDB database
        YBHelper.execute(UtilStrings.getCreateTableYBStmt("test_table", 10));

        // Initiate the cdcsdkContainer
        cdcsdkContainer = TestHelper.getCdcsdkContainerForKafkaSink(10);
        cdcsdkContainer.withNetwork(containerNetwork);
        cdcsdkContainer.start();

        // Register the sink connector
        sinkConfig = PgHelper.getJdbcSinkConfiguration(pgContainer, "dbserver1.public.test_table", "test_table", "id");
        kafkaConnectContainer.registerConnector(SINK_CONNECTOR_NAME, sinkConfig);
    }

    @AfterEach
    public void afterEachTest() throws Exception {
        // Stop the cdcsdkContainer so that it doesn't cause any unexpected crashes
        cdcsdkContainer.stop();

        // Delete the sink connector from the Kafka Connect container
        kafkaConnectContainer.deleteConnector(SINK_CONNECTOR_NAME);

        // Delete the topic in Kafka Container
        KafkaHelper.deleteTopicInKafka("dbserver1.public.test_table");

        // Drop the table in YugabyteDB as well as the sink table in Postgres
        YBHelper.execute(UtilStrings.getDropTableStmt("test_table"));
        PgHelper.execute(UtilStrings.getDropTableStmt("test_table"));
    }

    @AfterAll
    public static void afterAll() throws Exception {
        // Stop the running containers
        pgContainer.stop();
        kafkaConnectContainer.stop();
        kafkaContainer.stop();
    }

    @Test
    public void testInsertUpdateDelete() throws Exception {
        // At this point in time, all the containers are up and running properly
        YBHelper.execute(UtilStrings.getInsertStmt("test_table", 1, "Vaibhav", "Kushwaha", 23.456));
        YBHelper.execute("UPDATE test_table SET last_name='Kush' WHERE id = 1;");
        YBHelper.execute("DELETE FROM test_table WHERE id = 1;");

        // Wait some time for the table to get created in postgres and for replication
        // to complete
        Thread.sleep(5000);

        PgHelper.assertRecordCountInPostgres(0);
    }

    @Test
    public void testUpdateAfterInsert() throws Exception {
        YBHelper.execute(UtilStrings.getInsertStmt("test_table", 1, "Vaibhav", "Kushwaha", 23.456));
        YBHelper.execute("UPDATE test_table SET last_name='Kush' WHERE id = 1;");

        // Wait some time for the table to get created in postgres and for replication
        // to complete
        Thread.sleep(5000);

        ResultSet rs = PgHelper.executeAndGetResultSet("SELECT * FROM test_table;");
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
    public void testBatchInserts() throws Exception {
        int totalRowsToBeInserted = 50;
        try (Connection conn = YBHelper.getConnection()) {
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
        Thread.sleep(5000);

        // Assert for the count of the records
        PgHelper.assertRecordCountInPostgres(totalRowsToBeInserted);

        // Now assert for the values in Postgres
        ResultSet resultSet = PgHelper.executeAndGetResultSet("SELECT * FROM test_table ORDER BY id;");
        int id = 0;
        while (resultSet.next()) {
            assertValuesInResultSet(resultSet, id, "first_" + id, "last_" + id, 23.456);
            ++id;
        }
    }

    @Test
    public void testLargeTransaction() throws Exception {
        int rowsToBeInserted = 30000;
        YBHelper.execute(
                String.format("INSERT INTO test_table VALUES (generate_series(1,%d), 'Vaibhav', 'Kushwaha', 23.456);",
                        rowsToBeInserted));

        // Wait for records to be replicated across Postgres
        Thread.sleep(70000);

        // Assert for the count of the records
        PgHelper.assertRecordCountInPostgres(rowsToBeInserted);

        // Now assert for the values in Postgres
        ResultSet resultSet = PgHelper.executeAndGetResultSet("SELECT * FROM test_table ORDER BY id;");
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
