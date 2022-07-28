package com.yugabyte.cdcsdk.testing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.net.InetAddress;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Arrays;

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

import io.debezium.testing.testcontainers.ConnectorConfiguration;
import io.debezium.testing.testcontainers.DebeziumContainer;

/**
 * Release test that verifies reading of multiple combination of operations from 
 * a YugabyteDB database and writing to Kafka and then further to a PostgreSQL sink database 
 *
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class MultiOpsPostgresSinkConsumerIT {
    private static final String CREATE_TABLE_STMT = "CREATE TABLE IF NOT EXISTS test_table (id int primary key, first_name varchar(30), last_name varchar(50), days_worked double precision)";
    private static final String DROP_TABLE_STMT = "DROP TABLE test_table";
    private static final String INSERT_FORMAT_STRING = "INSERT INTO test_table VALUES (%d, '%s', '%s', %f);";

    private static final String SINK_CONNECTOR_NAME = "jdbc-sink-pg";

    private static KafkaContainer kafkaContainer;
    private static GenericContainer<?> cdcsdkContainer;
    private static DebeziumContainer kafkaConnectContainer;
    private static PostgreSQLContainer<?> pgContainer;

    private static Network containerNetwork;
    private static ConnectorConfiguration sinkConfig;

    private static String pgContainerIp;
    private static String hostIp;
    private static String kafkaContainerIp;

    @BeforeAll
    public static void beforeAll() throws Exception {
        containerNetwork = Network.newNetwork();

        kafkaContainer = new KafkaContainer(TestHelper.KAFKA_IMAGE)
                .withNetworkAliases("kafka")
                .withNetwork(containerNetwork);

        pgContainer = new PostgreSQLContainer<>(TestHelper.POSTGRES_IMAGE)
                .withPassword("postgres")
                .withUsername("postgres")
                .withExposedPorts(5432)
                .withReuse(true)
                .withNetwork(containerNetwork);

        kafkaConnectContainer = new DebeziumContainer("quay.io/yugabyte/debezium-connector:1.3.7-BETA")
                .withKafka(kafkaContainer)
                .dependsOn(kafkaContainer)
                .withNetwork(containerNetwork);

        kafkaContainer.start();
        kafkaConnectContainer.start();
        pgContainer.start();
        Awaitility.await()
                .atMost(Duration.ofSeconds(20))
                .until(() -> pgContainer.isRunning());

        kafkaContainerIp = kafkaContainer.getContainerInfo().getNetworkSettings().getNetworks()
                .entrySet().stream().findFirst().get().getValue().getIpAddress();
        pgContainerIp = pgContainer.getContainerInfo().getNetworkSettings().getNetworks()
                .entrySet().stream().findFirst().get().getValue().getIpAddress();
        hostIp = InetAddress.getLocalHost().getHostAddress();

        TestHelper.setHost(hostIp);
        TestHelper.setBootstrapServer(kafkaContainer.getNetworkAliases().get(0) + ":9092");
    }

    @BeforeEach
    public void beforeEachTest() throws Exception {
        // Create table in the YugabyteDB database
        TestHelper.execute(CREATE_TABLE_STMT);

        // Initiate the cdcsdkContainer
        cdcsdkContainer = TestHelper.getCdcsdkContainerForKafkaSink();
        cdcsdkContainer.withNetwork(containerNetwork);
        cdcsdkContainer.start();

        // Register the sink connector
        sinkConfig = TestHelper.getPostgresSinkConfiguration(pgContainerIp, pgContainer);
        kafkaConnectContainer.registerConnector(SINK_CONNECTOR_NAME, sinkConfig);
    }

    @AfterEach
    public void afterEachTest() throws Exception {
        // Stop the cdcsdkContainer so that it doesn't cause any unexpected crashes
        cdcsdkContainer.stop();

        // Delete the sink connector from the Kafka Connect container
        kafkaConnectContainer.deleteConnector(SINK_CONNECTOR_NAME);

        // Delete the topic in Kafka Container
        TestHelper.deleteTopicInKafka(kafkaContainerIp, kafkaContainer.KAFKA_PORT,
                Arrays.asList("dbserver1.public.test_table"));

        // Drop the table in YugabyteDB as well as the sink table in Postgres
        TestHelper.execute(DROP_TABLE_STMT);

        // Using the same drop table statement since the table name is the same for both YugabyteDB
        // and Postgres
        TestHelper.executeInPostgres(pgContainerIp, DROP_TABLE_STMT);
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
        TestHelper.execute(String.format(INSERT_FORMAT_STRING, 1, "Vaibhav", "Kushwaha", 23.456));
        TestHelper.execute("UPDATE test_table SET last_name='Kush' WHERE id = 1;");
        TestHelper.execute("DELETE FROM test_table WHERE id = 1;");

        // Wait some time for the table to get created in postgres and for replication to complete
        Thread.sleep(5000);

        boolean isCountInPostgresZero = false;
        ResultSet rs = TestHelper.executeAndGetResultSetPostgres(pgContainerIp, "SELECT COUNT(*) FROM test_table;");
        if (rs.next() && rs.getInt(1) == 0) {
            isCountInPostgresZero = true;
        }
        assertTrue(isCountInPostgresZero);
    }

    @Test
    public void testUpdateAfterInsert() throws Exception {
        TestHelper.execute(String.format(INSERT_FORMAT_STRING, 1, "Vaibhav", "Kushwaha", 23.456));
        TestHelper.execute("UPDATE test_table SET last_name='Kush' WHERE id = 1;");

        // Wait some time for the table to get created in postgres and for replication to complete
        Thread.sleep(5000);

        ResultSet rs = TestHelper.executeAndGetResultSetPostgres(pgContainerIp,
                "SELECT * FROM test_table;");
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
        try (Connection conn = TestHelper.getConnectionOnYugabyteDB()) {
            Statement st = conn.createStatement();
            int ind = 0;
            while (ind < totalRowsToBeInserted) {
                for (int i = ind; i < ind + 5; ++i) {
                    st.addBatch(String.format(INSERT_FORMAT_STRING, i, "first_" + i, "last_" + i, 23.456));
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
        TestHelper.assertRecordCountInPostgres(totalRowsToBeInserted, pgContainerIp);

        // Now assert for the values in Postgres
        ResultSet resultSet = TestHelper.executeAndGetResultSetPostgres(pgContainerIp, "SELECT * FROM test_table ORDER BY id;");
        int id = 0;
        while (resultSet.next()) {
            assertValuesInResultSet(resultSet, id, "first_" + id, "last_" + id, 23.456);
            ++id;
        }
    }

    @Test
    public void testLargeTransaction() throws Exception {
        int rowsToBeInserted = 30000;
        TestHelper.execute(String.format("INSERT INTO test_table VALUES (generate_series(1,%d), 'Vaibhav', 'Kushwaha', 23.456);", rowsToBeInserted));

        // Wait for records to be replicated across Postgres
        Thread.sleep(70000);

        // Assert for the count of the records
        TestHelper.assertRecordCountInPostgres(rowsToBeInserted, pgContainerIp);

        // Now assert for the values in Postgres
        ResultSet resultSet = TestHelper.executeAndGetResultSetPostgres(pgContainerIp, "SELECT * FROM test_table ORDER BY id;");
        int id = 1;
        while (resultSet.next()) {
            assertValuesInResultSet(resultSet, id, "Vaibhav", "Kushwaha", 23.456);
            ++id;
        }
    }

    private void assertValuesInResultSet(ResultSet rs, int idCol, String firstNameCol, String lastNameCol, double daysWorkedCol) throws SQLException {
        assertEquals(idCol, rs.getInt(1));
        assertEquals(firstNameCol, rs.getString(2));
        assertEquals(lastNameCol, rs.getString(3));
        assertEquals(daysWorkedCol, rs.getDouble(4));
    }
}
