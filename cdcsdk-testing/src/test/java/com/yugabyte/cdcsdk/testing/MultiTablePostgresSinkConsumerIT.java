package com.yugabyte.cdcsdk.testing;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.InetAddress;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;

import com.yugabyte.cdcsdk.testing.util.CdcsdkTestBase;
import com.yugabyte.cdcsdk.testing.util.KafkaHelper;
import com.yugabyte.cdcsdk.testing.util.PgHelper;
import com.yugabyte.cdcsdk.testing.util.UtilStrings;
import com.yugabyte.cdcsdk.testing.util.YBHelper;

import io.debezium.testing.testcontainers.ConnectorConfiguration;

/**
 * Release test that verifies reading of multiple combination of operations from
 * a YugabyteDB database and writing to Kafka and then further to a PostgreSQL
 * sink database
 *
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class MultiTablePostgresSinkConsumerIT extends CdcsdkTestBase {
    private static final String CONNECTOR_1 = "jdbc-sink-1";
    private static final String CONNECTOR_2 = "jdbc-sink-2";

    private static final String TABLE_1 = "test_table_1";
    private static final String TABLE_2 = "test_table_2";

    private static ConnectorConfiguration sinkConfig1;
    private static ConnectorConfiguration sinkConfig2;

    private static YBHelper ybHelper2;
    private static PgHelper pgHelper2;

    @BeforeAll
    public static void beforeAll() throws Exception {
        initializeContainers();

        kafkaContainer.start();
        kafkaConnectContainer.start();
        postgresContainer.start();
        Awaitility.await()
                .atMost(Duration.ofSeconds(20))
                .until(() -> postgresContainer.isRunning());

        kafkaHelper = new KafkaHelper(kafkaContainer.getNetworkAliases().get(0) + ":9092",
                kafkaContainer.getContainerInfo().getNetworkSettings().getNetworks()
                        .entrySet().stream().findFirst().get().getValue().getIpAddress() + ":" + KafkaContainer.KAFKA_PORT);

        ybHelper = new YBHelper(InetAddress.getLocalHost().getHostAddress(), TABLE_1);
        ybHelper2 = new YBHelper(InetAddress.getLocalHost().getHostAddress(), TABLE_2);
        pgHelper = new PgHelper(postgresContainer, TABLE_1);
        pgHelper2 = new PgHelper(postgresContainer, TABLE_2);
    }

    @BeforeEach
    public void beforeEachTest() throws Exception {
        // Create table in the YugabyteDB database
        ybHelper.execute(UtilStrings.getCreateTableYBStmt(TABLE_1, 10));

        // Create another table with a composite primary key - id,first_name
        String CREATE_STMT = "CREATE TABLE %s (id INT, first_name VARCHAR(30), last_name VARCHAR(50), days_worked DOUBLE PRECISION, PRIMARY KEY (id,first_name)) SPLIT INTO 10 TABLETS;";
        ybHelper.execute(String.format(CREATE_STMT, TABLE_2));

        // Initiate the cdcsdkContainer
        // There will be total 20 tablets so the log line will come 20 times
        cdcsdkContainer = kafkaHelper.getCdcsdkContainer(ybHelper, createTableIncludeListWithTableName(Arrays.asList(TABLE_1, TABLE_2)), 20);
        cdcsdkContainer.withNetwork(containerNetwork);
        cdcsdkContainer.start();

        // Register the sink connectors
        sinkConfig1 = pgHelper.getJdbcSinkConfiguration(postgresContainer, "id");
        sinkConfig2 = pgHelper2.getJdbcSinkConfiguration(postgresContainer, "id,first_name");
        kafkaConnectContainer.registerConnector(CONNECTOR_1, sinkConfig1);
        kafkaConnectContainer.registerConnector(CONNECTOR_2, sinkConfig2);
    }

    @AfterEach
    public void afterEachTest() throws Exception {
        // Stop the cdcsdkContainer so that it doesn't cause any unexpected crashes
        cdcsdkContainer.stop();

        // Delete the sink connector from the Kafka Connect container
        kafkaConnectContainer.deleteConnector(CONNECTOR_1);
        kafkaConnectContainer.deleteConnector(CONNECTOR_2);

        // Delete the topic in Kafka Container
        kafkaHelper.deleteTopicInKafka(ybHelper.getKafkaTopicName());
        kafkaHelper.deleteTopicInKafka(ybHelper2.getKafkaTopicName());

        // TODO: Drop tables
        dropTablesAfterEachTest(TABLE_1);
        dropTablesAfterEachTest(TABLE_2);
    }

    @AfterAll
    public static void afterAll() throws Exception {
        // Stop the running containers
        postgresContainer.stop();
        kafkaConnectContainer.stop();
        kafkaContainer.stop();
    }

    @Test
    public void insertDataInBothSourceTablesAlternatively() throws Exception {
        int recordsToBeInserted = 10;
        for (int i = 1; i <= recordsToBeInserted; ++i) {
            ybHelper.execute(UtilStrings.getInsertStmt(TABLE_1, i, "first_" + i, "last_" + i, 23.45));

            int j = (-1) * i;
            ybHelper2.execute(UtilStrings.getInsertStmt(TABLE_2, j, "first_" + j, "last_" + j, 123.45));
        }

        // Wait for records to be reflected across postgres
        Thread.sleep(5000);

        pgHelper.assertRecordCountInPostgres(recordsToBeInserted);
        pgHelper2.assertRecordCountInPostgres(recordsToBeInserted);
    }

    private void assertValuesInResultSet(ResultSet rs, int idCol, String firstNameCol, String lastNameCol,
                                         double daysWorkedCol)
            throws SQLException {
        assertEquals(idCol, rs.getInt(1));
        assertEquals(firstNameCol, rs.getString(2));
        assertEquals(lastNameCol, rs.getString(3));
        assertEquals(daysWorkedCol, rs.getDouble(4));
    }

    private String createTableIncludeListWithTableName(List<String> tableNames) {
        String res = "";
        for (String tableName : tableNames) {
            res += "public." + tableName + ",";
        }

        // Remove the last character - the unintended comma (,) - and return the list
        return res.substring(0, res.length() - 1);
    }
}
