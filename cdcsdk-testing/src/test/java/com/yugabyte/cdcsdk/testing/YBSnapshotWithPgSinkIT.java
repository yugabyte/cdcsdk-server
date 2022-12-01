package com.yugabyte.cdcsdk.testing;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.InetAddress;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.time.Duration;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.yugabyte.cdcsdk.testing.util.CdcsdkTestBase;
import com.yugabyte.cdcsdk.testing.util.PgHelper;
import com.yugabyte.cdcsdk.testing.util.UtilStrings;
import com.yugabyte.cdcsdk.testing.util.YBHelper;

import io.debezium.testing.testcontainers.ConnectorConfiguration;

/**
 * Release tests that verify the snapshot functionality of Change data capture (CDC) using the
 * Debezium Connector for YugabyteDB.
 *
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
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

    @Disabled
    @Test
    public void verifyBasicSnapshotStreaming() throws Exception {
        int rowsToBeInserted = 100;
        // Insert data to the source table
        ybHelper.insertBulk(0, rowsToBeInserted);

        // Start the CDCSDKContainer with initial snapshot mode
        cdcsdkContainer = kafkaHelper.getCdcsdkContainer(ybHelper, "public." + DEFAULT_TABLE_NAME, 10, "initial");
        cdcsdkContainer.withNetwork(containerNetwork);
        cdcsdkContainer.start();

        pgHelper.waitTillRecordsAreVerified(rowsToBeInserted, 10000);

        pgHelper.assertRecordCountInPostgres(rowsToBeInserted);
    }

    @Disabled
    @Test
    public void performStreamingAfterSnapshotAndPerformInsUpdDelOps() throws Exception {
        int rowsToBeInserted = 100;
        // Insert data to the source table
        ybHelper.insertBulk(0, rowsToBeInserted);

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

        ybHelper.insertBulk(100, 150);

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

    @Disabled
    @Test
    public void restartCdcsdkServerWhileSnapshotInProgress() throws Exception {
        int rowsToBeInserted = 10000;
        ybHelper.insertBulk(0, rowsToBeInserted);

        // Get the cdcsdk container instance
        cdcsdkContainer = kafkaHelper.getCdcsdkContainer(ybHelper, "public." + DEFAULT_TABLE_NAME, 10, "initial");
        cdcsdkContainer.withNetwork(containerNetwork);
        cdcsdkContainer.start();

        // Wait for at least 500 rows to be replicated across postgres sink and then restart the cdcsdk server
        // Doing the above would effectively mean that the server will take the snapshot again
        // Ignore the exceptions of type SQLException which may be thrown if the table does not exist
        Awaitility.await()
                .atMost(Duration.ofSeconds(20))
                .ignoreExceptionsInstanceOf(SQLException.class)
                .until(() -> {
                    ResultSet rSet = pgHelper.executeAndGetResultSet("SELECT COUNT(*) FROM " + DEFAULT_TABLE_NAME + ";");
                    rSet.next();
                    return rSet.getInt(1) >= 500;
                });

        // Restart the cdcsdk container
        cdcsdkContainer.stop();
        cdcsdkContainer.start();

        // Wait for all the other records to be replicated after the restart
        pgHelper.waitTillRecordsAreVerified(rowsToBeInserted, 30000);

        // Verify the record count once
        pgHelper.assertRecordCountInPostgres(rowsToBeInserted);
    }

    @Disabled
    @Test
    public void restartSnapshotOnceCompleted() throws Exception {
        int rowsToBeInserted = 1000;
        ybHelper.insertBulk(0, rowsToBeInserted);

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

    @Disabled
    @Test
    public void performOpsWhileCdcsdkServerIsStopped() throws Exception {
        int rowsToBeInserted = 10000;
        ybHelper.insertBulk(0, rowsToBeInserted);

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

        ybHelper.insertBulk(-100, 0);

        // When the container is started, it would take a snapshot again
        // Note: this snapshot may contain some rows which were inserted while the cdcsdk server was stopped
        cdcsdkContainer.start();

        Awaitility.await()
                .atMost(Duration.ofSeconds(20))
                .ignoreExceptionsInstanceOf(SQLException.class)
                .until(() -> {
                    ResultSet rSet = pgHelper.executeAndGetResultSet("SELECT COUNT(*) FROM " + DEFAULT_TABLE_NAME + ";");
                    rSet.next();
                    return rSet.getInt(1) >= rowsToBeInserted;
                });

        // Restart the connector with never snapshot mode
        cdcsdkContainer.stop();
        cdcsdkContainer.withEnv("CDCSDK_SOURCE_SNAPSHOT_MODE", "never");
        cdcsdkContainer.start();

        // Wait for records to be replicated
        pgHelper.waitTillRecordsAreVerified(rowsToBeInserted + 100, 10000);

        // Check final record count in postgres
        pgHelper.assertRecordCountInPostgres(rowsToBeInserted + 100 /* records inserted while cdcsdk container was stopped */);
    }

    @Disabled
    @Test
    public void verifySnapshotForAllSupportedDatatypes() throws Exception {
        final String tableName = UtilStrings.ALL_TYPES_TABLE_NAME;

        YBHelper yb = new YBHelper(InetAddress.getLocalHost().getHostAddress(), tableName);
        PgHelper pg = new PgHelper(postgresContainer, tableName);

        yb.execute(UtilStrings.CREATE_ALL_TYPES_TABLE);

        // Register the sink connector
        ConnectorConfiguration config = pg.getJdbcSinkConfiguration(postgresContainer, "id");
        kafkaConnectContainer.registerConnector("all-types-connector", config);

        // Insert data into the table
        int rowsToBeInserted = 50;
        for (int i = 0; i < rowsToBeInserted; ++i) {
            yb.execute(String.format(UtilStrings.INSERT_ALL_TYPES_FORMAT, i));
        }

        cdcsdkContainer = kafkaHelper.getCdcsdkContainer(yb, "public." + tableName, 1, "initial");
        cdcsdkContainer.withNetwork(containerNetwork);
        cdcsdkContainer.start();

        // Wait till the records are replicated across the sink
        pg.waitTillRecordsAreVerified(rowsToBeInserted, 10000);

        // Verify the count of records in the sink
        pg.assertRecordCountInPostgres(rowsToBeInserted);

        // Verify data in postgres
        ResultSet rs = pg.executeAndGetResultSet("SELECT * FROM " + tableName + " ORDER BY id;");

        // Now since all the rows will have the same data, asserting the data of a single row should do
        rs.next();
        DecimalFormat df = new DecimalFormat("###.###");
        assertEquals(0, rs.getInt(1));
        assertEquals(123456, rs.getInt(2));
        assertEquals("11011", rs.getString(3));
        assertEquals("10101", rs.getString(4));
        assertEquals(false, rs.getBoolean(5));
        assertEquals("\\x01", rs.getString(6));
        assertEquals("five5", rs.getString(7));
        assertEquals("sample_text", rs.getString(8));
        assertEquals("10.1.0.0/16", rs.getString(9));
        assertEquals(19047, rs.getInt(10));
        assertEquals(12.345, Double.valueOf(df.format(rs.getDouble(11))));
        assertEquals("127.0.0.1", rs.getString(12));
        assertEquals(2505600000000L, rs.getLong(13));
        assertEquals("{\"a\":\"b\"}", rs.getString(14));
        assertEquals("{\"a\": \"b\"}", rs.getString(15));
        assertEquals("2c:54:91:88:c9:e3", rs.getString(16));
        assertEquals("22:00:5c:03:55:08:01:02", rs.getString(17));
        assertEquals(100.500, Double.valueOf(df.format(rs.getDouble(18))));
        assertEquals(12.345, Double.valueOf(df.format(rs.getDouble(19))));
        assertEquals(32.145, Double.valueOf(df.format(rs.getDouble(20))));
        assertEquals(12, rs.getInt(21));
        assertEquals("[2,10)", rs.getString(22));
        assertEquals("[101,200)", rs.getString(23));
        assertEquals("(10.45,21.32)", rs.getString(24));
        assertEquals("(\"1970-01-01 00:00:00\",\"2000-01-01 12:00:00\")", rs.getString(25));
        assertEquals("(\"2017-07-04 12:30:30+00\",\"2021-07-04 07:00:30+00\")", rs.getString(26));
        assertEquals("[2019-10-08,2021-10-07)", rs.getString(27));
        assertEquals("text to verify behaviour", rs.getString(28));
        assertEquals(46052000, rs.getInt(29));
        assertEquals("06:30:00Z", rs.getString(30));
        assertEquals(1637841600000L, rs.getLong(31));
        assertEquals("2021-11-25T06:30:00Z", rs.getString(32));
        assertEquals("ffffffff-ffff-ffff-ffff-ffffffffffff", rs.getString(33));

        // Drop the created tables
        dropTablesAfterEachTest(tableName);
    }
}
