package com.yugabyte.cdcsdk.testing;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.String;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;

import com.yugabyte.cdcsdk.testing.util.CdcsdkContainer;
import com.yugabyte.cdcsdk.testing.util.CdcsdkTestBase;
import com.yugabyte.cdcsdk.testing.util.IOT;

import io.debezium.testing.testcontainers.*;

/**
 * Release test that verifies basic reading from a YugabyteDB database and
 * writing to Kafka and then further to a PostgreSQL sink database
 *
 * @author Isha Amoncar, Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class ServerRestartTestIT extends CdcsdkTestBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(ServerRestartTestIT.class);
    private static List<Map<String, Object>> expectedDataInKafka = new ArrayList<>();

    private static ConnectorConfiguration connector;

    private static IOT testTable;

    private static String streamId;

    @BeforeAll
    public static void beforeClass() throws Exception {
        initializeContainers();

        // Start test containers.
        kafkaContainer.start();
        kafkaConnectContainer.start();
        postgresContainer.start();
        Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> postgresContainer.isRunning());

        // Initialize all the helpers
        initHelpers();

        // Set JDBC sink connector config.
        connector = pgHelper.getJdbcSinkConfiguration(postgresContainer, "id");
        // setConnectorConfiguration();
        kafkaConnectContainer.registerConnector("test-connector", connector);

        testTable = new IOT(DEFAULT_TABLE_NAME);
        // Assuming that yugabyted is running.
        ybHelper.execute(testTable.getCreateTableYBStmt());

        streamId = ybHelper.getNewDbStreamId(ybHelper.getDatabaseName());

        // Start CDCSDK server testcontainer.
        cdcsdkContainer = new CdcsdkContainer()
                .withDatabaseHostname(ybHelper.getHostName())
                .withMasterPort(String.valueOf(ybHelper.getMasterPort()))
                .withKafkaBootstrapServers(kafkaHelper.getBootstrapServersAlias())
                .withTableIncludeList("public."
                        + DEFAULT_TABLE_NAME)
                .withStreamId(streamId)
                .buildForKafkaSink();

        cdcsdkContainer.withNetwork(containerNetwork).withExposedPorts(8080);
        try {
            cdcsdkContainer.start();
        } catch (Exception e) {
            LOGGER.error(cdcsdkContainer.getLogs());
            throw e;
        }

        ybHelper.execute(testTable.insertStmt());
        ResultSet rs = ybHelper
                .executeAndGetResultSet(String.format("SELECT * FROM %s order by id;", DEFAULT_TABLE_NAME));
        expectedDataInKafka = extracted(rs);

    }

    @AfterAll
    public static void afterClass() throws Exception {
        cdcsdkContainer.stop();
        kafkaConnectContainer.stop();
        postgresContainer.stop();
        kafkaContainer.stop();
        ybHelper.execute(testTable.dropTable());
    }

    @Test
    @Order(1)
    public void verifyRecordsInPostgresFromKafka() throws Exception {
        try {
            waitForRecordsInPG();
        } catch (ConditionTimeoutException exception) {
            // If this exception is thrown then it means the records were not found to be
            // equal within the specified duration. Fail the test at this stage.
            fail();
        }

        ResultSet rs = pgHelper
                .executeAndGetResultSet(String.format("SELECT * FROM %s order by id;", DEFAULT_TABLE_NAME));
        List<Map<String, Object>> postgresRecords = extracted(rs);

        Iterator<Map<String, Object>> it = expectedDataInKafka.iterator();

        int recordsAsserted = 0;
        for (Map<String, Object> postgresRecord : postgresRecords) {
            LOGGER.debug("Postgres record:" + postgresRecord);
            assertEquals(it.next(), postgresRecord);
            ++recordsAsserted;
            if (!it.hasNext()) {
                break;
            }
        }
        assertEquals(expectedDataInKafka.size(), recordsAsserted);
    }

    private void waitForRecordsInPG() throws ConditionTimeoutException {
        Awaitility.await()
                .atLeast(Duration.ofSeconds(3))
                .atMost(Duration.ofSeconds(30))
                .pollDelay(Duration.ofSeconds(3))
                .until(() -> {
                    try {
                        ResultSet rs = pgHelper
                                .executeAndGetResultSet(
                                        String.format("SELECT count(*) FROM %s;", DEFAULT_TABLE_NAME));
                        rs.next();
                        LOGGER.debug("No. of records: {}", rs.getInt(1));
                        return rs.getInt(1) == expectedDataInKafka.size();
                    } catch (Exception e) {
                        LOGGER.error(e.getMessage());
                        return false;
                    }
                });
    }

    @Test
    @Order(2)
    public void verifyRecordsAfterRestart() throws Exception {
        cdcsdkContainer.stop();
        ybHelper.execute(testTable.insertStmt("2022-07-02"));
        ResultSet rs_expected = ybHelper
                .executeAndGetResultSet(String.format("SELECT * FROM %s order by id;", DEFAULT_TABLE_NAME));
        expectedDataInKafka = extracted(rs_expected);

        GenericContainer<?> restartContainer = new CdcsdkContainer()
                .withDatabaseHostname(ybHelper.getHostName())
                .withMasterPort(String.valueOf(ybHelper.getMasterPort()))
                .withKafkaBootstrapServers(kafkaHelper.getBootstrapServersAlias())
                .withTableIncludeList("public."
                        + DEFAULT_TABLE_NAME)
                .withStreamId(streamId)
                .withLogMessageRegex(".*Mapping table.*\\n")
                .buildForKafkaSink();
        restartContainer.withNetwork(containerNetwork);
        try {
            restartContainer.start();
        } catch (Exception e) {
            LOGGER.error(restartContainer.getLogs());
            throw e;
        }

        try {
            waitForRecordsInPG();
        } catch (ConditionTimeoutException exception) {
            // If this exception is thrown then it means the records were not found to be
            // equal within the specified duration. Fail the test at this stage.
            fail();
        }

        ResultSet rs = pgHelper
                .executeAndGetResultSet(String.format("SELECT * FROM %s order by id;", DEFAULT_TABLE_NAME));
        List<Map<String, Object>> postgresRecords = extracted(rs);

        Iterator<Map<String, Object>> it = expectedDataInKafka.iterator();

        int recordsAsserted = 0;
        for (Map<String, Object> postgresRecord : postgresRecords) {
            LOGGER.debug("Postgres record:" + postgresRecord);
            assertEquals(it.next(), postgresRecord);
            ++recordsAsserted;
            if (!it.hasNext()) {
                break;
            }
        }
        assertEquals(expectedDataInKafka.size(), recordsAsserted);
    }

    private static List<Map<String, Object>> extracted(ResultSet rs) throws SQLException {
        List<Map<String, Object>> records = new ArrayList<>();
        while (rs.next()) {
            Map<String, Object> result = new LinkedHashMap<String, Object>();
            result.put("id", rs.getInt("id"));
            result.put("host_id", rs.getInt("host_id"));
            // result.put("date", rs.getTimestamp("date"));
            result.put("cpu", rs.getDouble("cpu"));
            result.put("tempc", rs.getInt("tempc"));
            records.add(result);
        }
        return records;
    }
}
