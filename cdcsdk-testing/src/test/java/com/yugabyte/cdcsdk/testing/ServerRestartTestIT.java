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

import org.apache.kafka.clients.consumer.*;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.yugabyte.cdcsdk.testing.util.CdcsdkTestBase;
import com.yugabyte.cdcsdk.testing.util.IOT;
import com.yugabyte.cdcsdk.testing.util.TestTable;

import io.debezium.testing.testcontainers.*;

/**
 * Release test that verifies basic reading from a YugabyteDB database and
 * writing to Kafka and then further to a PostgreSQL sink database
 *
 * @author Isha Amoncar, Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class ServerRestartTestIT extends CdcsdkTestBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(ServerRestartTestIT.class);
    private KafkaConsumer<String, JsonNode> consumer;
    private static List<Map<String, Object>> expectedDataInKafka = new ArrayList<>();
    private static int recordsToBeInserted = 70;

    private static ConnectorConfiguration connector;

    private static TestTable testTable;

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

        // Start CDCSDK server testcontainer.
        cdcsdkContainer = kafkaHelper.getCdcsdkContainer(ybHelper, "public." + DEFAULT_TABLE_NAME, 1);
        cdcsdkContainer.withNetwork(containerNetwork);
        cdcsdkContainer.start();

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
        // Adding Thread.sleep() here because apparently Awaitility didn't seem to work
        // as expected.
        // TODO Vaibhav: Replace the Thread.sleep() function with Awaitility
        Thread.sleep(10000);

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
        assertEquals(recordsToBeInserted, recordsAsserted);
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
