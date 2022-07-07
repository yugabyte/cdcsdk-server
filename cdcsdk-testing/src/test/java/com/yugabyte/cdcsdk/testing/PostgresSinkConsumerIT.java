package com.yugabyte.cdcsdk.testing;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.Double;
import java.lang.Integer;
import java.lang.String;
import java.net.InetAddress;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.*;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.YugabyteYSQLContainer;
import org.testcontainers.utility.DockerImageName;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.testing.testcontainers.*;

public class PostgresSinkConsumerIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresSinkConsumerIT.class);
    private static String HOST_ADDRESS = "127.0.0.1";
    protected static String BOOTSTRAP_SERVER = "127.0.0.1:9092";
    private KafkaConsumer<String, JsonNode> consumer;
    private static List<Map<String, Object>> expected_data = new ArrayList<>();
    private static int recordsInserted = 5;
    protected static KafkaContainer kafka;
    private static YugabyteYSQLContainer ybContainer;
    private static GenericContainer<?> cdcContainer;
    private static final DockerImageName KAFKA_TEST_IMAGE = DockerImageName.parse("confluentinc/cp-kafka:6.2.1");
    private static Network network;
    private static DockerImageName myImage = DockerImageName.parse("debezium/example-postgres:1.6").asCompatibleSubstituteFor("postgres");
    private static PostgreSQLContainer<?> postgreSQLContainer;
    private static DebeziumContainer kafkaConnectContainer;
    private static String POSTGRES_IP;
    private static ConnectorConfiguration connector;

    @BeforeAll
    public static void beforeClass() throws Exception {
        network = Network.newNetwork();
        kafka = new KafkaContainer(KAFKA_TEST_IMAGE).withNetworkAliases("kafka").withNetwork(network);
        postgreSQLContainer = new PostgreSQLContainer<>(myImage)
                .withNetwork(network)
                .withPassword("postgres")
                .withUsername("postgres")
                .withExposedPorts(5432)
                .withReuse(true);
        // Using the Yugabyte's Kafka Connect image since it comes with a bundled JDBCSinkConnector.
        kafkaConnectContainer = new DebeziumContainer("quay.io/yugabyte/debezium-connector:1.3.7-BETA")
                .withNetwork(network)
                .withKafka(kafka)
                .dependsOn(kafka);

        // Start test containers.
        kafka.start();
        kafkaConnectContainer.start();
        postgreSQLContainer.start();
        Thread.sleep(10000);

        // Get IPs and addresses.
        POSTGRES_IP = postgreSQLContainer.getContainerInfo().getNetworkSettings().getNetworks().entrySet().stream().findFirst().get().getValue().getIpAddress();
        HOST_ADDRESS = InetAddress.getLocalHost().getHostAddress();
        TestHelper.setHost(HOST_ADDRESS);
        BOOTSTRAP_SERVER = kafka.getNetworkAliases().get(0) + ":9092";
        TestHelper.setBootstrapServer(BOOTSTRAP_SERVER);

        // Set JDBC sink connector config.
        setConnectorConfiguration();
        kafkaConnectContainer.registerConnector("test-connector", connector);

        // Assuming that yugabyted is running.
        String createTableSql = "CREATE TABLE IF NOT EXISTS test_table (id int primary key, first_name varchar(30), last_name varchar(50), days_worked double precision)";
        TestHelper.execute(createTableSql);
        String deleteFromTableSql = "DELETE FROM test_table";
        TestHelper.execute(deleteFromTableSql);

        // Initialise expected_data.
        for (int i = 0; i < recordsInserted; i++) {
            Map<String, Object> expected_record = new LinkedHashMap<String, Object>();
            expected_record.put("id", new Integer(i));
            expected_record.put("first_name", new String("first_" + i));
            expected_record.put("last_name", new String("last_" + i));
            expected_record.put("days_worked", new Double(23.45));
            expected_data.add(expected_record);
        }

        // Start CDCSDK server testcontainer.
        cdcContainer = TestHelper.getCdcsdkContainer();
        cdcContainer.withNetwork(network);
        cdcContainer.start();
        Thread.sleep(10000);

        // Insert records in YB.
        for (int i = 0; i < recordsInserted; ++i) {
            String insertSql = String.format("INSERT INTO test_table VALUES (%d, '%s', '%s', %f);", i, "first_" + i,
                    "last_" + i, 23.45);
            TestHelper.execute(insertSql);
        }
    }

    @AfterAll
    public static void afterClass() throws Exception {
        cdcContainer.stop();
        kafkaConnectContainer.stop();
        postgreSQLContainer.stop();
        kafka.stop();
        String dropTableSql = "DROP TABLE test_table";
        TestHelper.execute(dropTableSql);
    }

    @Test
    @Order(1)
    public void testAutomationOfKafkaAssertions() throws Exception {

        // Verify data on Kafka.

        setKafkaConsumerProperties();
        consumer.subscribe(Arrays.asList("dbserver1.public.test_table"));
        long expectedtime = System.currentTimeMillis() + 10000;
        int flag = 0;
        Thread.sleep(5000);
        while (System.currentTimeMillis() < expectedtime) {
            consumer.seekToBeginning(consumer.assignment());
            ConsumerRecords<String, JsonNode> records = consumer.poll(15);
            LOGGER.info("Record count: " + records.count());
            List<Map<String, Object>> kafkaRecords = new ArrayList<>();
            for (ConsumerRecord<String, JsonNode> record : records) {
                ObjectMapper mapper = new ObjectMapper();
                if (record.value() != null) {
                    JsonNode jsonNode = record.value().get("payload");
                    Map<String, Object> result = mapper.convertValue(jsonNode, new TypeReference<Map<String, Object>>() {
                    });
                    kafkaRecords.add(result);
                }
            }
            Iterator<Map<String, Object>> it = expected_data.iterator();
            int recordsAsserted = 0;

            for (Map<String, Object> kafkaRecord : kafkaRecords) {
                LOGGER.info("Kafka record " + kafkaRecord);
                assertEquals(it.next(), kafkaRecord);
                ++recordsAsserted;
                if (recordsAsserted == recordsInserted) {
                    flag = 1;
                    break;
                }
            }
            if (flag == 1) {
                break;
            }
        }
    }

    @Test
    @Order(2)
    public void testAutomationOfPostgresAssertions() throws Exception {

        // Verify data on Postgres.

        Class.forName("org.postgresql.Driver");
        Connection conn = DriverManager.getConnection("jdbc:postgresql://" + POSTGRES_IP + ":5432/postgres", "postgres",
                "postgres");
        Statement stmt = conn.createStatement();
        Thread.sleep(10000);

        ResultSet rs = stmt.executeQuery("select * from sink");
        List<Map<String, Object>> postgresRecords = new ArrayList<>();
        while (rs.next()) {
            Map<String, Object> result = new LinkedHashMap<String, Object>();
            result.put("id", rs.getInt("id"));
            result.put("first_name", rs.getString("first_name"));
            result.put("last_name", rs.getString("last_name"));
            result.put("days_worked", rs.getDouble("days_worked"));
            postgresRecords.add(result);
        }

        Iterator<Map<String, Object>> it = expected_data.iterator();

        int recordsAsserted = 0;
        for (Map<String, Object> postgresRecord : postgresRecords) {
            LOGGER.info("Postgres record:" + postgresRecord);
            assertEquals(it.next(), postgresRecord);
            ++recordsAsserted;
            if (recordsAsserted == recordsInserted) {
                break;
            }
        }

    }

    private void setKafkaConsumerProperties() throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers",
                kafka.getContainerInfo().getNetworkSettings().getNetworks().entrySet().stream().findFirst()
                        .get().getValue().getIpAddress() + ":" + kafka.KAFKA_PORT);
        props.put("group.id", "myapp");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.connect.json.JsonDeserializer");
        consumer = new KafkaConsumer<>(props);
    }

    private static void setConnectorConfiguration() throws Exception {
        Connection connection = DriverManager.getConnection(postgreSQLContainer.getJdbcUrl(),
                postgreSQLContainer.getUsername(),
                postgreSQLContainer.getPassword());

        connector = ConnectorConfiguration
                .forJdbcContainer(postgreSQLContainer)
                .with("connector.class", "io.confluent.connect.jdbc.JdbcSinkConnector")
                .with("tasks.max", "1")
                .with("topics", "dbserver1.public.test_table")
                .with("database.server.name", "dbserver1")
                .with("dialect.name", "PostgreSqlDatabaseDialect")
                .with("table.name.format", "sink")
                .with("connection.url", "jdbc:postgresql://" + POSTGRES_IP + ":5432/postgres?user=postgres&password=postgres&sslMode=require")
                .with("auto.create", "true")
                .with("insert.mode", "upsert")
                .with("pk.fields", "id")
                .with("pk.mode", "record_key")
                .with("delete.enabled", "true")
                .with("auto.evolve", "true")
                .with("value.converter", "org.apache.kafka.connect.json.JsonConverter")
                .with("value.converter.schemas.enable", "true")
                .with("key.converter", "org.apache.kafka.connect.json.JsonConverter")
                .with("key.converter.schemas.enable", "true");
    }
}
