package com.yugabyte.cdcsdk.testing;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetAddress;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicCollection;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;

import com.fasterxml.jackson.databind.JsonNode;

import io.debezium.testing.testcontainers.ConnectorConfiguration;
import io.debezium.testing.testcontainers.DebeziumContainer;

/**
 * Release test that verifies reading of multiple combination of operations from 
 * a YugabyteDB database and writing to Kafka and then further to a PostgreSQL sink database 
 *
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class MultiOpsSinkConsumerIT {
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

    private static Admin kafkaAdmin;
    private static AdminClient adminClient;
    private static KafkaConsumer<String, JsonNode> kafkaConsumer;

    private static String pgContainerIp;
    private static String hostIp;
    private static String kafkaContainerIp;

    @BeforeAll
    public static void beforeAll() throws Exception {
        System.out.println("Starting before all function");
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

        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainerIp + ":9092");

        kafkaAdmin = Admin.create(properties);
        adminClient = KafkaAdminClient.create(properties);

        TestHelper.setHost(hostIp);
        System.out.println("Setting host as: " + hostIp);
        System.out.println("Kafka container IP: " + kafkaContainerIp);
        TestHelper.setBootstrapServer(kafkaContainer.getNetworkAliases().get(0) + ":9092");
    }

    @BeforeEach
    public void beforeEachTest() throws Exception {
        System.out.println("Starting before each function");
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
        System.out.println("starting after each function");
        // Stop the cdcsdkContainer so that it doesn't cause any unexpected crashes
        cdcsdkContainer.stop();

        // Delete the sink connector from the Kafka Connect container
        kafkaConnectContainer.deleteConnector(SINK_CONNECTOR_NAME);

        // // Delete the topic in Kafka Container
        DeleteTopicsResult res = adminClient.deleteTopics(TopicCollection.ofTopicNames(Arrays.asList("dbserver1.public.test_table")));
        Map<String, ?> mp = res.topicNameValues();
        for (Map.Entry<String, ?> element : mp.entrySet()) {
            System.out.println(element.getKey());
        }
        // Drop the table in YugabyteDB as well as the sink table in Postgres
        TestHelper.execute(DROP_TABLE_STMT);

        // Using the same drop table statement since the table name is the same for both YugabyteDB
        // and Postgres
        TestHelper.executeInPostgres(pgContainerIp, DROP_TABLE_STMT);
    }

    @AfterAll
    public static void afterAll() throws Exception {
        System.out.println("Starting after all functions");
        // Stop the running containers
        pgContainer.stop();
        kafkaConnectContainer.stop();
        kafkaContainer.stop();

        System.out.println("Adios amigo...");
    }

    @Test
    public void testInsertUpdateDelete() throws Exception {
        System.out.println("Starting the test now...");
        // At this point in time, all the containers are up and running properly
        TestHelper.execute(String.format(INSERT_FORMAT_STRING, 1, "Vaibhav", "Kushwaha", 23.456));
        TestHelper.execute("UPDATE test_table SET last_name='Kush' WHERE id = 1;");
        TestHelper.execute("DELETE FROM test_table WHERE id = 1;");

        // The above insert will cause the records. Check if the topic exists
        // ListTopicsResult result = adminClient.listTopics(); // kafkaAdmin.listTopics();
        // // Set<String> topicNames = result.names().get();
        // result.names().get().forEach(System.out::println);
        // // for (String topic : topicNames) {
        // // System.out.println("Topic: " + topic);
        // // }
        // adminClient.close(Duration.ofSeconds(20));

        // System.out.println("Waiting for 5s for the replication...");
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

    }

    @Test
    public void testBatchInserts() throws Exception {
        
    }
}
