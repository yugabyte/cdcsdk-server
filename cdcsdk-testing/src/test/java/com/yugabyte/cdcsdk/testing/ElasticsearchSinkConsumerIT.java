package com.yugabyte.cdcsdk.testing;

import static org.junit.jupiter.api.Assertions.fail;

import java.net.InetAddress;
import java.time.Duration;
import java.util.Arrays;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.DockerImageName;

import com.fasterxml.jackson.databind.JsonNode;

import io.debezium.testing.testcontainers.ConnectorConfiguration;
import io.debezium.testing.testcontainers.DebeziumContainer;

/**
 * Release test that verifies basic reading from a YugabyteDB database and
 * writing to Kafka and then further to Elasticsearch
 *
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class ElasticsearchSinkConsumerIT {
    private static final String KAFKA_CONNECT_IMAGE = "quay.io/yugabyte/connect-jdbc-es:1.0";
    private static final String KAFKA_IMAGE = "confluentinc/cp-kafka:6.2.1";

    private static final String createTableSql = "CREATE TABLE IF NOT EXISTS test_table (id int primary key, first_name varchar(30), last_name varchar(50), days_worked double precision)";
    private static final String dropTableSql = "DROP TABLE test_table";

    private static KafkaContainer kafkaContainer;
    private static ElasticsearchContainer esContainer;
    private static DebeziumContainer kafkaConnectContainer;
    private static GenericContainer<?> cdcsdkContainer;

    private static ConnectorConfiguration sinkConfig;

    private static Network containerNetwork;

    private static GenericContainer<?> getCdcsdkContainerWithoutTransforms() throws Exception {
        GenericContainer<?> container = TestHelper.getCdcsdkContainerForKafkaSink();

        // Removing unwrap related properties since the ES connector needs a schema to propagate
        // records properly
        container.getEnvMap().remove("CDCSDK_SERVER_TRANSFORMS");
        container.getEnvMap().remove("CDCSDK_SERVER_TRANSFORMS_UNWRAP_DROP_TOMBSTONES");
        container.getEnvMap().remove("CDCSDK_SERVER_TRANSFORMS_UNWRAP_TYPE");

        // Assuming the container network is initialized by the time cdcsdkContainer is created
        // If not initialized, throw an exception
        if (containerNetwork == null) {
            throw new RuntimeException("Docker container network not initialized for tests");
        }
        container.withNetwork(containerNetwork);

        return container;
    }

    private static ConnectorConfiguration getElasticsearchSinkConfiguration(String connectionUrl) throws Exception {
        return ConnectorConfiguration.create()
                .with("connector.class", "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector")
                .with("tasks.max", 1)
                .with("topics", "dbserver1.public.test_table")
                .with("connection.url", connectionUrl)
                .with("transforms", "unwrap,key")
                .with("transforms.unwrap.type", "io.debezium.connector.yugabytedb.transforms.YBExtractNewRecordState")
                .with("transforms.key.type", "org.apache.kafka.connect.transforms.ExtractField$Key")
                .with("transforms.key.field", "id")
                .with("key.ignore", "false")
                .with("type.name", "test_table")
                .with("schema.ignore", "true")
                .with("key.ignore", "true");
    }

    @BeforeAll
    public static void beforeAll() throws Exception {
        containerNetwork = Network.newNetwork();

        kafkaContainer = new KafkaContainer(DockerImageName.parse(KAFKA_IMAGE))
                .withNetworkAliases("kafka")
                .withNetwork(containerNetwork);

        kafkaConnectContainer = new DebeziumContainer(KAFKA_CONNECT_IMAGE)
                .withKafka(kafkaContainer)
                .dependsOn(kafkaContainer)
                .withNetwork(containerNetwork);

        esContainer = TestHelper.getElasticsearchContainer(containerNetwork);
        esContainer.getEnvMap().remove("xpack.security.enabled");

        kafkaContainer.start();
        kafkaConnectContainer.start();
        try {
            esContainer.start();
        }
        catch (Exception e) {
            System.out.println(esContainer.getLogs());
            throw e;
        }

        TestHelper.setHost(InetAddress.getLocalHost().getHostAddress());
        TestHelper.setBootstrapServer(kafkaContainer.getNetworkAliases().get(0) + ":9092");

        TestHelper.execute(createTableSql);

        cdcsdkContainer = getCdcsdkContainerWithoutTransforms();
        cdcsdkContainer.start();
    }

    @AfterAll
    public static void afterAll() throws Exception {
        esContainer.stop();
        kafkaConnectContainer.stop();
        cdcsdkContainer.stop();
        kafkaContainer.stop();
        TestHelper.execute(dropTableSql);
    }

    @Test
    public void testElasticsearchSink() throws Exception {
        final int recordsToBeInserted = 15;
        // Insert records in YB.
        for (int i = 0; i < recordsToBeInserted; ++i) {
            String insertSql = String.format("INSERT INTO test_table VALUES (%d, '%s', '%s', %f);",
                    i, "first_" + i, "last_" + i, 23.45);
            TestHelper.execute(insertSql);
        }

        KafkaConsumer<String, JsonNode> kConsumer = TestHelper.getKafkaConsumer(
                kafkaContainer.getContainerInfo()
                        .getNetworkSettings()
                        .getNetworks()
                        .entrySet().stream().findFirst().get().getValue()
                        .getIpAddress() + ":9093");
        Awaitility.await()
                .atLeast(Duration.ofSeconds(15))
                .atMost(Duration.ofSeconds(45))
                .until(() -> TestHelper.waitTillKafkaHasRecords(kConsumer,
                        Arrays.asList("dbserver1.public.test_table")));

        sinkConfig = getElasticsearchSinkConfiguration("http://"
                + InetAddress.getLocalHost().getHostAddress()
                + ":" + esContainer.getMappedPort(9200));
        kafkaConnectContainer.registerConnector("es-sink-connector", sinkConfig);

        String command = "curl -X GET "
                + InetAddress.getLocalHost().getHostAddress()
                + ":" + esContainer.getMappedPort(9200)
                + "/dbserver1.public.test_table/_search?pretty";

        try {
            Awaitility.await()
                    .atLeast(Duration.ofSeconds(3))
                    .atMost(Duration.ofSeconds(30))
                    .pollDelay(Duration.ofSeconds(3))
                    .until(() -> {
                        JSONObject response = new JSONObject(TestHelper.executeShellCommand(command));
                        int totalRecordsInElasticSearch = response.getJSONObject("hits")
                                .getJSONObject("total")
                                .getInt("value");
                        return recordsToBeInserted == totalRecordsInElasticSearch;
                    });
        }
        catch (ConditionTimeoutException exception) {
            // If this exception is thrown then it means the records were not found to be equal
            // within the specified duration. Fail the test at this stage.
            fail();
        }
    }
}
