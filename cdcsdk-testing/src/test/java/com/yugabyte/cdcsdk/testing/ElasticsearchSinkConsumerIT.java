package com.yugabyte.cdcsdk.testing;

import static org.junit.jupiter.api.Assertions.fail;

import java.net.InetAddress;
import java.time.Duration;
import java.util.Arrays;

import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

import com.yugabyte.cdcsdk.testing.util.CdcsdkTestBase;
import com.yugabyte.cdcsdk.testing.util.TestImages;
import com.yugabyte.cdcsdk.testing.util.UtilStrings;

import io.debezium.testing.testcontainers.ConnectorConfiguration;
import io.debezium.testing.testcontainers.DebeziumContainer;

/**
 * Release test that verifies basic reading from a YugabyteDB database and
 * writing to Kafka and then further to Elasticsearch
 *
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class ElasticsearchSinkConsumerIT extends CdcsdkTestBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchSinkConsumerIT.class);
    private static ElasticsearchContainer esContainer;

    private static ConnectorConfiguration sinkConfig;

    @BeforeAll
    public static void beforeAll() throws Exception {
        initializeContainers();

        // Changing the default Kafka Connect image with the one having ElasticsearchSinkConnector
        kafkaConnectContainer = new DebeziumContainer(TestImages.KAFKA_CONNECT_ES)
                .withKafka(kafkaContainer)
                .dependsOn(kafkaContainer)
                .withNetwork(containerNetwork);

        esContainer = getElasticsearchContainer();
        esContainer.getEnvMap().remove("xpack.security.enabled");

        kafkaContainer.addExposedPort(KafkaContainer.KAFKA_PORT);
        kafkaContainer.start();
        kafkaConnectContainer.start();
        esContainer.start();

        // Initialize only the YBHelper and the KafkaHelper only
        initHelpers(true, true, false);
    }

    @BeforeEach
    public void beforeEachTest() throws Exception {
        // Drop tables before each test if they exist to ensure proper test flow
        ybHelper.execute(UtilStrings.getDropTableStmt(DEFAULT_TABLE_NAME));

        ybHelper.execute(UtilStrings.getCreateTableYBStmt(DEFAULT_TABLE_NAME));

        cdcsdkContainer = getCdcsdkContainerWithoutTransforms();
        cdcsdkContainer.start();
    }

    @AfterEach
    public void afterEachTest() throws Exception {
        // Stop the CDCSDK container
        cdcsdkContainer.stop();

        // Drop the table in the source database
        ybHelper.execute(UtilStrings.getDropTableStmt(DEFAULT_TABLE_NAME));
    }

    @AfterAll
    public static void afterAll() throws Exception {
        esContainer.stop();
        kafkaConnectContainer.stop();
        kafkaContainer.stop();
    }

    @Test
    public void verifyIfRecordsAreBeingSentToElasticsearch() throws Exception {
        final int recordsToBeInserted = 15;
        // Insert records in YB.
        for (int i = 0; i < recordsToBeInserted; ++i) {
            ybHelper.execute(UtilStrings.getInsertStmt(DEFAULT_TABLE_NAME, i, "first_" + i, "last_" + i, 23.45));
        }

        try {
            Awaitility.await()
                    .atLeast(Duration.ofSeconds(5))
                    .atMost(Duration.ofSeconds(60))
                    .until(() -> kafkaHelper.waitTillKafkaHasRecords(Arrays.asList(ybHelper.getKafkaTopicName())));

        }
        catch (ConditionTimeoutException e) {
            // It was observerd intermittently that a ConditionTimeoutException was being thrown
            // indicating that Kafka got no records, but on further observation, it was observed that only the
            // KafkaConsumer was unable to read the records while the rest of the test passed where the count
            // of the records was also being verified in Elasticsearch

            // Log for the warning here and move ahead in such exception cases
            // TODO Vaibhav: Debug this issue
            LOGGER.warn("Timeout while waiting for records on Kafka, check KafkaConsumer instance correctness.");
        }
        sinkConfig = getElasticsearchSinkConfiguration("http://"
                + InetAddress.getLocalHost().getHostAddress()
                + ":" + esContainer.getMappedPort(9200));
        kafkaConnectContainer.registerConnector("es-sink-connector", sinkConfig);

        String command = "curl -X GET "
                + InetAddress.getLocalHost().getHostAddress()
                + ":" + esContainer.getMappedPort(9200)
                + "/" + ybHelper.getKafkaTopicName() + "/_search?pretty";

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
            // If this exception is thrown then it means the records were not found to be
            // equal within the specified duration. Fail the test at this stage.
            fail();
        }
    }

    private ConnectorConfiguration getElasticsearchSinkConfiguration(String connectionUrl) throws Exception {
        return ConnectorConfiguration.create()
                .with("connector.class", "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector")
                .with("tasks.max", 1)
                .with("topics", ybHelper.getKafkaTopicName())
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

    private static GenericContainer<?> getCdcsdkContainerWithoutTransforms() throws Exception {
        GenericContainer<?> container = kafkaHelper.getCdcsdkContainer(ybHelper, "public." + DEFAULT_TABLE_NAME, 1);

        // Removing unwrap related properties since the ES connector needs a schema to
        // propagate records properly
        container.getEnvMap().remove("CDCSDK_SERVER_TRANSFORMS");
        container.getEnvMap().remove("CDCSDK_SERVER_TRANSFORMS_UNWRAP_DROP_TOMBSTONES");
        container.getEnvMap().remove("CDCSDK_SERVER_TRANSFORMS_UNWRAP_TYPE");

        // Assuming the container network is initialized by the time cdcsdkContainer is
        // created - if not initialized, throw an exception
        if (containerNetwork == null) {
            throw new RuntimeException("Docker container network not initialized for tests");
        }
        container.withNetwork(containerNetwork);

        return container;
    }

    private static ElasticsearchContainer getElasticsearchContainer() {
        return new ElasticsearchContainer(TestImages.ELASTICSEARCH_IMG_NAME)
                .withNetwork(containerNetwork)
                .withEnv("http.host", "0.0.0.0")
                .withEnv("ES_JAVA_OPTS", "-Xms512m -Xmx512m")
                .withEnv("transport.host", "127.0.0.1")
                .withExposedPorts(9200)
                .withPassword("password");
    }
}
