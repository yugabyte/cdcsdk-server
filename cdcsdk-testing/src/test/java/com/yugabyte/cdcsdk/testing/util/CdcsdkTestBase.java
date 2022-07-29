package com.yugabyte.cdcsdk.testing.util;

import java.net.InetAddress;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;

import io.debezium.testing.testcontainers.DebeziumContainer;

// NOTE FOR ANYBODY WRITING TESTS:
// Make sure the name of the source table and sink table are the same, if you need them to be
// different for any reason at all, you will need to modify/add the relevant helper functions. 

/**
 * Base class for common test related configurations.
 * 
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class CdcsdkTestBase {
    protected static Network containerNetwork;

    // Some basic containers required across all tests
    protected static KafkaContainer kafkaContainer;
    protected static DebeziumContainer kafkaConnectContainer;
    protected static GenericContainer<?> cdcsdkContainer;
    protected static PostgreSQLContainer<?> postgresContainer;

    // Helper objects
    protected static PgHelper pgHelper;
    protected static YBHelper ybHelper;
    protected static KafkaHelper kafkaHelper;

    // Default table name
    protected static final String DEFAULT_TABLE_NAME = "test_table";

    /**
     * Set the default name of the table
     * @param tableName the default name to be set
     */
    // protected void setDefaultTableName(String tableName) {
    // DEFAULT_TABLE_NAME = tableName;
    // }

    /**
     * Base function to create containers
     * @throws Exception if things go wrong
     */
    protected static void initializeContainers() throws Exception {
        // Initialize the Docker container network
        containerNetwork = Network.newNetwork();

        kafkaContainer = new KafkaContainer(TestImages.KAFKA)
                .withNetworkAliases("kafka")
                .withNetwork(containerNetwork);

        kafkaConnectContainer = new DebeziumContainer(TestImages.KAFKA_CONNECT)
                .withKafka(kafkaContainer)
                .dependsOn(kafkaContainer)
                .withNetwork(containerNetwork);

        postgresContainer = new PostgreSQLContainer<>(TestImages.POSTGRES)
                .withUsername("postgres")
                .withPassword("postgres")
                .withExposedPorts(5432)
                .withReuse(true)
                .withNetwork(containerNetwork);

        ybHelper = new YBHelper(InetAddress.getLocalHost().getHostAddress(), DEFAULT_TABLE_NAME);
    }

    /**
     * Drop the table with the provided name in both source and sink database
     * @param tableName name of the table in both source and sink PG database
     * @throws Exception if pgHelper is not initialized or if the tables cannot be dropped
     */
    protected void dropTablesAfterEachTest(String tableName) throws Exception {
        if (pgHelper == null) {
            throw new RuntimeException("pgHelper not initialized. Check if the test actually needs this function to drop both source and sink tables");
        }

        ybHelper.execute(UtilStrings.getDropTableStmt(tableName));
        pgHelper.execute(UtilStrings.getDropTableStmt(tableName));
    }
}
