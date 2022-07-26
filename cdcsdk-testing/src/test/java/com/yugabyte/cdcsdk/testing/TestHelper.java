package com.yugabyte.cdcsdk.testing;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.yb.client.AsyncYBClient;
import org.yb.client.ListTablesResponse;
import org.yb.client.YBClient;
import org.yb.client.YBTable;
import org.yb.master.MasterDdlOuterClass.ListTablesResponsePB.TableInfo;

import com.fasterxml.jackson.databind.JsonNode;
import com.yugabyte.cdcsdk.testing.util.CdcsdkContainer;

public class TestHelper {
    private static String HOST = "127.0.0.1";
    private static int YSQL_PORT = 5433;
    private static int MASTER_PORT = 7100;
    private static String BOOTSTRAP_SERVER = "127.0.0.1:9092";
    private static Network containerNetwork;

    private static final String ELASTIC_SEARCH_IMAGE = "docker.elastic.co/elasticsearch/elasticsearch:7.3.0";

    private static final Logger LOGGER = LoggerFactory.getLogger(TestHelper.class);

    private static Connection getConnection() throws SQLException {
        String jdbcUrl = "jdbc:yugabytedb://" + HOST + ":" + YSQL_PORT + "/yugabyte?user=yugabyte&password=yugabyte";
        return DriverManager.getConnection(jdbcUrl);
    }

    public static void setBootstrapServer(String bootstrapServer) {
        BOOTSTRAP_SERVER = bootstrapServer;
    }

    public static void setHost(String host) {
        HOST = host;
    }

    public static void setYsqlPort(int port) {
        YSQL_PORT = port;
    }

    public static void setMasterPort(int masterPort) {
        MASTER_PORT = masterPort;
    }

    public static YBClient getYbClient() {
        AsyncYBClient asyncClient = new AsyncYBClient.AsyncYBClientBuilder(HOST + ":" + MASTER_PORT)
                .defaultAdminOperationTimeoutMs(60000)
                .defaultOperationTimeoutMs(60000)
                .defaultSocketReadTimeoutMs(60000)
                .numTablets(10)
                .build();

        return new YBClient(asyncClient);
    }

    protected static YBTable getTableUUID(YBClient syncClient, String tableName) throws Exception {
        ListTablesResponse resp = syncClient.getTablesList();

        for (TableInfo tableInfo : resp.getTableInfoList()) {
            if (Objects.equals(tableInfo.getName(), tableName)) {
                return syncClient.openTableByUUID(tableInfo.getId().toStringUtf8());
            }
        }

        // This will be returned in case no table match has been found for the given table name
        return null;
    }

    public static String getNewDbStreamId(String dbName) throws Exception {
        YBClient syncClient = getYbClient();
        YBTable placeholderTable = getTableUUID(syncClient, "test_table");

        if (placeholderTable == null) {
            throw new NullPointerException("No table found with the specified name");
        }

        return syncClient.createCDCStream(placeholderTable, dbName, "PROTO", "IMPLICIT").getStreamId();
    }

    public static GenericContainer<?> getCdcsdkContainerForS3Sink() throws Exception {
        return new CdcsdkContainer()
                .withDatabaseHostname(HOST)
                .withMasterPort(String.valueOf(MASTER_PORT))
                .withAwsAccessKeyId(System.getenv("AWS_ACCESS_KEY_ID"))
                .withAwsSecretAccessKey(System.getenv("AWS_SECRET_ACCESS_KEY"))
                .withAwsSessionToken(System.getenv("AWS_SESSION_TOKEN"))
                .withTableIncludeList("public.test_table")
                .buildForS3Sink();
    }

    public static GenericContainer<?> getCdcsdkContainerForKafkaSink() throws Exception {
        return new CdcsdkContainer()
                .withDatabaseHostname(HOST)
                .withMasterPort(String.valueOf(MASTER_PORT))
                .withKafkaBootstrapServers(BOOTSTRAP_SERVER)
                .withTableIncludeList("public.test_table")
                .buildForKafkaSink();
    }

    public static ElasticsearchContainer getElasticsearchContainer(Network containeNetwork) throws Exception {
        return new ElasticsearchContainer(ELASTIC_SEARCH_IMAGE)
                .withNetwork(containerNetwork)
                .withEnv("http.host", "0.0.0.0")
                .withEnv("ES_JAVA_OPTS", "-Xms512m -Xmx512m")
                .withEnv("transport.host", "127.0.0.1")
                .withExposedPorts(9200)
                .withPassword("password");
    }

    public static void execute(String sqlQuery) throws Exception {
        try (Connection conn = getConnection()) {
            Statement st = conn.createStatement();
            st.execute(sqlQuery);
        }
        catch (Exception e) {
            LOGGER.error("Error executing query: " + sqlQuery, e);
            throw e;
        }
    }

    public static ResultSet executeAndGetResultSet(String sqlQuery) throws Exception {
        try (Connection conn = getConnection()) {
            return conn.createStatement().executeQuery(sqlQuery);
        }
        catch (Exception e) {
            LOGGER.error("Error executing query: " + sqlQuery, e);
            throw e;
        }
    }

    public static KafkaConsumer<String, JsonNode> getKafkaConsumer(String bootstrapServers) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("group.id", "testapp");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.connect.json.JsonDeserializer");
        return new KafkaConsumer<>(props);
    }

    public static boolean waitTillKafkaHasRecords(KafkaConsumer<String, JsonNode> kConsumer, List<String> topics) throws Exception {
        kConsumer.subscribe(topics);
        kConsumer.seekToBeginning(kConsumer.assignment());
        ConsumerRecords<String, JsonNode> records = kConsumer.poll(Duration.ofSeconds(15));

        return records.count() != 0;
    }

    public static String executeShellCommand(String command) throws Exception {
        Process process = Runtime.getRuntime().exec(command);
        String stdOutput = IOUtils.toString(process.getInputStream(), StandardCharsets.UTF_8);
        process.destroy();
        return stdOutput;
    }
}
