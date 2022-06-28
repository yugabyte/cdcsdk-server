package com.yugabyte.cdcsdk.testing;

// import static org.junit.jupiter.api.Assertions.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
<<<<<<< HEAD
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.YugabyteYSQLContainer;
import org.testcontainers.utility.DockerImageName;
import org.yb.client.AsyncYBClient;
import org.yb.client.ListTablesResponse;
import org.yb.client.YBClient;
import org.yb.client.YBTable;
import org.yb.master.MasterDdlOuterClass.ListTablesResponsePB.TableInfo;

<<<<<<< HEAD
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;

=======
>>>>>>> main
public class TestHelper {
    private static String HOST = "127.0.0.1";
    private static int YSQL_PORT = 5433;
    private static int MASTER_PORT = 7100;

    private static final Logger LOGGER = LoggerFactory.getLogger(TestHelper.class);

    private static Connection getConnection() throws SQLException {
        String jdbcUrl = "jdbc:yugabytedb://" + HOST + ":" + YSQL_PORT + "/yugabyte?user=yugabyte&password=yugabyte";
        return DriverManager.getConnection(jdbcUrl);
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
        System.out.println("Going to create a CDC stream...");

        return syncClient.createCDCStream(placeholderTable, dbName, "PROTO", "IMPLICIT").getStreamId();
    }

    public static YugabyteYSQLContainer getYbContainer() throws Exception {
        YugabyteYSQLContainer container = new YugabyteYSQLContainer("yugabytedb/yugabyte:2.13.2.0-b135");
        container.withPassword("yugabyte");
        container.withUsername("yugabyte");
        container.withDatabaseName("yugabyte");
        container.withExposedPorts(7100, 9100, 5433, 9042);
        container.withCreateContainerCmdModifier(cmd -> cmd.withHostName("127.0.0.1").getHostConfig().withPortBindings(new ArrayList<PortBinding>() {
            {
                add(new PortBinding(Ports.Binding.bindPort(7100), new ExposedPort(7100)));
                add(new PortBinding(Ports.Binding.bindPort(9100), new ExposedPort(9100)));
                add(new PortBinding(Ports.Binding.bindPort(5433), new ExposedPort(5433)));
                add(new PortBinding(Ports.Binding.bindPort(9042), new ExposedPort(9042)));
            }
        }));
        container.withCommand("bin/yugabyted start --listen=0.0.0.0 --master_flags=rpc_bind_addresses=0.0.0.0 --daemon=false");
        return container;
    }

    private static Map<String, String> getConfigMap() {
        Map<String, String> configs = new HashMap<>();
        configs.put("CDCSDK_SOURCE_CONNECTOR_CLASS", "io.debezium.connector.yugabytedb.YugabyteDBConnector");
        configs.put("CDCSDK_SOURCE_DATABASE_HOSTNAME", HOST);
        configs.put("CDCSDK_SOURCE_DATABASE_PORT", "5433");
        configs.put("CDCSDK_SOURCE_DATABASE_MASTER_ADDRESSES", HOST+":"+MASTER_PORT);
        configs.put("CDCSDK_SOURCE_DATABASE_SERVER_NAME", "dbserver1");
        configs.put("CDCSDK_SOURCE_DATABASE_DBNAME", "yugabyte");
        configs.put("CDCSDK_SOURCE_DATABASE_USER", "yugabyte");
        configs.put("CDCSDK_SOURCE_DATABASE_PASSWORD", "yugabyte");
        configs.put("CDCSDK_SOURCE_TABLE_INCLUDE_LIST", "public.test_table");
        configs.put("CDCSDK_SOURCE_SNAPSHOT_MODE", "never");
        configs.put("CDCSDK_SOURCE_DATABASE_STREAM_ID", getNewDbStreamId("yugabyte"));

        // Add configs for the sink
        configs.put("CDCSDK_SINK_TYPE", "s3");
        configs.put("CDCSDK_SINK_S3_BUCKET_NAME", "cdcsdk-testing");
        configs.put("CDCSDK_SINK_S3_REGION", "us-west-2");
        configs.put("CDCSDK_SINK_S3_BASEDIR", "S3ConsumerIT/");
        configs.put("CDCSDK_SINK_S3_PATTERN", "stream_123");
        configs.put("CDCSDK_SINK_S3_FLUSH_RECORDS", "4");
        configs.put("CDCSDK_SINK_S3_FLUSH_SIZEMB", "200");
        configs.put("CDCSDK_SERVER_TRANSFORMS", "FLATTEN");

        return configs;
    }

    public static GenericContainer<?> getCdcsdkContainer() throws Exception {
        GenericContainer<?> cdcsdkContainer = new GenericContainer<>(DockerImageName.parse("yugabyte/cdcsdk-server:${some-project-version}"));
        
        // By the time this container is created, the table should be there in the database already
        cdcsdkContainer.withEnv(getConfigMap());

        return null;

        // todo: add an image tag to the DockerImageName.parse and then only return the cdcsdkContainer
        // return cdcsdkContainer;
    }

    public static void execute(String sqlQuery) throws Exception {
        try (Connection conn = getConnection()) {
            // System.out.println("Creating a statement");
            Statement st = conn.createStatement();
            // System.out.println("Executing the SQL query");
            st.execute(sqlQuery);
            // conn.createStatement().execute(sqlQuery);
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
}
