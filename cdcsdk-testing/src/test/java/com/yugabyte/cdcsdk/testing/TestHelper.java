package com.yugabyte.cdcsdk.testing;

// import static org.junit.jupiter.api.Assertions.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.YugabyteYSQLContainer;
import org.yb.client.AsyncYBClient;
import org.yb.client.ListTablesResponse;
import org.yb.client.YBClient;
import org.yb.client.YBTable;
import org.yb.master.MasterDdlOuterClass.ListTablesResponsePB.TableInfo;

import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;

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
