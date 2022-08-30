package com.yugabyte.cdcsdk.testing.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;

import org.yb.client.AsyncYBClient;
import org.yb.client.ListTablesResponse;
import org.yb.client.YBClient;
import org.yb.client.YBTable;
import org.yb.master.MasterDdlOuterClass.ListTablesResponsePB.TableInfo;

/**
 * Helper class to facilitate the operations performed on the YugabyteDB database
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class YBHelper {
    private String hostName = "127.0.0.1";
    private int ysqlPort = 5433;
    private int masterPort = 7100;

    private String database = "yugabyte";
    private String username = "yugabyte";
    private String password = "yugabyte";

    private String sourceTableName;

    public YBHelper(String hostName, String sourceTableName) {
        this.hostName = hostName;
        this.sourceTableName = sourceTableName;
    }

    public YBHelper(String hostName, int ysqlPort, int masterPort, String database, String username, String password, String sourceTableName) {
        this.hostName = hostName;
        this.ysqlPort = ysqlPort;
        this.masterPort = masterPort;
        this.database = database;
        this.username = username;
        this.password = password;
        this.sourceTableName = sourceTableName;
    }

    /**
     * Get the host on which YugabyteDB is running
     * @return hostname of the source database
     */
    public String getHostName() {
        return this.hostName;
    }

    /**
     * Get the port on which the YSQL process is running
     * @return the port of the YSQL process
     */
    public int getYsqlPort() {
        return this.ysqlPort;
    }

    /**
     * Get the port on which the master process is running
     * @return the port of the master process
     */
    public int getMasterPort() {
        return this.masterPort;
    }

    /**
     * Get the database name on which this object is created
     * @return name of the database
     */
    public String getDatabaseName() {
        return this.database;
    }

    /**
     * Get the name of the source table
     * @return name of the source table
     */
    public String getSourceTableName() {
        return this.sourceTableName;
    }

    /**
     * Get the name of the Kafka topic based on the name of the source table
     */
    public String getKafkaTopicName() {
        return UtilStrings.DATABASE_SERVER_NAME + ".public." + this.sourceTableName;
    }

    /**
     * Create a connection on the source YugabyteDB database
     * @return a JDBC Connection on the YugabyteDB database
     * @throws SQLException if the connection attempt fails
     */
    public Connection getConnection() throws SQLException {
        String connUrl = "jdbc:yugabytedb://%s:%d/%s?user=%s&password=%s";

        return DriverManager.getConnection(String.format(connUrl, hostName, ysqlPort, database, username, password));
    }

    /**
     * Execute the provided SQL query on the YugabyteDB database
     * @param sqlQuery the query to be executed
     * @throws SQLException if the connection attempt fails or the statement cannot be executed
     */
    public void execute(String sqlQuery) throws SQLException {
        try (Connection conn = getConnection()) {
            Statement st = conn.createStatement();
            st.execute(sqlQuery);
        }
        catch (SQLException e) {
            throw e;
        }
    }

    /**
     * Execute the provided SQL query on the YugabyteDB database and return the ResultSet thus 
     * obtained
     * @param sqlQuery the query to be executed
     * @return the resulting ResultSet object
     * @throws SQLException if the connection attempt fails or the statement cannot be executed
     * @see ResultSet
     */
    public ResultSet executeAndGetResultSet(String sqlQuery) throws SQLException {
        try (Connection conn = getConnection()) {
            Statement st = conn.createStatement();
            return st.executeQuery(sqlQuery);
        }
        catch (SQLException e) {
            throw e;
        }
    }

    /**
     * Insert records in the range [startIndex, endIndex) in the source table
     * @param startIndex start index for insertion
     * @param endIndex end index for insertion - exclusive
     * @throws SQLException if the connection cannot be created or statement cannot be executed
     */
    public void insertBulk(int startIndex, int endIndex) throws SQLException {
        try (Connection conn = getConnection()) {
            Statement st = conn.createStatement();
            for (int i = startIndex; i < endIndex; ++i) {
                st.execute(UtilStrings.getInsertStmt(sourceTableName, i, "first_" + i, "last_" + i, 23.45));
            }
        }
        catch (SQLException e) {
            throw e;
        }
    }

    /**
     * Get a {@link YBClient} instance for the source database
     * @return the YBClient instance
     * @see YBClient
     */
    public YBClient getYbClient() {
        AsyncYBClient asyncClient = new AsyncYBClient.AsyncYBClientBuilder(hostName + ":" + masterPort)
                .defaultAdminOperationTimeoutMs(60000)
                .defaultOperationTimeoutMs(60000)
                .defaultSocketReadTimeoutMs(60000)
                .numTablets(25)
                .build();

        return new YBClient(asyncClient);
    }

    /**
     * Get a {@link YBTable} instance for the provided table name
     * @param syncClient the YBClient instance to use
     * @param tableName the name of the table to get the tableId for
     * @return a YBTable instance for the YSQL table of the given name or null in case the table is
     * not found
     * @throws Exception if things go wrong
     * @see YBClient
     * @see YBTable
     */
    private YBTable getTableUUID(YBClient syncClient, String tableName) throws Exception {
        ListTablesResponse resp = syncClient.getTablesList();

        for (TableInfo tableInfo : resp.getTableInfoList()) {
            if (Objects.equals(tableInfo.getName(), tableName)) {
                return syncClient.openTableByUUID(tableInfo.getId().toStringUtf8());
            }
        }

        // This will be returned in case no table match has been found for the given table name
        return null;
    }

    /**
     * Get a DB stream ID on the given database
     * @param dbName name of the database on which DB stream ID will be created
     * @return the created DB stream ID
     * @throws Exception if stream id cannot be created because - a) YBClient cannot be created 
     * or b) a table UUID cannot be obtained or c) something goes wrong at the stream creation step
     * @see YBClient
     * @see YBTable
     */
    public String getNewDbStreamId(String dbName) throws Exception {
        YBClient syncClient = getYbClient();
        YBTable placeholderTable = getTableUUID(syncClient, sourceTableName);

        if (placeholderTable == null) {
            throw new NullPointerException("No table found with the specified name");
        }

        return syncClient.createCDCStream(placeholderTable, dbName, "PROTO", "IMPLICIT").getStreamId();
    }
}
