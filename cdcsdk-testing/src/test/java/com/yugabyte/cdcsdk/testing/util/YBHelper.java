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
  private static String host = "127.0.0.1";
  private static int ysqlPort = 5433;
  private static int masterPort = 7100;

  /**
   * Set the host IP address on which YugabyteDB is running
   * @param hostname IP address of the source database
   */
  public static void setHost(String hostname) {
    host = hostname;
  }

  /**
   * Set the port on which the YSQL process is running
   * @param port the port of the YSQL process
   */
  public static void setYsqlPort(int port) {
    ysqlPort = port;
  }

  /**
   * Set the port on which the master process is running
   * @param port the port of the master process
   */
  public static void setMasterPort(int port) {
    masterPort = port;
  }

  /**
   * Create a connection on the source YugabyteDB database
   * @return a JDBC Connection on the YugabyteDB database
   * @throws SQLException if the connection attempt fails
   */
  public static Connection getConnection() throws SQLException {
    String connUrl = "jdbc:yugabytedb://" + host + ":" + ysqlPort + "/yugabyte?user=yugabyte&password=yugabyte";

    return DriverManager.getConnection(connUrl);
  }

  /**
   * Execute the provided SQL query on the YugabyteDB database
   * @param sqlQuery the query to be executed
   * @throws SQLException if the connection attempt fails or the statement cannot be executed
   */
  public static void execute(String sqlQuery) throws SQLException {
    try (Connection conn = getConnection()) {
      Statement st = conn.createStatement();
      st.execute(sqlQuery);
    } catch (SQLException e) {
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
  public static ResultSet executeAndGetResultSet(String sqlQuery) throws SQLException {
    try (Connection conn = getConnection()) {
      Statement st = conn.createStatement();
      return st.executeQuery(sqlQuery);
    } catch (SQLException e) {
      throw e;
    }
  }

  /**
   * Get a {@link YBClient} instance for the source database
   * @return the YBClient instance
   * @see YBClient
   */
  public static YBClient getYbClient() {
    AsyncYBClient asyncClient = new AsyncYBClient.AsyncYBClientBuilder(host + ":" + masterPort)
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
  private static YBTable getTableUUID(YBClient syncClient, String tableName) throws Exception {
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
  public static String getNewDbStreamId(String dbName) throws Exception {
    YBClient syncClient = getYbClient();
    YBTable placeholderTable = getTableUUID(syncClient, "test_table");

    if (placeholderTable == null) {
        throw new NullPointerException("No table found with the specified name");
    }

    return syncClient.createCDCStream(placeholderTable, dbName, "PROTO", "IMPLICIT").getStreamId();
  }
}
