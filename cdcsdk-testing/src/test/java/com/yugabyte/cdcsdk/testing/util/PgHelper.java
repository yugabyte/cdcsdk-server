package com.yugabyte.cdcsdk.testing.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.testcontainers.containers.PostgreSQLContainer;

import io.debezium.testing.testcontainers.ConnectorConfiguration;

/**
 * Helper class to facilitate Postgres related operations
 * 
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class PgHelper {
  private static String host = "127.0.0.1";
  private static int pgPort = 5432;

  private static String defaultPgSinkTableName = "test_table";

  /**
   * Set the host IP address where Postgres instance is running
   * 
   * @param hostname the IP of the Postgres instance
   */
  public static void setHost(String hostname) {
    host = hostname;
  }

  /**
   * The port on which the Postgres process is running
   * 
   * @param port the port number
   */
  public static void setPgPort(int port) {
    pgPort = port;
  }

  /**
   * Get a JDBC connection on the Postgres instance
   * 
   * @return the JDBC Connection object
   * @param postgresHost the IP on which Postgres instance is running
   * @throws SQLException if the connection cannot be created
   * @see Connection
   */
  public static Connection getConnection(String postgresHost) throws SQLException {
    String connUrl = "jdbc:postgresql://" + host + ":" + String.valueOf(pgPort)
        + "/postgres?user=postgres&password=postgres";
    return DriverManager.getConnection(connUrl);
  }

  /**
   * Wrapper function around {@link #getConnection(String)} using the preset host
   * 
   * @return the JDBC Connection
   * @throws SQLException if the connection cannot be created
   */
  public static Connection getConnection() throws SQLException {
    return getConnection(host);
  }

  /**
   * Execute the provided SQL query in the Postgres database
   * 
   * @param postgresIp the IP address of the Postgres instance
   * @param sqlQuery   the query to be executed
   * @throws SQLException if connection cannot be created or query cannot be
   *                      executed
   */
  public static void execute(String postgresIp, String sqlQuery) throws SQLException {
    try (Connection conn = getConnection(postgresIp)) {
      Statement st = conn.createStatement();
      st.execute(sqlQuery);
    } catch (SQLException e) {
      throw e;
    }
  }

  /**
   * Wrapper function around {@link #execute(String, String)} using the preset
   * host
   * 
   * @param sqlQuery the query to be executed
   * @throws SQLException if connection cannot be created or query cannot be
   *                      executed
   */
  public static void execute(String sqlQuery) throws SQLException {
    execute(host, sqlQuery);
  }

  /**
   * Execute the provided SQL query and return the resulting ResultSet
   * 
   * @param postgresIp the IP of the Postgres instance
   * @param sqlQuery   the query to be executed
   * @return the {@link ResultSet} on executing the query
   * @throws SQLException if connection cannot be created or the query cannot be
   *                      executed
   */
  public static ResultSet executeAndGetResultSet(String postgresIp, String sqlQuery) throws SQLException {
    try (Connection conn = getConnection(postgresIp)) {
      return conn.createStatement().executeQuery(sqlQuery);
    } catch (SQLException e) {
      throw e;
    }
  }

  /**
   * Wrapper function around {@link #executeAndGetResultSet(String, String)} using
   * the preset host
   * 
   * @param sqlQuery the query to be executed
   * @return the {@link ResultSet} on executing the query
   * @throws SQLException if connection cannot be created or the query cannot be
   *                      executed
   */
  public static ResultSet executeAndGetResultSet(String sqlQuery) throws SQLException {
    return executeAndGetResultSet(host, sqlQuery);
  }

  /**
   * Verify that the count of records in the given table name is the one as
   * expected
   * 
   * @param pgContainerIp       the IP of the Postgres instance
   * @param expectedRecordCount the expected number of records in the given table
   * @param tableNameInPostgres the table name in Postgres where the records
   *                            should be counted
   * @throws SQLException if the {@link ResultSet} cannot be retrieved
   */
  public static void assertRecordCountInPostgres(String pgContainerIp, int expectedRecordCount,
      String tableNameInPostgres) throws SQLException {
    ResultSet rs = executeAndGetResultSet(pgContainerIp,
        String.format("SELECT COUNT(*) FROM %s;", tableNameInPostgres));
    if (rs.next()) {
      assertEquals(expectedRecordCount, rs.getInt(1));
    } else {
      // Fail in case no ResultSet object is retrieved
      fail();
    }
  }

  /**
   * Wrapper function around
   * {@link #assertRecordCountInPostgres(String, int, String)} using the
   * preset Postgres host
   * 
   * @param expectedRecordCount the expected number of records in the given table
   * @param tableNameInPostgres the table name in Postgres where the records
   *                            should be counted
   * @throws SQLException if the {@link ResultSet} cannot be retrieved
   */
  public static void assertRecordCountInPostgres(int expectedRecordCount, String tableNameInPostgres)
      throws SQLException {
    assertRecordCountInPostgres(host, expectedRecordCount, tableNameInPostgres);
  }

  /**
   * Wrapper function around
   * {@link #assertRecordCountInPostgres(String, int, String)} using the
   * preset Postgres host and the default sink table
   * 
   * @param expectedRecordCount the expected number of records in the given table
   * @throws SQLException if the {@link ResultSet} cannot be retrieved
   */
  public static void assertRecordCountInPostgres(int expectedRecordCount) throws SQLException {
    assertRecordCountInPostgres(host, expectedRecordCount, defaultPgSinkTableName);
  }

  /**
   * Get a connector configuration for the JDBCSinkConnector
   * 
   * @param pgContainer      PostgreSQLContainer instance
   * @param kafkaTopics      comma separated values of Kafka topics to read from
   * @param tableNameFormat  name of the target/sink table
   * @param primaryKeyFields comma separated values of the primary key fields
   * @return the connector configuration for JDBC sink
   */
  public static ConnectorConfiguration getJdbcSinkConfiguration(PostgreSQLContainer<?> pgContainer,
      String kafkaTopics, String tableNameFormat, String primaryKeyFields) {
    String containerIp = pgContainer.getContainerInfo().getNetworkSettings().getNetworks()
        .entrySet().stream().findFirst().get().getValue().getIpAddress();
    return ConnectorConfiguration
        .forJdbcContainer(pgContainer)
        .with("connector.class", "io.confluent.connect.jdbc.JdbcSinkConnector")
        .with("tasks.max", "1")
        .with("topics", kafkaTopics)
        .with("database.server.name", "dbserver1")
        .with("dialect.name", "PostgreSqlDatabaseDialect")
        .with("table.name.format", tableNameFormat)
        .with("connection.url",
            "jdbc:postgresql://" + containerIp + ":5432/postgres?user=postgres&password=postgres&sslMode=require")
        .with("auto.create", "true")
        .with("insert.mode", "upsert")
        .with("pk.fields", primaryKeyFields)
        .with("pk.mode", "record_key")
        .with("delete.enabled", "true")
        .with("auto.evolve", "true")
        .with("value.converter", "org.apache.kafka.connect.json.JsonConverter")
        .with("value.converter.schemas.enable", "true")
        .with("key.converter", "org.apache.kafka.connect.json.JsonConverter")
        .with("key.converter.schemas.enable", "true");
  }
}
