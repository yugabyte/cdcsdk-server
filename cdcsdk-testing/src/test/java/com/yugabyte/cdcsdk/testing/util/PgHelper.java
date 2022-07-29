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
    private String hostName;
    private int postgresPort = 5432;
    private String username = "postgres";
    private String password = "postgres";
    private String database = "postgres";

    private String sinkTableName;

    /**
     * Intantiate an object of the class with the help of the PostgreSQLContainer
     * @param pgContainer the {@link PostgreSQLContainer} running Postgres
     * @param sinkTableName name of the sink table
     */
    public PgHelper(PostgreSQLContainer<?> pgContainer, String sinkTableName) {
        this.hostName = pgContainer.getContainerInfo().getNetworkSettings().getNetworks()
                .entrySet().stream().findFirst().get().getValue().getIpAddress();
        this.username = pgContainer.getUsername();
        this.password = pgContainer.getPassword();
        this.sinkTableName = sinkTableName;
    }

    public PgHelper(String hostName, int postgresPort, String sinkTableName) {
        this.hostName = hostName;
        this.postgresPort = postgresPort;
        this.sinkTableName = sinkTableName;
    }

    public PgHelper(String hostName, int postgresPort, String sinkTableName, String username, String password, String database) {
        this.hostName = hostName;
        this.postgresPort = postgresPort;
        this.sinkTableName = sinkTableName;
        this.username = username;
        this.password = password;
        this.database = database;
    }

    /**
     * Get the host IP address where Postgres instance is running
     * 
     * @return hostname of the Postgres instance
     */
    public String getHostName() {
        return this.hostName;
    }

    /**
     * Get the port on which the Postgres process is running
     * 
     * @return the Postgres port
     */
    public int getPostgresPort() {
        return this.postgresPort;
    }

    /**
     * Get the name of the Kafka topic based on the name of the sink table
     */
    public String getKafkaTopicName() {
        return UtilStrings.DATABASE_SERVER_NAME + ".public." + sinkTableName;
    }

    /**
     * Get a JDBC connection on the Postgres instance
     * 
     * @return the JDBC Connection object
     * @throws SQLException if the connection cannot be created
     * @see Connection
     */
    public Connection getConnection() throws SQLException {
        String connUrl = "jdbc:postgresql://%s:%d/%s?user=%s&password=%s&sslMode=require";
        return DriverManager.getConnection(String.format(connUrl, hostName, postgresPort, database, username, password));
    }

    /**
     * Execute the provided SQL query in the Postgres database
     * 
     * @param sqlQuery   the query to be executed
     * @throws SQLException if connection cannot be created or query cannot be
     *                      executed
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
     * Execute the provided SQL query and return the resulting ResultSet
     * 
     * @param sqlQuery   the query to be executed
     * @return the {@link ResultSet} on executing the query
     * @throws SQLException if connection cannot be created or the query cannot be
     *                      executed
     */
    public ResultSet executeAndGetResultSet(String sqlQuery) throws SQLException {
        try (Connection conn = getConnection()) {
            return conn.createStatement().executeQuery(sqlQuery);
        }
        catch (SQLException e) {
            throw e;
        }
    }

    /**
     * Verify that the count of records in the given table name is the one as
     * expected
     * 
     * @param expectedRecordCount the expected number of records in the given table
     * @param tableNameInPostgres the table name in Postgres where the records
     *                            should be counted
     * @throws SQLException if the {@link ResultSet} cannot be retrieved
     */
    public void assertRecordCountInPostgres(int expectedRecordCount) throws SQLException {
        ResultSet rs = executeAndGetResultSet(String.format("SELECT COUNT(*) FROM %s;", sinkTableName));

        if (rs.next()) {
            assertEquals(expectedRecordCount, rs.getInt(1));
        }
        else {
            // Fail in case no ResultSet object is retrieved
            fail();
        }
    }

    /**
     * Get a connector configuration for the JDBCSinkConnector
     * 
     * @param pgContainer      PostgreSQLContainer instance
     * @param kafkaTopics      comma separated values of Kafka topics to read from
     * @param tableName  name of the target/sink table
     * @param primaryKeyFields comma separated values of the primary key fields
     * @return the connector configuration for JDBC sink
     */
    public ConnectorConfiguration getJdbcSinkConfiguration(PostgreSQLContainer<?> pgContainer, String tableName, String primaryKeyFields) {
        String connUrl = "jdbc:postgresql://%s:%d/%s?user=%s&password=%s&sslMode=require";
        return ConnectorConfiguration
                .forJdbcContainer(pgContainer)
                .with("connector.class", "io.confluent.connect.jdbc.JdbcSinkConnector")
                .with("tasks.max", "1")
                .with("topics", getKafkaTopicName())
                .with("database.server.name", UtilStrings.DATABASE_SERVER_NAME)
                .with("dialect.name", "PostgreSqlDatabaseDialect")
                .with("table.name.format", tableName)
                .with("connection.url", String.format(connUrl, hostName, postgresPort, database, username, password))
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

    /**
     * Get a connector configuration for the JDBCSinkConnector
     * 
     * @param pgContainer      PostgreSQLContainer instance
     * @param primaryKeyFields comma separated values of the primary key fields
     * @return the connector configuration for JDBC sink
     */
    public ConnectorConfiguration getJdbcSinkConfiguration(PostgreSQLContainer<?> pgContainer, String primaryKeyFields) {
        return getJdbcSinkConfiguration(pgContainer, this.sinkTableName, primaryKeyFields);
    }
}
