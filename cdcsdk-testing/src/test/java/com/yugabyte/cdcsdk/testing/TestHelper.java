package com.yugabyte.cdcsdk.testing;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.io.IOUtils;
import org.testcontainers.containers.GenericContainer;

import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.Ports;
import com.github.dockerjava.api.model.Ports.Binding;
import com.yugabyte.cdcsdk.testing.util.CdcsdkContainer;
import com.yugabyte.cdcsdk.testing.util.UtilStrings;
import com.yugabyte.cdcsdk.testing.util.YBHelper;

import io.debezium.testing.testcontainers.Connector;
import io.debezium.testing.testcontainers.ConnectorConfiguration;

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

public class TestHelper {
    public static GenericContainer<?> getCdcsdkContainerForS3Sink(YBHelper ybHelper, String tableIncludeList) throws Exception {
        return new CdcsdkContainer()
                .withDatabaseHostname(ybHelper.getHostName())
                .withMasterPort(String.valueOf(ybHelper.getMasterPort()))
                .withAwsAccessKeyId(System.getenv("AWS_ACCESS_KEY_ID"))
                .withAwsSecretAccessKey(System.getenv("AWS_SECRET_ACCESS_KEY"))
                .withAwsSessionToken(System.getenv("AWS_SESSION_TOKEN"))
                .withStreamId(ybHelper.getNewDbStreamId(ybHelper.getDatabaseName()))
                .withTableIncludeList(tableIncludeList)
                .buildForS3Sink();
    }

    public static GenericContainer<?> getCdcsdkContainerForPubSubSink(YBHelper ybHelper, String projectId, String tableIncludeList) throws Exception {
        return new CdcsdkContainer()
                .withDatabaseHostname(ybHelper.getHostName())
                .withMasterPort(String.valueOf(ybHelper.getMasterPort()))
                .withProjectId(projectId)
                .withStreamId(ybHelper.getNewDbStreamId(ybHelper.getDatabaseName()))
                .withTableIncludeList(tableIncludeList)
                .buildForPubSubSink();
    }

    public static GenericContainer<?> getCdcsdkContainerForKinesisSink(YBHelper ybHelper, String tableIncludeList) throws Exception {
        return new CdcsdkContainer()
                .withDatabaseHostname(ybHelper.getHostName())
                .withMasterPort(String.valueOf(ybHelper.getMasterPort()))
                .withStreamId(ybHelper.getNewDbStreamId(ybHelper.getDatabaseName()))
                .withTableIncludeList(tableIncludeList)
                .buildForKinesisSink();
    }

    public static GenericContainer<?> getCdcsdkContainerForEventHubSink(YBHelper ybHelper, String tableIncludeList, String connectionString, String hubName)
            throws Exception {
        return new CdcsdkContainer()
                .withDatabaseHostname(ybHelper.getHostName())
                .withMasterPort(String.valueOf(ybHelper.getMasterPort()))
                .withStreamId(ybHelper.getNewDbStreamId(ybHelper.getDatabaseName()))
                .withTableIncludeList(tableIncludeList)
                .withConnectionString(connectionString)
                .withHubName(hubName)
                .buildForEventHubSink();
    }

    public static String executeShellCommand(String command) throws Exception {
        Process process = Runtime.getRuntime().exec(command);
        String stdOutput = IOUtils.toString(process.getInputStream(), StandardCharsets.UTF_8);
        process.destroy();
        return stdOutput;
    }

    /**
     * Insert rows into the source table, the primary keys will be in the range [0, rowsToBeInserted)
     * @param ybHelper {@link YBHelper} object to execute the insert statements
     * @param tableName the source table name
     * @param rowsToBeInserted number of rows to be inserted
     * @throws SQLException if the inserts are not successful
     */
    public static void insertRowsInSourceTable(YBHelper ybHelper, String tableName, int rowsToBeInserted) throws SQLException {
        for (int i = 0; i < rowsToBeInserted; ++i) {
            ybHelper.execute(UtilStrings.getInsertStmt(tableName, i, "first_" + i, "last_" + i, 23.45));
        }
    }

    /**
     * Helper function to assert the values of the passed ResultSet
     * @param rs the {@link ResultSet} instance
     * @param idCol value of the first column - id
     * @param firstNameCol value of the second column - first_name
     * @param lastNameCol value of the third column - last_name
     * @param daysWorkedCol value of the fourth column - days_worked
     * @throws SQLException if the given ResultSet cannot be accessed
     */
    public static void assertValuesInResultSet(ResultSet rs, int idCol, String firstNameCol, String lastNameCol,
                                               double daysWorkedCol)
            throws SQLException {
        assertEquals(idCol, rs.getInt(1));
        assertEquals(firstNameCol, rs.getString(2));
        assertEquals(lastNameCol, rs.getString(3));
        assertEquals(daysWorkedCol, rs.getDouble(4));
    }

    /**
     * Helper function to register connectors to the Kafka Connect container.
     *
     * We cannot use {@code kafkaConnectContainer.registerConnector()} since the command doesn't work
     * after the containers were restarted i.e. in the backend the command still refers to the
     * mapped ports before the restart.
     * @param connectorsEndpoint connector URI
     * @param connectorName name of the connector
     * @param config configuration for the connector to be deployed
     */
    public static void registerConnector(String connectorsEndpoint, String connectorName, ConnectorConfiguration config) {
        final OkHttpClient httpClient = new OkHttpClient();
        final Connector connector = Connector.from(connectorName, config);

        final RequestBody requestBody = RequestBody.create(connector.toJson(), MediaType.get("application/json; charset=utf-8"));
        final Request request = new Request.Builder().url(connectorsEndpoint).post(requestBody).build();

        try (final Response response = httpClient.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw new RuntimeException("Cannot deploy the connector: " + connectorName);
            }
        }
        catch (IOException e) {
            throw new RuntimeException("Error connecting to Debezium container", e);
        }
    }

    /**
     * Helper function to delete connectors from Kafka Connect container.
     * @param connectorUriString connector URI with connector name
     */
    public static void deleteConnector(String connectorUriString) {
        final OkHttpClient httpClient = new OkHttpClient();
        final Request request = new Request.Builder().url(connectorUriString).delete().build();
        try (final Response response = httpClient.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw new RuntimeException("Error deleting the connector");
            }
        }
        catch (IOException e) {
            throw new RuntimeException("Error deleting the connector", e);
        }
    }

    /**
     * Helper function to retrieve the mapped port on the host machine for the given port in the docker container
     * @param container the container instance
     * @param port the exposed port in docker container
     * @return the mapped port on host machine
     */
    public static int getContainerMappedPortFor(GenericContainer<?> container, int port) {
        Ports ports = container.getCurrentContainerInfo().getNetworkSettings().getPorts();
        Binding[] binding = ports.getBindings().get(ExposedPort.tcp(port));
        return Integer.valueOf(binding[0].getHostPortSpec());
    }
}
