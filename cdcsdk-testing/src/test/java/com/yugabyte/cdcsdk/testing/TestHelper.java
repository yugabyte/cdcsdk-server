package com.yugabyte.cdcsdk.testing;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;

import com.yugabyte.cdcsdk.testing.util.CdcsdkContainer;
import com.yugabyte.cdcsdk.testing.util.UtilStrings;
import com.yugabyte.cdcsdk.testing.util.YBHelper;

public class TestHelper {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestHelper.class);

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
}
