package com.yugabyte.cdcsdk.testing;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.json.JSONObject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.yugabyte.cdcsdk.testing.util.CdcsdkTestBase;
import com.yugabyte.cdcsdk.testing.util.UtilStrings;

public class HealthChecksIT extends CdcsdkTestBase {
    private int healthCheckMappedPort;

    @BeforeAll
    public static void beforeAll() throws Exception {
        initializeContainers();

        // We only need the Kafka container to be started here as the CDCSDK server will try to
        // establish a connection to it
        kafkaContainer.start();

        initHelpers(true, true, false);
    }

    @BeforeEach
    public void beforeEachTest() throws Exception {
        // Create a table in YugabyteDB
        ybHelper.execute(UtilStrings.getCreateTableYBStmt(DEFAULT_TABLE_NAME));

        cdcsdkContainer = kafkaHelper.getCdcsdkContainer(ybHelper, "public." + DEFAULT_TABLE_NAME, 1);
        cdcsdkContainer.withNetwork(containerNetwork);
        cdcsdkContainer.start();

        healthCheckMappedPort = cdcsdkContainer.getMappedPort(8080);
    }

    @AfterEach
    public void afterEachTest() throws Exception {
        // Stop the CDCSDK Server container
        cdcsdkContainer.stop();

        // Delete the table in YugabyteDB
        ybHelper.execute(UtilStrings.getDropTableStmt(DEFAULT_TABLE_NAME));
    }

    @Test
    public void liveHealthCheckWhenContainerIsRunning() throws Exception {
        String output = TestHelper.executeShellCommand("curl http://localhost:" + healthCheckMappedPort + "/q/health/live");

        JSONObject obj = new JSONObject(output);
        assertEquals("UP", obj.getString("status"));
    }

    @Test
    public void readyHealthCheckWhenContainerIsRunning() throws Exception {
        String output = TestHelper.executeShellCommand("curl http://localhost:" + healthCheckMappedPort + "/q/health/ready");

        JSONObject obj = new JSONObject(output);
        assertEquals("UP", obj.getString("status"));
    }

    @AfterAll
    public static void afterAll() throws Exception {
        kafkaContainer.stop();
    }
}
