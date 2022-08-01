package com.yugabyte.cdcsdk.testing;

import java.nio.charset.StandardCharsets;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;

import com.yugabyte.cdcsdk.testing.util.CdcsdkContainer;
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
}
