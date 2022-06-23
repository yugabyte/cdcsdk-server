package com.yugabyte.cdcsdk.sink.s3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

import org.apache.kafka.common.config.ConfigException;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.yugabyte.cdcsdk.sink.s3.config.S3SinkConnectorConfig;
import com.yugabyte.cdcsdk.sink.s3.config.StorageCommonConfig;

public class S3SinkConnectorConfigTest extends S3SinkConnectorTestBase {

    protected Map<String, String> localProps = new HashMap<>();

    @BeforeEach
    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    @AfterEach
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        localProps.clear();
    }

    @Override
    protected Map<String, String> createProps() {
        Map<String, String> props = super.createProps();
        props.putAll(localProps);
        return props;
    }

    @Test
    public void testUndefinedURL() {
        properties.remove(StorageCommonConfig.STORE_URL_CONFIG);
        connectorConfig = new S3SinkConnectorConfig(properties);
        assertNull(connectorConfig.getString(StorageCommonConfig.STORE_URL_CONFIG));
    }

    @Test
    public void testConfigurableCredentialProvider() {
        final String ACCESS_KEY_VALUE = "AKIAAAAAKKKKIIIIAAAA";
        final String SECRET_KEY_VALUE = "WhoIsJohnGalt?";

        properties.put(
                S3SinkConnectorConfig.CREDENTIALS_PROVIDER_CLASS_CONFIG,
                DummyAssertiveCredentialsProvider.class.getName());
        String configPrefix = S3SinkConnectorConfig.CREDENTIALS_PROVIDER_CONFIG_PREFIX;
        properties.put(
                configPrefix.concat(DummyAssertiveCredentialsProvider.ACCESS_KEY_NAME),
                ACCESS_KEY_VALUE);
        properties.put(
                configPrefix.concat(DummyAssertiveCredentialsProvider.SECRET_KEY_NAME),
                SECRET_KEY_VALUE);
        properties.put(
                configPrefix.concat(DummyAssertiveCredentialsProvider.CONFIGS_NUM_KEY_NAME),
                "3");
        connectorConfig = new S3SinkConnectorConfig(properties);

        AWSCredentialsProvider credentialsProvider = connectorConfig.getCredentialsProvider();

        assertEquals(ACCESS_KEY_VALUE, credentialsProvider.getCredentials().getAWSAccessKeyId());
        assertEquals(SECRET_KEY_VALUE, credentialsProvider.getCredentials().getAWSSecretKey());
    }

    @Test
    public void testBasicCredentialProvider() {
        final String ACCESS_KEY_VALUE = "AKIAAAAAKKKKIIIIAAAA";
        final String SECRET_KEY_VALUE = "WhoIsJohnGalt?";

        properties.put(S3SinkConnectorConfig.AWS_ACCESS_KEY_ID_CONFIG, ACCESS_KEY_VALUE);
        properties.put(S3SinkConnectorConfig.AWS_SECRET_ACCESS_KEY_CONFIG, SECRET_KEY_VALUE);
        connectorConfig = new S3SinkConnectorConfig(properties);
        S3Storage storage = new S3Storage(connectorConfig, url, S3_TEST_BUCKET_NAME, null);
        AWSCredentialsProvider credentialsProvider = storage.newCredentialsProvider(connectorConfig);

        assertEquals(AWSStaticCredentialsProvider.class, credentialsProvider.getClass());
        assertEquals(ACCESS_KEY_VALUE, credentialsProvider.getCredentials().getAWSAccessKeyId());
        assertEquals(SECRET_KEY_VALUE, credentialsProvider.getCredentials().getAWSSecretKey());
    }

    @Test
    public void testUseExpectContinueDefault() throws Exception {
        setUp();
        S3Storage storage = new S3Storage(connectorConfig, url, S3_TEST_BUCKET_NAME, null);
        ClientConfiguration clientConfig = storage.newClientConfiguration(connectorConfig);
        assertEquals(true, clientConfig.isUseExpectContinue());
    }

    @Test
    public void testUseExpectContinueFalse() throws Exception {
        localProps.put(S3SinkConnectorConfig.HEADERS_USE_EXPECT_CONTINUE_CONFIG, "false");
        setUp();
        S3Storage storage = new S3Storage(connectorConfig, url, S3_TEST_BUCKET_NAME, null);
        ClientConfiguration clientConfig = storage.newClientConfiguration(connectorConfig);
        assertEquals(false, clientConfig.isUseExpectContinue());
    }

    @Test
    public void testConfigurableCredentialProviderMissingConfigs() {
        String configPrefix = S3SinkConnectorConfig.CREDENTIALS_PROVIDER_CONFIG_PREFIX;
        properties.put(
                S3SinkConnectorConfig.CREDENTIALS_PROVIDER_CLASS_CONFIG,
                DummyAssertiveCredentialsProvider.class.getName());
        properties.put(
                configPrefix.concat(DummyAssertiveCredentialsProvider.CONFIGS_NUM_KEY_NAME),
                "2");

        connectorConfig = new S3SinkConnectorConfig(properties);
        assertThrows("are mandatory configuration properties", ConfigException.class,
                () -> connectorConfig.getCredentialsProvider());
    }

    @Test
    public void testConfigurableS3ObjectTaggingConfigs() {
        connectorConfig = new S3SinkConnectorConfig(properties);
        assertEquals(false, connectorConfig.get(S3SinkConnectorConfig.S3_OBJECT_TAGGING_CONFIG));

        properties.put(S3SinkConnectorConfig.S3_OBJECT_TAGGING_CONFIG, "true");
        connectorConfig = new S3SinkConnectorConfig(properties);
        assertEquals(true, connectorConfig.get(S3SinkConnectorConfig.S3_OBJECT_TAGGING_CONFIG));

        properties.put(S3SinkConnectorConfig.S3_OBJECT_TAGGING_CONFIG, "false");
        connectorConfig = new S3SinkConnectorConfig(properties);
        assertEquals(false, connectorConfig.get(S3SinkConnectorConfig.S3_OBJECT_TAGGING_CONFIG));
    }

    @Test
    public void testS3PartRetriesNegative() {
        properties.put(S3SinkConnectorConfig.S3_PART_RETRIES_CONFIG, "-1");
        assertThrows(ConfigException.class, () -> connectorConfig = new S3SinkConnectorConfig(properties),
                "Expected to throw ConfigException");
    }

    @Test
    public void testS3RetryBackoffNegative() {
        properties.put(S3SinkConnectorConfig.S3_RETRY_BACKOFF_CONFIG, "-1");
        assertThrows(ConfigException.class, () -> connectorConfig = new S3SinkConnectorConfig(properties),
                "Expected to throw ConfigException");
    }

    @Test
    public void testInvalidHighCompressionLevel() {
        properties.put(S3SinkConnectorConfig.COMPRESSION_LEVEL_CONFIG, "10");
        assertThrows(ConfigException.class, () -> connectorConfig = new S3SinkConnectorConfig(properties),
                "Expected to throw ConfigException");
    }

    @Test
    public void testInvalidLowCompressionLevel() {
        properties.put(S3SinkConnectorConfig.COMPRESSION_LEVEL_CONFIG, "-2");
        assertThrows(ConfigException.class, () -> connectorConfig = new S3SinkConnectorConfig(properties),
                "Expected to throw ConfigException");
    }

    @Test
    public void testValidCompressionLevels() {
        IntStream.range(-1, 9).boxed().forEach(i -> {
            properties.put(S3SinkConnectorConfig.COMPRESSION_LEVEL_CONFIG, String.valueOf(i));
            connectorConfig = new S3SinkConnectorConfig(properties);
            assertEquals((int) i, connectorConfig.getCompressionLevel());
        });
    }

    @Test
    void testConfigDefConversion() {
        System.setProperty("cdcsdk.sink.s3.aws.access.key.id", "AKIAAAAAKKKKIIIIAAAA");
        System.setProperty("cdcsdk.sink.s3.aws.secret.access.key", "WhoIsJohnGalt?");
        System.setProperty("cdcsdk.sink.s3.bucket.name", "cdcsdk-test");
        System.setProperty("cdcsdk.sink.s3.region", "us-west-2");
        System.setProperty("cdcsdk.sink.s3.basedir", "S3ConsumerIT/");
        System.setProperty("cdcsdk.sink.s3.pattern", "stream_{EPOCH}");
        System.setProperty("cdcsdk.sink.s3.flush.records", "4");
        System.setProperty("cdcsdk.server.transforms", "FLATTEN");

        Config s3Config = ConfigProvider.getConfig();
        S3SinkConnectorConfig connectorConfig = S3ChangeConsumer.microProfileConfigToConfigDef(s3Config);

        assertEquals("AKIAAAAAKKKKIIIIAAAA", connectorConfig.get("aws.access.key.id"));
        assertNotNull(connectorConfig.get("aws.secret.access.key"));
        assertEquals("cdcsdk-test", connectorConfig.get("bucket.name"));
        assertEquals("us-west-2", connectorConfig.get("region"));
        assertEquals(4L, connectorConfig.get("flush.records"));
    }
}
