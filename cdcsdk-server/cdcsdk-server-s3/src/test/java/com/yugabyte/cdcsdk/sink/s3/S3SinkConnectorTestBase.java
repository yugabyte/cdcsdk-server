/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.yugabyte.cdcsdk.sink.s3;

import java.util.Map;

import org.junit.Rule;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.yugabyte.cdcsdk.sink.s3.config.S3SinkConnectorConfig;

import io.confluent.common.utils.SystemTime;
import io.confluent.common.utils.Time;

public class S3SinkConnectorTestBase extends StorageSinkTestBase {

    private static final Logger log = LoggerFactory.getLogger(S3SinkConnectorTestBase.class);

    protected static final String S3_TEST_URL = "http://127.0.0.1:8181";
    protected static final String S3_TEST_BUCKET_NAME = "kafka.bucket";
    protected static final Time SYSTEM_TIME = new SystemTime();

    protected S3SinkConnectorConfig connectorConfig;
    protected String topicsDir;
    protected Map<String, Object> parsedConfig;

    @Rule
    public TestRule watcher = new TestWatcher() {
        @Override
        protected void starting(Description description) {
            log.info(
                    "Starting test: {}.{}",
                    description.getTestClass().getSimpleName(),
                    description.getMethodName());
        }
    };

    @Override
    protected Map<String, String> createProps() {
        url = S3_TEST_URL;
        Map<String, String> props = super.createProps();
        props.put(S3SinkConnectorConfig.S3_BUCKET_CONFIG, S3_TEST_BUCKET_NAME);
        props.put(S3SinkConnectorConfig.S3_BASEDIR, "baseDir");
        props.put(S3SinkConnectorConfig.S3_PATTERN, "pattern");
        props.put(S3SinkConnectorConfig.S3_PATH_STYLE_ACCESS_ENABLED_CONFIG, "false");
        return props;
    }

    @BeforeEach
    @Override
    public void setUp() throws Exception {
        super.setUp();
        connectorConfig = new S3SinkConnectorConfig(properties);
    }

    @AfterEach
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    public AmazonS3 newS3Client(S3SinkConnectorConfig config) {
        AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard()
                .withAccelerateModeEnabled(config.getBoolean(S3SinkConnectorConfig.WAN_MODE_CONFIG))
                .withPathStyleAccessEnabled(
                        config.getBoolean(S3SinkConnectorConfig.S3_PATH_STYLE_ACCESS_ENABLED_CONFIG))
                .withCredentials(new DefaultAWSCredentialsProviderChain());

        builder = url == null ? builder.withRegion(config.getString(S3SinkConnectorConfig.REGION_CONFIG))
                : builder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(url, ""));

        return builder.build();
    }

}
