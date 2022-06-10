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

package com.yugabyte.cdcsdk.sink.s3.storage.config;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;

import com.yugabyte.cdcsdk.sink.s3.storage.ComposableConfig;

public class StorageSinkConnectorConfig extends AbstractConfig implements ComposableConfig {

    // Connector group
    public static final String ROTATE_INTERVAL_MS_CONFIG = "rotate.interval.ms";
    public static final String ROTATE_INTERVAL_MS_DOC = "The time interval in milliseconds to invoke file commits. You can configure this parameter"
            + " so that the time interval is determined by using a timestamp extractor (for "
            + "example, Kafka Record Time, Record Field, or Wall Clock extractor). When the first "
            + "record is processed, a timestamp is set as the base time. This is useful if you "
            + "require exactly-once-semantics. This configuration ensures that file commits are "
            + "invoked at every configured interval. The default value ``-1`` indicates that this "
            + "feature is disabled.";
    public static final long ROTATE_INTERVAL_MS_DEFAULT = -1L;
    public static final String ROTATE_INTERVAL_MS_DISPLAY = "Rotate Interval (ms)";

    public static final String ROTATE_SCHEDULE_INTERVAL_MS_CONFIG = "rotate.schedule.interval.ms";
    public static final String ROTATE_SCHEDULE_INTERVAL_MS_DOC = "The time interval in milliseconds to periodically invoke file commits. This configuration "
            + "ensures that file commits are invoked at every configured interval. Time of commit "
            + "will be adjusted to 00:00 of selected timezone. The commit will be performed at the "
            + "scheduled time, regardless of the previous commit time or number of messages. This "
            + "configuration is useful when you have to commit your data based on current server "
            + "time, for example at the beginning of every hour. The default value ``-1`` means "
            + "that this feature is disabled.";
    public static final long ROTATE_SCHEDULE_INTERVAL_MS_DEFAULT = -1L;
    public static final String ROTATE_SCHEDULE_INTERVAL_MS_DISPLAY = "Rotate Schedule Interval (ms)";

    public static final String RETRY_BACKOFF_CONFIG = "retry.backoff.ms";
    public static final String RETRY_BACKOFF_DOC = "The retry backoff in milliseconds. This config is used to notify Kafka connect to retry "
            + "delivering a message batch or performing recovery in case of transient exceptions.";
    public static final long RETRY_BACKOFF_DEFAULT = 5000L;
    public static final String RETRY_BACKOFF_DISPLAY = "Retry Backoff (ms)";

    public static final String SHUTDOWN_TIMEOUT_CONFIG = "shutdown.timeout.ms";
    public static final String SHUTDOWN_TIMEOUT_DOC = "Clean shutdown timeout. This makes sure that asynchronous Hive metastore updates are "
            + "completed during connector shutdown.";
    public static final long SHUTDOWN_TIMEOUT_DEFAULT = 3000L;
    public static final String SHUTDOWN_TIMEOUT_DISPLAY = "Shutdown Timeout (ms)";

    public static final String FILENAME_OFFSET_ZERO_PAD_WIDTH_CONFIG = "filename.offset.zero.pad.width";
    public static final String FILENAME_OFFSET_ZERO_PAD_WIDTH_DOC = "Width to zero pad offsets in store's filenames if offsets are too short in order to "
            + "provide fixed width filenames that can be ordered by simple lexicographic sorting.";
    public static final int FILENAME_OFFSET_ZERO_PAD_WIDTH_DEFAULT = 10;
    public static final String FILENAME_OFFSET_ZERO_PAD_WIDTH_DISPLAY = "Filename Offset Zero Pad Width";

    public static final String SCHEMA_CACHE_SIZE_CONFIG = "schemas.cache.config";
    public static final int SCHEMA_CACHE_SIZE_DEFAULT = 1000;
    public static final String SCHEMA_CACHE_SIZE_DOC = "Size of the converted schemas cache";
    public static final String SCHEMA_CACHE_SIZE_DISPLAY = "Schema Cache Size";

    public static final String CONNECT_META_DATA_CONFIG = "connect.meta.data";
    public static final boolean CONNECT_META_DATA_DEFAULT = true;
    public static final String CONNECT_META_DATA_DOC = "Allow connect converter to add its metadata to the output schema";
    public static final String CONNECT_META_DATA_DISPLAY = "Connect Metadata";

    // Schema group
    public static final String SCHEMA_COMPATIBILITY_CONFIG = "schema.compatibility";
    public static final String SCHEMA_COMPATIBILITY_DOC = "The schema compatibility rule to use when the connector is observing schema changes. The "
            + "supported configurations are NONE, BACKWARD, FORWARD and FULL.";
    public static final String SCHEMA_COMPATIBILITY_DEFAULT = "NONE";
    public static final String SCHEMA_COMPATIBILITY_DISPLAY = "Schema Compatibility";

    // CHECKSTYLE:OFF
    public static final ConfigDef.Recommender schemaCompatibilityRecommender = new SchemaCompatibilityRecommender();
    // CHECKSTYLE:ON

    /**
     * Create a new configuration definition.
     *
     * @param formatClassRecommender A recommender for format classes shipping
     *                               out-of-the-box with
     *                               a connector. The recommender should not prevent
     *                               additional custom classes from being
     *                               added during runtime.
     * @return the newly created configuration definition.
     */
    public static ConfigDef newConfigDef() {
        ConfigDef configDef = new ConfigDef();
        {
            // Define Store's basic configuration group
            final String group = "Connector";
            int orderInGroup = 0;

            configDef.define(
                    ROTATE_INTERVAL_MS_CONFIG,
                    Type.LONG,
                    ROTATE_INTERVAL_MS_DEFAULT,
                    Importance.HIGH,
                    ROTATE_INTERVAL_MS_DOC,
                    group,
                    ++orderInGroup,
                    Width.MEDIUM,
                    ROTATE_INTERVAL_MS_DISPLAY);

            configDef.define(
                    ROTATE_SCHEDULE_INTERVAL_MS_CONFIG,
                    Type.LONG,
                    ROTATE_SCHEDULE_INTERVAL_MS_DEFAULT,
                    Importance.MEDIUM,
                    ROTATE_SCHEDULE_INTERVAL_MS_DOC,
                    group,
                    ++orderInGroup,
                    Width.MEDIUM,
                    ROTATE_SCHEDULE_INTERVAL_MS_DISPLAY);

            configDef.define(
                    SCHEMA_CACHE_SIZE_CONFIG,
                    Type.INT,
                    SCHEMA_CACHE_SIZE_DEFAULT,
                    Importance.LOW,
                    SCHEMA_CACHE_SIZE_DOC,
                    group,
                    ++orderInGroup,
                    Width.LONG,
                    SCHEMA_CACHE_SIZE_DISPLAY);

            configDef.define(
                    CONNECT_META_DATA_CONFIG,
                    Type.BOOLEAN,
                    CONNECT_META_DATA_DEFAULT,
                    Importance.LOW,
                    CONNECT_META_DATA_DOC,
                    group,
                    ++orderInGroup,
                    Width.SHORT,
                    CONNECT_META_DATA_DISPLAY);

            configDef.define(
                    RETRY_BACKOFF_CONFIG,
                    Type.LONG,
                    RETRY_BACKOFF_DEFAULT,
                    Importance.LOW,
                    RETRY_BACKOFF_DOC,
                    group,
                    ++orderInGroup,
                    Width.MEDIUM,
                    RETRY_BACKOFF_DISPLAY);

            configDef.define(
                    SHUTDOWN_TIMEOUT_CONFIG,
                    Type.LONG,
                    SHUTDOWN_TIMEOUT_DEFAULT,
                    Importance.MEDIUM,
                    SHUTDOWN_TIMEOUT_DOC,
                    group,
                    ++orderInGroup,
                    Width.MEDIUM,
                    SHUTDOWN_TIMEOUT_DISPLAY);

            configDef.define(
                    FILENAME_OFFSET_ZERO_PAD_WIDTH_CONFIG,
                    Type.INT,
                    FILENAME_OFFSET_ZERO_PAD_WIDTH_DEFAULT,
                    ConfigDef.Range.atLeast(0),
                    Importance.LOW,
                    FILENAME_OFFSET_ZERO_PAD_WIDTH_DOC,
                    group,
                    ++orderInGroup,
                    Width.LONG,
                    FILENAME_OFFSET_ZERO_PAD_WIDTH_DISPLAY);
        }

        {
            // Define Schema configuration group
            final String group = "Schema";
            int orderInGroup = 0;

            // Define Schema configuration group
            configDef.define(
                    SCHEMA_COMPATIBILITY_CONFIG,
                    Type.STRING,
                    SCHEMA_COMPATIBILITY_DEFAULT,
                    Importance.HIGH,
                    SCHEMA_COMPATIBILITY_DOC,
                    group,
                    ++orderInGroup,
                    Width.SHORT,
                    SCHEMA_COMPATIBILITY_DISPLAY,
                    schemaCompatibilityRecommender);
        }
        return configDef;
    }

    public static class SchemaCompatibilityRecommender extends BooleanParentRecommender {

        public SchemaCompatibilityRecommender() {
            super("hive.integration");
        }

        @Override
        public List<Object> validValues(String name, Map<String, Object> connectorConfigs) {
            Boolean hiveIntegration = (Boolean) connectorConfigs.get(parentConfigName);
            if (hiveIntegration != null && hiveIntegration) {
                return Arrays.asList("BACKWARD", "FORWARD", "FULL");
            }
            else {
                return Arrays.asList("NONE", "BACKWARD", "FORWARD", "FULL");
            }
        }

        @Override
        public boolean visible(String name, Map<String, Object> connectorConfigs) {
            return true;
        }
    }

    public static class BooleanParentRecommender implements ConfigDef.Recommender {

        protected final String parentConfigName;

        public BooleanParentRecommender(String parentConfigName) {
            this.parentConfigName = parentConfigName;
        }

        @Override
        public List<Object> validValues(String name, Map<String, Object> connectorConfigs) {
            return new LinkedList<>();
        }

        @Override
        public boolean visible(String name, Map<String, Object> connectorConfigs) {
            return (boolean) connectorConfigs.get(parentConfigName);
        }
    }

    @Override
    public Object get(String key) {
        return super.get(key);
    }

    public StorageSinkConnectorConfig(ConfigDef configDef, Map<String, String> props) {
        super(configDef, props);
    }
}
