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

package com.yugabyte.cdcsdk.sink.cloudstorage.storage.config;

import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;

import com.yugabyte.cdcsdk.sink.cloudstorage.storage.ComposableConfig;

public class StorageCommonConfig extends AbstractConfig implements ComposableConfig {

    public static final String STORE_URL_CONFIG = "store.url";
    public static final String STORE_URL_DOC = "Store's connection URL, if applicable.";
    public static final String STORE_URL_DEFAULT = null;
    public static final String STORE_URL_DISPLAY = "Store URL";

    public static final String TOPICS_DIR_CONFIG = "topics.dir";
    public static final String TOPICS_DIR_DOC = "Top level directory to store the data ingested from Kafka.";
    public static final String TOPICS_DIR_DEFAULT = "topics";
    public static final String TOPICS_DIR_DISPLAY = "Topics directory";

    public static final String DIRECTORY_DELIM_CONFIG = "directory.delim";
    public static final String DIRECTORY_DELIM_DOC = "Directory delimiter pattern";
    public static final String DIRECTORY_DELIM_DEFAULT = "/";
    public static final String DIRECTORY_DELIM_DISPLAY = "Directory Delimiter";

    public static final String FILE_DELIM_CONFIG = "file.delim";
    public static final String FILE_DELIM_DOC = "File delimiter pattern";
    public static final String FILE_DELIM_DEFAULT = "+";
    public static final String FILE_DELIM_DISPLAY = "File Delimiter";

    /**
     * Create a new configuration definition.
     *
     * @param storageClassRecommender A recommender for storage classes shipping
     *                                out-of-the-box
     *                                with a connector. The recommender should not
     *                                prevent additional custom classes from being
     *                                added during runtime.
     * @return the newly created configuration definition.
     */
    public static ConfigDef newConfigDef() {
        ConfigDef configDef = new ConfigDef();
        {
            // Define Store's basic configuration group
            final String group = "Storage";
            int orderInGroup = 0;

            configDef.define(
                    TOPICS_DIR_CONFIG,
                    Type.STRING,
                    TOPICS_DIR_DEFAULT,
                    Importance.HIGH,
                    TOPICS_DIR_DOC,
                    group,
                    ++orderInGroup,
                    Width.NONE,
                    TOPICS_DIR_DISPLAY);

            configDef.define(
                    STORE_URL_CONFIG,
                    Type.STRING,
                    STORE_URL_DEFAULT,
                    Importance.HIGH,
                    STORE_URL_DOC,
                    group,
                    ++orderInGroup,
                    Width.NONE,
                    STORE_URL_DISPLAY);

            configDef.define(
                    DIRECTORY_DELIM_CONFIG,
                    Type.STRING,
                    DIRECTORY_DELIM_DEFAULT,
                    Importance.MEDIUM,
                    DIRECTORY_DELIM_DOC,
                    group,
                    ++orderInGroup,
                    Width.LONG,
                    DIRECTORY_DELIM_DISPLAY);

            configDef.define(
                    FILE_DELIM_CONFIG,
                    Type.STRING,
                    FILE_DELIM_DEFAULT,
                    Importance.MEDIUM,
                    FILE_DELIM_DOC,
                    group,
                    ++orderInGroup,
                    Width.LONG,
                    FILE_DELIM_DISPLAY);
        }
        return configDef;
    }

    @Override
    public Object get(String key) {
        return super.get(key);
    }

    public StorageCommonConfig(ConfigDef configDef, Map<String, String> props) {
        super(configDef, props);
    }
}
