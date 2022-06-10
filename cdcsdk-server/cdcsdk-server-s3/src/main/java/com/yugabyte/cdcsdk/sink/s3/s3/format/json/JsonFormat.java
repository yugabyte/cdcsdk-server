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

package com.yugabyte.cdcsdk.sink.s3.s3.format.json;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.json.JsonConverter;

import com.yugabyte.cdcsdk.sink.s3.s3.S3SinkConnectorConfig;
import com.yugabyte.cdcsdk.sink.s3.s3.S3Storage;
import com.yugabyte.cdcsdk.sink.s3.storage.format.Format;
import com.yugabyte.cdcsdk.sink.s3.storage.format.RecordWriterProvider;

public class JsonFormat implements Format<S3SinkConnectorConfig, String> {
    private final S3Storage storage;
    private final JsonConverter converter;

    public JsonFormat(S3Storage storage) {
        this.storage = storage;
        this.converter = new JsonConverter();
        Map<String, Object> converterConfig = new HashMap<>();
        converterConfig.put("schemas.enable", "false");
        converterConfig.put(
                "schemas.cache.size",
                String.valueOf(storage.conf().get(S3SinkConnectorConfig.SCHEMA_CACHE_SIZE_CONFIG)));
        this.converter.configure(converterConfig, false);
    }

    @Override
    public RecordWriterProvider<S3SinkConnectorConfig> getRecordWriterProvider() {
        return new JsonRecordWriterProvider(storage, converter);
    }
}
