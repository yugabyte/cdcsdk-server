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

import java.io.IOException;
import java.io.OutputStream;

import org.apache.kafka.connect.json.JsonConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yugabyte.cdcsdk.sink.s3.s3.S3OutputStream;
import com.yugabyte.cdcsdk.sink.s3.s3.S3SinkConnectorConfig;
import com.yugabyte.cdcsdk.sink.s3.s3.S3Storage;
import com.yugabyte.cdcsdk.sink.s3.s3.format.RecordViewSetter;
import com.yugabyte.cdcsdk.sink.s3.s3.format.S3RetriableRecordWriter;
import com.yugabyte.cdcsdk.sink.s3.storage.format.RecordWriter;
import com.yugabyte.cdcsdk.sink.s3.storage.format.RecordWriterProvider;

public class JsonRecordWriterProvider extends RecordViewSetter
        implements RecordWriterProvider<S3SinkConnectorConfig> {

    private static final Logger LOGGER = LoggerFactory.getLogger(JsonRecordWriterProvider.class);
    private static final String EXTENSION = ".json";
    private final S3Storage storage;

    JsonRecordWriterProvider(S3Storage storage, JsonConverter converter) {
        this.storage = storage;
    }

    @Override
    public String getExtension() {
        return EXTENSION + storage.conf().getCompressionType().extension;
    }

    @Override
    public RecordWriter getRecordWriter(final S3SinkConnectorConfig conf, final String filename) {
        return new S3RetriableRecordWriter(
                new com.yugabyte.cdcsdk.sink.s3.s3.IORecordWriter() {
                    final S3OutputStream s3out = storage.create(filename, true, JsonFormat.class);
                    final OutputStream s3outWrapper = s3out.wrapForCompression();

                    @Override
                    public void write(byte[] jsonStr) throws IOException {
                        s3outWrapper.write(jsonStr);
                        LOGGER.debug("Wrote {} bytes", jsonStr.length);
                    }

                    @Override
                    public void write(byte[] jsonStr, int offset, int length) throws IOException {
                        s3outWrapper.write(jsonStr, offset, length);
                        LOGGER.debug("Wrote Offset: {}, Length: {}", offset, length);
                    }

                    @Override
                    public void commit() throws IOException {
                        // Flush is required here, because closing the writer will close the underlying
                        // S3
                        // output stream before committing any data to S3.
                        s3out.commit();
                        s3outWrapper.close();
                    }

                    @Override
                    public void close() throws IOException {
                    }
                });
    }
}
