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

package com.yugabyte.cdcsdk.sink.cloudstorage.storage.format;

import java.io.Closeable;

/**
 * Storage specific RecordWriter.
 */
public interface RecordWriter extends Closeable {
    /**
     * Write a record to storage.
     *
     * @param record the record to persist.
     */
    void write(byte[] value);

    void write(byte[] value, int offset, int length);

    /**
     * Close this writer.
     */
    void close();

    /**
     * Flush writer's data and commit the records in Kafka. Optionally, this
     * operation might also
     * close the writer.
     */
    void commit();
}
