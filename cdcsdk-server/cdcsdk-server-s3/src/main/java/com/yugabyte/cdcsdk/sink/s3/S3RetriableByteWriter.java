/*
 * Copyright 2022 Confluent Inc.
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

import static com.yugabyte.cdcsdk.sink.s3.S3ErrorUtils.throwConnectException;

import java.io.IOException;

/**
 * Wrapper class which may convert an IOException to either a ConnectException
 * or a RetriableException depending upon whether the exception is "retriable"
 * as determined within `throwConnectException()`.
 */
public class S3RetriableByteWriter {
    private final IOByteWriter writer;

    public S3RetriableByteWriter(IOByteWriter writer) {
        if (writer == null) {
            throw new NullPointerException(
                    "S3 Retriable record writer was passed a null writer (IORecordWriter)");
        }
        this.writer = writer;
    }

    public void write(byte[] value) {
        try {
            writer.write(value);
        }
        catch (IOException e) {
            throwConnectException(e);
        }
    }

    public void write(byte[] bytes, int offset, int length) {
        try {
            writer.write(bytes, offset, length);
        }
        catch (IOException e) {
            throwConnectException(e);
        }
    }

    public void commit() {
        try {
            writer.commit();
        }
        catch (IOException e) {
            throwConnectException(e);
        }
    }

    public void close() {
        try {
            writer.close();
        }
        catch (IOException e) {
            throwConnectException(e);
        }
    }
}
