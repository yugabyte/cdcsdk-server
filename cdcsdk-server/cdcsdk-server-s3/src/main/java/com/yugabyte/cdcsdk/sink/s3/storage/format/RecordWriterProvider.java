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

package com.yugabyte.cdcsdk.sink.s3.storage.format;

/**
 * Provider of a record writer for this storage.
 *
 * @param <C> Storage configuration type.
 */
public interface RecordWriterProvider<C> {
    String getExtension();

    RecordWriter getRecordWriter(C conf, String fileName);
}
