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

/**
 * Storage format.
 *
 * @param <C> Storage configuration type.
 * @param <T> Type used to discover objects in storage (e.g. Path in HDFS,
 *            String in S3).
 */
public interface Format<C, T> {
    RecordWriterProvider<C> getRecordWriterProvider();
}
