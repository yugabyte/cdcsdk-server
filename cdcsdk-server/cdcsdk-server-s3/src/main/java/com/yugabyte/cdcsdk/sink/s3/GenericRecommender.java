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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import org.apache.kafka.common.config.ConfigDef;

public class GenericRecommender implements ConfigDef.Recommender {
    // We need a set to avoid duplicates
    private final Set<Object> validValues = new CopyOnWriteArraySet<>();

    public void addValidValues(Collection<Object> values) {
        validValues.addAll(values);
    }

    @Override
    public List<Object> validValues(String name, Map<String, Object> connectorConfigs) {
        return new ArrayList<>(validValues);
    }

    @Override
    public boolean visible(String name, Map<String, Object> connectorConfigs) {
        return true;
    }
}
