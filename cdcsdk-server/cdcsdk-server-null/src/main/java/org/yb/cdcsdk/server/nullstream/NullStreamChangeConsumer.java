/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.yb.cdcsdk.server.nullstream;

import java.util.List;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.Dependent;
import javax.inject.Inject;
import javax.inject.Named;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.DebeziumEngine.RecordCommitter;
import io.debezium.server.BaseChangeConsumer;
import io.debezium.server.CustomConsumerBuilder;

/**
 * Implementation of the consumer that delivers the messages into Redis (stream) destination.
 *
 * @author M Sazzadul Hoque
 */
@Named("nullStream")
@Dependent
public class NullStreamChangeConsumer extends BaseChangeConsumer
        implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(NullStreamChangeConsumer.class);

    private long numLogged = 0;
    private long totalRecords = 0;

    @Inject
    @CustomConsumerBuilder

    @PostConstruct
    void connect() {
        LOGGER.info("Instaintiated NULL consumer");
    }

    @PreDestroy
    void close() {
        LOGGER.info("Total records processed = {}", totalRecords);
    }

    @Override
    public void handleBatch(List<ChangeEvent<Object, Object>> records,
                            RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {

        totalRecords += records.size();
        if (totalRecords / 100000 > numLogged) {
            LOGGER.info("Log Number: {}. Total records processed = {}.", numLogged, totalRecords);
            numLogged++;
        }
        committer.markBatchFinished();
    }
}
