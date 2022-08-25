/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server;

import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Event;
import javax.inject.Inject;

import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.Liveness;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yugabyte.cdcsdk.server.Metrics;

import io.debezium.engine.DebeziumEngine;
import io.debezium.server.events.ConnectorCompletedEvent;
import io.debezium.server.events.ConnectorStartedEvent;
import io.debezium.server.events.ConnectorStoppedEvent;
import io.debezium.server.events.TaskStartedEvent;
import io.debezium.server.events.TaskStoppedEvent;
import io.micrometer.core.instrument.Gauge;

/**
 * The server lifecycle listener that published CDI events based on the lifecycle changes and also provides
 * Microprofile Health information.
 *
 * @author Jiri Pechanec
 *
 */
@Liveness
@ApplicationScoped
public class ConnectorLifecycle implements HealthCheck, DebeziumEngine.ConnectorCallback, DebeziumEngine.CompletionCallback {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectorLifecycle.class);

    private AtomicInteger numLive;

    private int numEngines = 0;

    @Inject
    Event<ConnectorStartedEvent> connectorStartedEvent;

    @Inject
    Event<ConnectorStoppedEvent> connectorStoppedEvent;

    @Inject
    Event<TaskStartedEvent> taskStartedEvent;

    @Inject
    Event<TaskStoppedEvent> taskStoppedEvent;

    @Inject
    Event<ConnectorCompletedEvent> connectorCompletedEvent;

    @Inject
    Metrics metrics;

    @PostConstruct
    public void init() {
        Gauge.builder("cdcsdk.server.health", this::getStatusCode).strongReference(true).register(metrics.registry());
        this.numLive = new AtomicInteger(0);
    }

    public void setEngines(int engines) {
        this.numEngines = engines;
    }

    @Override
    public void connectorStarted() {
        LOGGER.debug("Connector started");
        connectorStartedEvent.fire(new ConnectorStartedEvent());
    }

    @Override
    public void connectorStopped() {
        LOGGER.debug("Connector stopped");
        connectorStoppedEvent.fire(new ConnectorStoppedEvent());
    }

    @Override
    public void taskStarted() {
        LOGGER.debug("Task started");
        taskStartedEvent.fire(new TaskStartedEvent());
        this.numLive.incrementAndGet();
    }

    @Override
    public void taskStopped() {
        LOGGER.debug("Task stopped");
        taskStoppedEvent.fire(new TaskStoppedEvent());
        this.numLive.decrementAndGet();
    }

    @Override
    public void handle(boolean success, String message, Throwable error) {
        LOGGER.info("Connector completed: success = '{}', message = '{}', error = '{}'", success, message, error);
        connectorCompletedEvent.fire(new ConnectorCompletedEvent(success, message, error));
        this.numLive.set(0);
    }

    @Override
    public HealthCheckResponse call() {
        LOGGER.trace("Healthcheck called - live = '{}'", numLive);
        return HealthCheckResponse.named("cdcsdk-server").status(this.numLive.get() == this.numEngines).build();
    }

    private int getStatusCode() {
        return (this.numLive.get() == this.numEngines) ? 0 : 1;
    }
}
