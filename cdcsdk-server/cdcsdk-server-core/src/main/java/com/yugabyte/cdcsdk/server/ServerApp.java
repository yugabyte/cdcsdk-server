/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.yugabyte.cdcsdk.server;

import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.context.spi.CreationalContext;
import javax.enterprise.event.Observes;
import javax.enterprise.inject.spi.Bean;
import javax.enterprise.inject.spi.BeanManager;
import javax.inject.Inject;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.health.Liveness;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yugabyte.cdcsdk.engine.MTEngine;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.DebeziumEngine.ChangeConsumer;
import io.debezium.engine.format.Avro;
import io.debezium.engine.format.Json;
import io.debezium.engine.format.Protobuf;
import io.debezium.server.ConnectorLifecycle;
import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.Startup;

/**
 * <p>
 * The entry point of the Quarkus-based standalone server. The server is
 * configured via Quarkus/Microprofile Configuration sources
 * and provides few out-of-the-box target implementations.
 * </p>
 * <p>
 * The implementation uses CDI to find all classes that implements
 * {@link DebeziumEngine.ChangeConsumer} interface.
 * The candidate classes should be annotated with {@code @Named} annotation and
 * should be {@code Dependent}.
 * </p>
 * <p>
 * The configuration option {@code debezium.consumer} provides a name of the
 * consumer that should be used and the value
 * must match to exactly one of the implementation classes.
 * </p>
 *
 * @author Jiri Pechanec
 *
 */
@ApplicationScoped
@Startup
public class ServerApp {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServerApp.class);

    // For testing only
    private static final String PROP_DEBEZIUM_PREFIX = "debezium.";
    private static final String PROP_DEBEZIUM_SOURCE_PREFIX = PROP_DEBEZIUM_PREFIX + "source.";

    private static final String PROP_PREFIX = "cdcsdk.";
    private static final String PROP_SERVER_PREFIX = PROP_PREFIX + "server.";
    private static final String PROP_SOURCE_PREFIX = PROP_PREFIX + "source.";
    private static final String PROP_SINK_PREFIX = PROP_PREFIX + "sink.";
    private static final String PROP_FORMAT_PREFIX = PROP_SERVER_PREFIX + "format.";
    private static final String PROP_KEY_FORMAT_PREFIX = PROP_FORMAT_PREFIX + "key.";
    private static final String PROP_VALUE_FORMAT_PREFIX = PROP_FORMAT_PREFIX + "value.";

    private static final String PROP_SINK_TYPE = PROP_SINK_PREFIX + "type";
    private static final String PROP_KEY_FORMAT = PROP_FORMAT_PREFIX + "key";
    private static final String PROP_VALUE_FORMAT = PROP_FORMAT_PREFIX + "value";
    private static final String PROP_TERMINATION_WAIT = PROP_PREFIX + "termination.wait";
    private static final String PROP_OFFSET_STORAGE = PROP_SOURCE_PREFIX + MTEngine.OFFSET_STORAGE;
    private static final String MEMORY_OFFSET_STORAGE = "org.apache.kafka.connect.storage.MemoryOffsetBackingStore";

    private static final String FORMAT_JSON = Json.class.getSimpleName().toLowerCase();
    private static final String FORMAT_AVRO = Avro.class.getSimpleName().toLowerCase();
    private static final String FORMAT_PROTOBUF = Protobuf.class.getSimpleName().toLowerCase();

    private static final Pattern SHELL_PROPERTY_NAME_PATTERN = Pattern.compile("^[a-zA-Z0-9_]+_+[a-zA-Z0-9_]+$");

    private static final String FLATTEN_TRANSFORM = "FLATTEN";

    private static final String PROP_TRANSFORMS = PROP_SERVER_PREFIX + "transforms";
    private static final String PROP_TRANSFORMS_PREFIX = PROP_SERVER_PREFIX + "transforms.";

    private static final String PROP_THREADS = PROP_SERVER_PREFIX + "threads";
    private static final Integer DEFAULT_NUM_THREADS = 1;

    private ExecutorService executor = Executors.newSingleThreadExecutor();

    @Inject
    BeanManager beanManager;

    @Inject
    @Liveness
    ConnectorLifecycle health;

    private Bean<DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>>> consumerBean;
    private CreationalContext<ChangeConsumer<ChangeEvent<Object, Object>>> consumerBeanCreationalContext;
    private DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> consumer;
    private DebeziumEngine<?> engine;
    private final Properties props = new Properties();

    @SuppressWarnings("unchecked")
    @PostConstruct
    public void start() {
        final Config config = ConfigProvider.getConfig();
        final String name = config.getValue(PROP_SINK_TYPE, String.class);

        final Set<Bean<?>> beans = beanManager.getBeans(name).stream()
                .filter(x -> DebeziumEngine.ChangeConsumer.class.isAssignableFrom(x.getBeanClass()))
                .collect(Collectors.toSet());
        LOGGER.debug("Found {} candidate consumer(s)", beans.size());

        if (beans.size() == 0) {
            throw new DebeziumException("No Debezium consumer named '" + name + "' is available");
        }
        else if (beans.size() > 1) {
            throw new DebeziumException("Multiple Debezium consumers named '" + name + "' were found");
        }

        consumerBean = (Bean<DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>>>) beans.iterator().next();
        consumerBeanCreationalContext = beanManager.createCreationalContext(consumerBean);
        consumer = consumerBean.create(consumerBeanCreationalContext);
        LOGGER.info("Consumer '{}' instantiated", consumer.getClass().getName());

        final Class<Any> keyFormat = (Class<Any>) getFormat(config, PROP_KEY_FORMAT);
        final Class<Any> valueFormat = (Class<Any>) getFormat(config, PROP_VALUE_FORMAT);
        configToProperties(config, props, PROP_DEBEZIUM_SOURCE_PREFIX, "");
        configToProperties(config, props, PROP_SOURCE_PREFIX, "");
        configToProperties(config, props, PROP_FORMAT_PREFIX, "key.converter.");
        configToProperties(config, props, PROP_FORMAT_PREFIX, "value.converter.");
        configToProperties(config, props, PROP_KEY_FORMAT_PREFIX, "key.converter.");
        configToProperties(config, props, PROP_VALUE_FORMAT_PREFIX, "value.converter.");
        final Optional<String> transforms = config.getOptionalValue(PROP_TRANSFORMS, String.class);
        if (transforms.isPresent()) {
            LOGGER.debug("Transforms Setting: -{}-", transforms.get());

            if (transforms.get().equals(FLATTEN_TRANSFORM)) {
                LOGGER.info("Enabling FLATTEN transform");
                props.setProperty("transforms", "flatten");
                props.setProperty("transforms.flatten.type", "io.debezium.transforms.ExtractNewRecordState");
                props.setProperty("value.converter.schemas.enable", "false");
            }
            else {
                props.setProperty("transforms", transforms.get());
            }
            configToProperties(config, props, PROP_TRANSFORMS_PREFIX, "transforms.");
        }

        if (!consumer.supportsTombstoneEvents()) {
            props.setProperty(CommonConnectorConfig.TOMBSTONES_ON_DELETE.name(), Boolean.FALSE.toString());
        }

        Integer numThreads = config.getOptionalValue(PROP_THREADS, Integer.class).orElse(DEFAULT_NUM_THREADS);
        LOGGER.info("Number of threads: {}", numThreads);

        props.setProperty("name", name);
        // Set backing store to MemoryOffsetBackingStorage if not set.
        final Optional<String> backingStorage = config.getOptionalValue(PROP_OFFSET_STORAGE, String.class);
        if (!backingStorage.isPresent()) {
            props.setProperty(MTEngine.OFFSET_STORAGE.name(), MEMORY_OFFSET_STORAGE);
            LOGGER.info("CDCSDK Server is running in stateless mode");
        }
        LOGGER.debug("Configuration for DebeziumEngine: {}", props);

        executor = Executors.newFixedThreadPool(numThreads);

        for (int index = 0; index < numThreads; index++) {
            props.setProperty("taskId", String.valueOf(index));
            props.setProperty("maxTasks", String.valueOf(numThreads));
            props.setProperty("offset.storage.file.filename", "data/" + String.valueOf(index));
            engine = DebeziumEngine.create(keyFormat, valueFormat)
                    .notifying(consumer)
                    .using(props)
                    .using((DebeziumEngine.ConnectorCallback) health)
                    .using((DebeziumEngine.CompletionCallback) health)
                    .build();
            executor.execute(() -> {
                try {
                    engine.run();
                }
                finally {
                    Quarkus.asyncExit();
                }
            });
            LOGGER.info("Engine executor {} started", index);
        }
    }

    private void configToProperties(Config config, Properties props, String oldPrefix, String newPrefix) {
        for (String name : config.getPropertyNames()) {
            String updatedPropertyName = null;
            if (SHELL_PROPERTY_NAME_PATTERN.matcher(name).matches()) {
                updatedPropertyName = name.replace("_", ".").toLowerCase();
            }
            if (updatedPropertyName != null && updatedPropertyName.startsWith(oldPrefix)) {
                props.setProperty(newPrefix + updatedPropertyName.substring(oldPrefix.length()),
                        config.getValue(name, String.class));
            }
            else if (name.startsWith(oldPrefix)) {
                LOGGER.trace("Setting {} to {}", newPrefix + name.substring(oldPrefix.length()),
                        config.getValue(name, String.class));
                props.setProperty(newPrefix + name.substring(oldPrefix.length()), config.getValue(name, String.class));
            }
        }
    }

    private Class<?> getFormat(Config config, String property) {
        final String formatName = config.getOptionalValue(property, String.class).orElse(FORMAT_JSON);
        if (FORMAT_JSON.equals(formatName)) {
            return Json.class;
        }
        else if (FORMAT_AVRO.equals(formatName)) {
            return Avro.class;
        }
        else if (FORMAT_PROTOBUF.equals(formatName)) {
            return Protobuf.class;
        }
        throw new DebeziumException("Unknown format '" + formatName + "' for option " + "'" + property + "'");
    }

    public void stop(@Observes ShutdownEvent event) {
        try {
            LOGGER.info("Received request to stop the engine");
            final Config config = ConfigProvider.getConfig();
            engine.close();
            executor.shutdown();
            executor.awaitTermination(config.getOptionalValue(PROP_TERMINATION_WAIT, Integer.class).orElse(10),
                    TimeUnit.SECONDS);
        }
        catch (Exception e) {
            LOGGER.error("Exception while shuttting down Debezium", e);
        }
        consumerBean.destroy(consumer, consumerBeanCreationalContext);
    }

    /**
     * For test purposes only
     */
    DebeziumEngine.ChangeConsumer<?> getConsumer() {
        return consumer;
    }

    public Properties getProps() {
        return props;
    }
}
