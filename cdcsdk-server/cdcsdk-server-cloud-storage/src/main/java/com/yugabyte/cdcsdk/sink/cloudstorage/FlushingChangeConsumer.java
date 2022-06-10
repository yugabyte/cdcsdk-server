/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.yugabyte.cdcsdk.sink.cloudstorage;

import java.io.IOException;
import java.nio.charset.Charset;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import javax.annotation.PostConstruct;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.server.BaseChangeConsumer;
import io.debezium.util.Clock;

/**
 * Basic services provided to all change consumers that want to track size of
 * data written and time
 * elapsed.
 *
 * @author Rajat Venkatesh
 *
 */
public abstract class FlushingChangeConsumer extends BaseChangeConsumer
        implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(FlushingChangeConsumer.class);

    protected String baseDir;
    protected String pattern;

    protected long totalBytesWritten = 0;
    protected long totalRecordsWritten = 0;
    protected long totalTimeElapsedMs = 0;

    protected long currentBytesWritten = 0;
    protected long currentRecordsWritten = 0;
    protected Instant previousFlushInstant;

    protected long flushBytesWritten = 0;
    protected long flushRecordsWritten = 0;
    protected Duration flushDuration;

    private static final Long FLUSH_BYTES_WRITTEN_DEFAULT = 200L * 1024 * 1024 * 1024;
    private static final Long FLUSH_RECORDS_WRITTEN_DEFAULT = 10000L;
    private static final Long FLUSH_DURATION_DEFAULT = 3600000L; // Default to 60minutes

    private final Clock clock = Clock.system();

    private static final String PROP_PREFIX = "cdcsdk.sink.storage.";
    private static final String PROP_BASE_DIR = "basedir";
    private static final String PROP_PATTERN = "pattern";
    private static final String PROP_FLUSH_BYTES = "flushMB";
    private static final String PROP_FLUSH_RECORDS = "flushRecords";
    private static final String PROP_FLUSH_SECONDS = "flushSeconds";

    private static final Pattern SHELL_PROPERTY_NAME_PATTERN = Pattern.compile("^[a-zA-Z0-9_]+_+[a-zA-Z0-9_]+$");

    private long lineSeparatorLength = 0;

    private Map<String, String> props = new HashMap<>();

    @PostConstruct
    void connect() throws IOException {
        final Config config = ConfigProvider.getConfig();
        this.baseDir = config.getValue(PROP_PREFIX + PROP_BASE_DIR, String.class);
        this.pattern = config.getValue(PROP_PREFIX + PROP_PATTERN, String.class);

        flushBytesWritten = FLUSH_BYTES_WRITTEN_DEFAULT;
        flushRecordsWritten = FLUSH_RECORDS_WRITTEN_DEFAULT;
        flushDuration = Duration.millis(FLUSH_DURATION_DEFAULT);

        config.getOptionalValue(PROP_PREFIX + PROP_FLUSH_BYTES, String.class)
                .ifPresent(t -> flushBytesWritten = Long.parseLong(t));
        config.getOptionalValue(PROP_PREFIX + PROP_FLUSH_RECORDS, String.class)
                .ifPresent(t -> flushRecordsWritten = Long.parseLong(t));
        config.getOptionalValue(PROP_PREFIX + PROP_FLUSH_SECONDS, String.class)
                .ifPresent(t -> flushDuration = Duration.millis(Long.parseLong(t)));

        this.lineSeparatorLength = System.lineSeparator().getBytes(Charset.defaultCharset()).length;

        this.configToMap(config, PROP_PREFIX, "");

        LOGGER.info("Properties:");
        props.forEach((k, v) -> LOGGER.info("{}:{}", k, v));
        this.newWriter();
        previousFlushInstant = clock.currentTimeAsInstant();
    }

    @Override
    public void handleBatch(List<ChangeEvent<Object, Object>> records,
                            DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {
        totalRecordsWritten += records.size();
        currentRecordsWritten += records.size();

        for (ChangeEvent<Object, Object> record : records) {
            LOGGER.trace("Received event '{}'", record);

            if (record.value() != null) {
                String value = (String) record.value();
                try {
                    this.write(value);
                    this.write(System.lineSeparator());
                    long bytesWritten = value.getBytes(Charset.defaultCharset()).length + lineSeparatorLength;
                    totalBytesWritten += bytesWritten;
                    currentBytesWritten += bytesWritten;
                }
                catch (IOException ioe) {
                    throw new InterruptedException(ioe.toString());
                }
            }
        }
        try {
            this.maybeFlush();
        }
        catch (IOException ioe) {
            throw new InterruptedException(ioe.toString());
        }
    }

    protected void maybeFlush() throws IOException {
        if (currentBytesWritten >= flushBytesWritten || currentRecordsWritten >= flushRecordsWritten) {
            this.flush();
            this.closeWriter();
            this.newWriter();

            LOGGER.info("Total Statistics: BytesWritten: {}, RecordsWritten: {}", this.totalBytesWritten,
                    this.totalRecordsWritten);
            LOGGER.info("Batch Statistics: BytesWritten: {}, RecordsWritten: {}", this.currentBytesWritten,
                    this.currentRecordsWritten);

        }
    }

    private void newWriter() throws IOException {
        final DateTime sync_datetime = DateTime.now(DateTimeZone.UTC);
        final String base = NamePatternResolver.resolvePath(sync_datetime, this.baseDir);
        final String path = NamePatternResolver.resolvePath(sync_datetime, this.pattern);

        this.createWriter(base, path, this.props);
        LOGGER.info("Created new writer at {}", sync_datetime);
    }

    private void configToMap(Config config, String oldPrefix, String newPrefix) {
        for (String name : config.getPropertyNames()) {
            String updatedPropertyName = null;
            if (SHELL_PROPERTY_NAME_PATTERN.matcher(name).matches()) {
                updatedPropertyName = name.replace("_", ".").toLowerCase();
            }
            if (updatedPropertyName != null && updatedPropertyName.startsWith(oldPrefix)) {
                this.props.put(newPrefix + updatedPropertyName.substring(oldPrefix.length()),
                        config.getValue(name, String.class));
            }
            else if (name.startsWith(oldPrefix)) {
                this.props.put(newPrefix + name.substring(oldPrefix.length()), config.getValue(name, String.class));
            }
        }
    }

    protected abstract void createWriter(String base, String path, Map<String, String> props) throws IOException;

    protected abstract void closeWriter() throws IOException;

    public abstract void write(String value) throws IOException;

    public abstract void flush() throws IOException;
}
