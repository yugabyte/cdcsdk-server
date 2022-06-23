package com.yugabyte.cdcsdk.sink.s3;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import javax.annotation.PostConstruct;
import javax.enterprise.context.Dependent;
import javax.inject.Named;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.AmazonClientException;
import com.yugabyte.cdcsdk.sink.s3.buffer.BufferStorage;
import com.yugabyte.cdcsdk.sink.s3.buffer.InMemoryBuffer;
import com.yugabyte.cdcsdk.sink.s3.config.S3SinkConnectorConfig;
import com.yugabyte.cdcsdk.sink.s3.config.StorageCommonConfig;
import com.yugabyte.cdcsdk.sink.s3.streams.RollingOutputStream;
import com.yugabyte.cdcsdk.sink.s3.streams.S3OutputStreamFactory;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.server.BaseChangeConsumer;
import io.debezium.util.Clock;

@Named("s3")
@Dependent
public class S3ChangeConsumer extends BaseChangeConsumer
        implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {

    public static final String PROP_SINK_PREFIX = "cdcsdk.sink.";
    protected static final String PROP_S3_PREFIX = PROP_SINK_PREFIX + "s3.";

    private static final Logger LOGGER = LoggerFactory.getLogger(S3ChangeConsumer.class);

    private S3SinkConnectorConfig connectorConfig;
    private String url;
    private S3Storage storage;

    private static final Pattern SHELL_PROPERTY_NAME_PATTERN = Pattern.compile("^[a-zA-Z0-9_]+_+[a-zA-Z0-9_]+$");

    private long lineSeparatorLength = 0;

    private BufferStorage buffer;
    private S3OutputStreamFactory outputStreamFactory;
    private Roller roller;
    private RollingOutputStream outputStream;

    protected long previousWriteInMillis;
    private final Clock clock = Clock.system();

    public static S3SinkConnectorConfig microProfileConfigToConfigDef(Config config, String oldPrefix,
                                                                      String newPrefix) {
        Map<String, String> configMap = new HashMap<>();
        for (String name : config.getPropertyNames()) {
            String updatedPropertyName = null;
            if (SHELL_PROPERTY_NAME_PATTERN.matcher(name).matches()) {
                updatedPropertyName = name.replace("_", ".").toLowerCase();
            }
            if (updatedPropertyName != null && updatedPropertyName.startsWith(oldPrefix)) {
                configMap.put(newPrefix + updatedPropertyName.substring(oldPrefix.length()),
                        config.getValue(name, String.class));
            }
            else if (name.startsWith(oldPrefix)) {
                LOGGER.trace("Config - {}:{}", newPrefix + name.substring(oldPrefix.length()),
                        config.getValue(name, String.class));
                configMap.put(newPrefix + name.substring(oldPrefix.length()), config.getValue(name, String.class));
            }
        }
        return new S3SinkConnectorConfig(configMap);
    }

    public static S3SinkConnectorConfig microProfileConfigToConfigDef(Config config) {
        return S3ChangeConsumer.microProfileConfigToConfigDef(config, PROP_S3_PREFIX, "");
    }

    @PostConstruct
    protected void connect() throws IOException {
        try {
            final Config config = ConfigProvider.getConfig();
            connectorConfig = S3ChangeConsumer.microProfileConfigToConfigDef(config);
            this.lineSeparatorLength = System.lineSeparator().getBytes(Charset.defaultCharset()).length;

            this.previousWriteInMillis = clock.currentTimeInMillis();
            LOGGER.info("ChangeConsumer Buffer initialized");
            url = connectorConfig.getString(StorageCommonConfig.STORE_URL_CONFIG);

            storage = new S3Storage(connectorConfig, url);
            LOGGER.info("storage created");
            if (!storage.bucketExists()) {
                throw new IOException("Non-existent S3 bucket: " + connectorConfig.getBucketName());
            }
            LOGGER.info("Storage validated");

            this.buffer = new InMemoryBuffer("tmp");
            this.outputStreamFactory = new S3OutputStreamFactory(storage,
                    connectorConfig.getString(S3SinkConnectorConfig.S3_BASEDIR),
                    connectorConfig.getString(S3SinkConnectorConfig.S3_PATTERN));
            this.roller = new Roller(connectorConfig.getLong(S3SinkConnectorConfig.FLUSH_SIZE_CONFIG),
                    connectorConfig.getLong(S3SinkConnectorConfig.FLUSH_RECORDS_CONFIG),
                    connectorConfig.getLong(S3SinkConnectorConfig.ROTATE_INTERVAL_MS_CONFIG));
            this.outputStream = new RollingOutputStream(this.outputStreamFactory, this.buffer, this.roller);

            LOGGER.info("OutputStreams created");
        }
        catch (AmazonClientException e) {
            LOGGER.error(e.getMessage());
            throw new IOException(e);
        }
        catch (Exception exc) {
            LOGGER.error(exc.getMessage());
            throw exc;
        }
    }

    @Override
    public void handleBatch(List<ChangeEvent<Object, Object>> records,
                            DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {
        LOGGER.trace("Handle Batch with {} records", records.size());
        for (ChangeEvent<Object, Object> record : records) {
            LOGGER.trace("Received event '{}'", record);

            if (record.value() != null) {
                String value = (String) record.value();
                try {
                    this.roller.updateStatistics(1,
                            value.getBytes(Charset.defaultCharset()).length + lineSeparatorLength,
                            clock.currentTimeInMillis() - this.previousWriteInMillis);

                    this.outputStream.write(value.getBytes());
                    this.outputStream.write(System.lineSeparator().getBytes());
                }
                catch (IOException ioe) {
                    LOGGER.error(ioe.getMessage());
                    throw new InterruptedException(ioe.toString());
                }
            }
        }
    }
}
