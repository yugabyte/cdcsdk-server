package com.yugabyte.cdcsdk.sink.cloudstorage.s3;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
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
import com.yugabyte.cdcsdk.sink.cloudstorage.FlushingChangeConsumer;
import com.yugabyte.cdcsdk.sink.cloudstorage.s3.format.json.JsonFormat;
import com.yugabyte.cdcsdk.sink.cloudstorage.storage.config.StorageCommonConfig;
import com.yugabyte.cdcsdk.sink.cloudstorage.storage.format.Format;
import com.yugabyte.cdcsdk.sink.cloudstorage.storage.format.RecordWriter;
import com.yugabyte.cdcsdk.sink.cloudstorage.storage.format.RecordWriterProvider;

@Named("s3")
@Dependent
public class ChangeConsumer extends FlushingChangeConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(ChangeConsumer.class);

    private S3SinkConnectorConfig connectorConfig;
    private String url;
    private long timeoutMs;
    private S3Storage storage;
    private Format<S3SinkConnectorConfig, String> format;
    private RecordWriterProvider<S3SinkConnectorConfig> writerProvider;

    private static final Pattern SHELL_PROPERTY_NAME_PATTERN = Pattern.compile("^[a-zA-Z0-9_]+_+[a-zA-Z0-9_]+$");
    private Map<String, String> props = new HashMap<>();

    private RecordWriter writer;

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

    @PostConstruct
    protected void connect() throws IOException {
        try {
            super.connect();
            final Config config = ConfigProvider.getConfig();
            this.configToMap(config, PROP_PREFIX, "");

            connectorConfig = new S3SinkConnectorConfig(this.props);
            url = connectorConfig.getString(StorageCommonConfig.STORE_URL_CONFIG);
            timeoutMs = connectorConfig.getLong(S3SinkConnectorConfig.RETRY_BACKOFF_CONFIG);

            storage = new S3Storage(connectorConfig, url);
            LOGGER.info("storage created");
            if (!storage.bucketExists()) {
                throw new IOException("Non-existent S3 bucket: " + connectorConfig.getBucketName());
            }
            LOGGER.info("Storage validated");
            writerProvider = new JsonFormat(storage).getRecordWriterProvider();

            LOGGER.info("Started S3 connector task with assigned partitions: {}", storage.toString());
        }
        catch (AmazonClientException e) {
            throw new IOException(e);
        }
    }

    @Override
    protected void createWriter(String base, String path) throws IOException {
        String commitFilename = base + path;
        LOGGER.info(
                "Creating new writer filename='{}'",
                commitFilename);
        this.writer = writerProvider.getRecordWriter(connectorConfig, commitFilename);
    }

    @Override
    protected void closeWriter() throws IOException {
        this.writer.commit();
        this.writer.close();
    }

    @Override
    public void write(InputStream is) throws IOException {
        byte[] buffer = new byte[1024]; // Adjust if you want
        int bytesRead;
        while ((bytesRead = is.read(buffer)) != -1) {
            this.writer.write(buffer, 0, bytesRead);
        }
    }
}
