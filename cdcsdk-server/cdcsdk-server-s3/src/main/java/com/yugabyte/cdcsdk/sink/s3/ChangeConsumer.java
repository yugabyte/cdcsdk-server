package com.yugabyte.cdcsdk.sink.s3;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
import com.yugabyte.cdcsdk.sink.s3.storage.config.StorageCommonConfig;
import com.yugabyte.cdcsdk.sink.s3.storage.format.RecordWriter;

@Named("s3")
@Dependent
public class ChangeConsumer extends FlushingChangeConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(ChangeConsumer.class);

    private S3SinkConnectorConfig connectorConfig;
    private String url;
    private long timeoutMs;
    private S3Storage storage;

    private static final Pattern SHELL_PROPERTY_NAME_PATTERN = Pattern.compile("^[a-zA-Z0-9_]+_+[a-zA-Z0-9_]+$");
    private Map<String, String> props = new HashMap<>();

    private RecordWriter writer;

    public static Map<String, String> configToMap(Config config, String oldPrefix, String newPrefix) {
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
                configMap.put(newPrefix + name.substring(oldPrefix.length()), config.getValue(name, String.class));
            }
        }
        return configMap;
    }

    @PostConstruct
    protected void connect() throws IOException {
        try {
            super.connect();
            final Config config = ConfigProvider.getConfig();
            this.props = ChangeConsumer.configToMap(config, PROP_PREFIX, "");

            connectorConfig = new S3SinkConnectorConfig(this.props);
            url = connectorConfig.getString(StorageCommonConfig.STORE_URL_CONFIG);
            timeoutMs = connectorConfig.getLong(S3SinkConnectorConfig.RETRY_BACKOFF_CONFIG);

            storage = new S3Storage(connectorConfig, url);
            LOGGER.info("storage created");
            if (!storage.bucketExists()) {
                throw new IOException("Non-existent S3 bucket: " + connectorConfig.getBucketName());
            }
            LOGGER.info("Storage validated");

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
        this.writer = this.getRecordWriter(connectorConfig, commitFilename);
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

    private RecordWriter getRecordWriter(final S3SinkConnectorConfig conf, final String filename) {
        return new S3RetriableRecordWriter(
                new com.yugabyte.cdcsdk.sink.s3.IORecordWriter() {
                    final S3OutputStream s3out = storage.create(filename, true);
                    final OutputStream s3outWrapper = s3out.wrapForCompression();

                    @Override
                    public void write(byte[] jsonStr) throws IOException {
                        s3outWrapper.write(jsonStr);
                        LOGGER.debug("Wrote {} bytes", jsonStr.length);
                    }

                    @Override
                    public void write(byte[] jsonStr, int offset, int length) throws IOException {
                        s3outWrapper.write(jsonStr, offset, length);
                        LOGGER.debug("Wrote Offset: {}, Length: {}", offset, length);
                    }

                    @Override
                    public void commit() throws IOException {
                        // Flush is required here, because closing the writer will close the underlying
                        // S3
                        // output stream before committing any data to S3.
                        s3out.commit();
                        s3outWrapper.close();
                    }

                    @Override
                    public void close() throws IOException {
                    }
                });
    }
}
