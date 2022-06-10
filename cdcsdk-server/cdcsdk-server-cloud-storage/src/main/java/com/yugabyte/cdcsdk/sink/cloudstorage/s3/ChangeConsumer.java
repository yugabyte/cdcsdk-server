package com.yugabyte.cdcsdk.sink.cloudstorage.s3;

import java.io.IOException;
import java.util.Map;

import javax.enterprise.context.Dependent;
import javax.inject.Named;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.AmazonClientException;
import com.yugabyte.cdcsdk.sink.cloudstorage.FlushingChangeConsumer;
import com.yugabyte.cdcsdk.sink.cloudstorage.s3.format.json.JsonFormat;
import com.yugabyte.cdcsdk.sink.cloudstorage.storage.config.StorageCommonConfig;
import com.yugabyte.cdcsdk.sink.cloudstorage.storage.format.Format;
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

    @Override
    protected void createWriter(String base, String path, Map<String, String> props) throws IOException {
        try {
            connectorConfig = new S3SinkConnectorConfig(props);
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
    protected void closeWriter() throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public void write(String value) throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public void flush() throws IOException {
        // TODO Auto-generated method stub

    }
}
