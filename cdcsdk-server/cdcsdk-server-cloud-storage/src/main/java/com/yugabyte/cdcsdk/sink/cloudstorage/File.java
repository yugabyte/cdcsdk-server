package com.yugabyte.cdcsdk.sink.cloudstorage;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import javax.annotation.PostConstruct;
import javax.enterprise.context.Dependent;
import javax.inject.Named;

import org.apache.commons.io.FileUtils;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.server.BaseChangeConsumer;

@Named("file")
@Dependent
public class File extends BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(File.class);

    private static final String PROP_PREFIX = "cdcsdk.sink.file.";
    private static final String PROP_TYPE = "type";
    private static final String PROP_BASE_DIR = "basedir";
    private static final String PROP_STREAM_PATTERN = "pattern";

    private Writer writer = null;

    @PostConstruct
    void connect() throws IOException {
        final Config config = ConfigProvider.getConfig();
        String baseDir = config.getValue(PROP_PREFIX + PROP_BASE_DIR, String.class);
        Path baseDirPath = Paths.get(baseDir);
        FileUtils.forceMkdir(baseDirPath.toFile());

        final String streamPattern = config.getValue(PROP_PREFIX + PROP_STREAM_PATTERN, String.class);
        final Path finalPath = baseDirPath.resolve(streamPattern + ".json");

        this.writer = new FileWriter(finalPath.toFile(), true);
    }

    @Override
    public void handleBatch(List<ChangeEvent<Object, Object>> records, DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {
        for (ChangeEvent<Object, Object> record : records) {
            LOGGER.trace("Received event '{}'", record);

            if (record.value() != null) {
                String value = (String) record.value();
                try {
                    this.writer.write(value);
                    this.writer.write(System.lineSeparator());
                }
                catch (IOException ioe) {
                    throw new InterruptedException(ioe.toString());
                }
            }
        }
        try {
            this.writer.flush();
        }
        catch (IOException ioe) {
            throw new InterruptedException(ioe.toString());
        }

        committer.markBatchFinished();
    }
}
