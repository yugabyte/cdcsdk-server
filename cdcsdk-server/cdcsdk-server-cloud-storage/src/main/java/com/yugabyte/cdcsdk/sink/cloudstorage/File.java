package com.yugabyte.cdcsdk.sink.cloudstorage;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import javax.enterprise.context.Dependent;
import javax.inject.Named;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Named("file")
@Dependent
public class File extends FlushingChangeConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(File.class);

    private Writer writer = null;

    protected void createWriter(String base, String path, Map<String, String> props) throws IOException {
        Path baseDirPath = Paths.get(baseDir);
        FileUtils.forceMkdir(baseDirPath.toFile());

        final Path finalPath = baseDirPath.resolve(path + ".json");
        this.writer = new FileWriter(finalPath.toFile(), true);
        LOGGER.info("Created file at {}", finalPath.toString());
    }

    protected void closeWriter() throws IOException {
        this.writer.close();
    }

    public void write(String value) throws IOException {
        this.writer.write(value);
    }

    public void flush() throws IOException {
        this.writer.flush();
    }
}
