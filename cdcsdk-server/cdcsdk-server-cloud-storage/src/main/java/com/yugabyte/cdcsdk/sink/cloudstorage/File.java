package com.yugabyte.cdcsdk.sink.cloudstorage;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;

import javax.enterprise.context.Dependent;
import javax.inject.Named;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Named("file")
@Dependent
public class File extends FlushingChangeConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(File.class);

    private long lineSeparatorLength = 0;
    private Writer writer = null;

    protected void createWriter(String base, String path) throws IOException {
        Path baseDirPath = Paths.get(baseDir);
        FileUtils.forceMkdir(baseDirPath.toFile());

        final Path finalPath = baseDirPath.resolve(path + ".json");

        this.writer = new FileWriter(finalPath.toFile(), true);
        this.lineSeparatorLength = System.lineSeparator().getBytes(Charset.defaultCharset()).length;
    }

    protected void closeWriter() throws IOException {
        this.writer.close();
    }

    public long write(String value) throws IOException {
        this.writer.write(value);
        this.writer.write(System.lineSeparator());

        return value.getBytes(Charset.defaultCharset()).length + lineSeparatorLength;
    }

    public void flush() throws IOException {
        this.writer.flush();
    }
}
