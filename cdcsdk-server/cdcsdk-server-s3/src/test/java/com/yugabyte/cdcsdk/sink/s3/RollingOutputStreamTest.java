package com.yugabyte.cdcsdk.sink.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;

import org.joda.time.DateTime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yugabyte.cdcsdk.sink.s3.buffer.InMemoryBuffer;
import com.yugabyte.cdcsdk.sink.s3.streams.OutputStreamFactory;
import com.yugabyte.cdcsdk.sink.s3.streams.RollingOutputStream;

public class RollingOutputStreamTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(RollingOutputStreamTest.class);

    public class FileOutputStreamFactory implements OutputStreamFactory {

        final String baseDir;
        final String pattern;

        public FileOutputStreamFactory(String baseDir, String pattern) {
            this.baseDir = baseDir;
            this.pattern = pattern;
        }

        @Override
        public OutputStream create(DateTime dateTime) {
            final String path = NamePatternResolver.resolvePath(dateTime, this.pattern);
            String commitFilename = this.baseDir + path;
            LOGGER.info(
                    "Creating new writer filename='{}'",
                    commitFilename);

            try {
                final FileOutputStream out = new FileOutputStream(commitFilename);

                return out;
            }
            catch (IOException ioe) {
                LOGGER.error(ioe.getMessage());
            }
            return null;
        }
    }

    @TempDir
    static Path testDir;

    String[] dbsStrings = {
            "postgresql",
            "mysql",
            "sqlite",
            "yugabytedb"
    };

    @AfterEach
    void cleanTempDir() {
        for (File file : testDir.toFile().listFiles()) {
            if (!file.isDirectory()) {
                file.delete();
            }
        }
    }

    @Test
    void testSingleFile() throws IOException {
        InMemoryBuffer buffer = new InMemoryBuffer("tmp");
        OutputStreamFactory outputStreamFactory = new FileOutputStreamFactory(testDir.toString() + "/",
                "testSingleFile");

        Roller roller = new Roller(200, 10000L, 10000L);
        RollingOutputStream outputStream = new RollingOutputStream(outputStreamFactory, buffer, roller);

        for (String db : dbsStrings) {
            roller.updateStatistics(1, db.getBytes().length, 0);
            outputStream.write(db.getBytes());
        }
        outputStream.flush();
        outputStream.close();

        assertEquals(1, testDir.toFile().listFiles().length);

    }

    @Test
    void testTwoFiles() throws IOException {
        InMemoryBuffer buffer = new InMemoryBuffer("tmp");
        OutputStreamFactory outputStreamFactory = new FileOutputStreamFactory(testDir.toString() + "/",
                "testSingleFile_{EPOCH}");

        Roller roller = new Roller(200, 2, 10000L);
        RollingOutputStream outputStream = new RollingOutputStream(outputStreamFactory, buffer, roller);

        for (String db : dbsStrings) {
            roller.updateStatistics(1, db.getBytes().length, 0);
            outputStream.write(db.getBytes());
        }
        outputStream.flush();
        outputStream.close();

        assertEquals(2, testDir.toFile().listFiles().length);
    }

    @Test
    void test4Files() throws IOException {
        InMemoryBuffer buffer = new InMemoryBuffer("tmp");
        OutputStreamFactory outputStreamFactory = new FileOutputStreamFactory(testDir.toString() + "/",
                "testSingleFile_{EPOCH}");

        Roller roller = new Roller(200, 1, 10000L);
        RollingOutputStream outputStream = new RollingOutputStream(outputStreamFactory, buffer, roller);

        for (String db : dbsStrings) {
            roller.updateStatistics(1, db.getBytes().length, 0);
            outputStream.write(db.getBytes());
        }
        outputStream.flush();
        outputStream.close();

        assertEquals(4, testDir.toFile().listFiles().length);
    }
}
