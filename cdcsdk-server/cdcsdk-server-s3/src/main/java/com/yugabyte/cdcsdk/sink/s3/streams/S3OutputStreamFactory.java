package com.yugabyte.cdcsdk.sink.s3.streams;

import java.io.OutputStream;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yugabyte.cdcsdk.sink.s3.NamePatternResolver;
import com.yugabyte.cdcsdk.sink.s3.S3Storage;

public class S3OutputStreamFactory implements OutputStreamFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3OutputStreamFactory.class);

    final S3Storage storage;
    final String baseDir;
    final String pattern;

    public S3OutputStreamFactory(S3Storage storage, String baseDir, String pattern) {
        this.storage = storage;
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

        final S3OutputStream s3out = storage.create(commitFilename, true);
        final OutputStream s3outWrapper = s3out.wrapForCompression();

        return s3outWrapper;
    }
}
