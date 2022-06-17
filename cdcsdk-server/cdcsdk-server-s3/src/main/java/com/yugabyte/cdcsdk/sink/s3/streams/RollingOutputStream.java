package com.yugabyte.cdcsdk.sink.s3.streams;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yugabyte.cdcsdk.sink.s3.Roller;
import com.yugabyte.cdcsdk.sink.s3.buffer.BufferStorage;

public class RollingOutputStream {
    private static final Logger LOGGER = LoggerFactory.getLogger(RollingOutputStream.class);

    final OutputStreamFactory factory;
    final BufferStorage buffer;
    final Roller roller;

    public RollingOutputStream(OutputStreamFactory factory, BufferStorage buffer, Roller roller) {
        this.factory = factory;
        this.buffer = buffer;
        this.roller = roller;
    }

    public void write(byte[] value) throws IOException {
        this.buffer.getOutputStream().write(value);
        if (roller.doRoll()) {
            this.flush();
            roller.reset();
        }
    }

    public void write(byte[] bytes, int offset, int length) throws IOException {
        this.buffer.getOutputStream().write(bytes, offset, length);
        if (roller.doRoll()) {
            this.flush();
            roller.reset();
        }
    }

    public void flush() throws IOException {
        this.buffer.getOutputStream().flush();
        this.buffer.close();
        LOGGER.trace("Flush {}:{}", roller.getCurrentRecordsWritten(), roller.getTotalRecordsWritten());
        if (roller.getCurrentRecordsWritten() > 0) {
            OutputStream finalOut = factory.create(DateTime.now(DateTimeZone.UTC));
            InputStream bufferIn = this.buffer.convertToInputStream();

            byte[] buffer = new byte[1024]; // Adjust if you want
            int bytesRead;
            while ((bytesRead = bufferIn.read(buffer)) != -1) {
                finalOut.write(buffer, 0, bytesRead);
            }
            finalOut.flush();
            finalOut.close();
        }

        this.buffer.reset();
    }

    public void close() throws IOException {
        this.buffer.close();
    }
}
