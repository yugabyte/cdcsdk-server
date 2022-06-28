package com.yugabyte.cdcsdk.sink.s3;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Roller {
    private static final Logger LOGGER = LoggerFactory.getLogger(Roller.class);

    protected long totalBytesWritten = 0;
    protected long totalRecordsWritten = 0;
    protected long totalTimeElapsedMs = 0;

    protected long currentBytesWritten = 0;
    protected long currentRecordsWritten = 0;
    protected long currentTimeElapsedMs = 0;

    protected final long flushWhenBytesWritten;
    protected final long flushWhenRecordsWritten;
    protected final long flushWhenTimeElapsedMs;

    public Roller(long flushWhenBytesWritten, long flushWhenRecordsWritten, long flushWhenTimeElapsedMs) {
        this.flushWhenBytesWritten = flushWhenBytesWritten * 1024L * 1024L * 1024L; // Convert to MB
        this.flushWhenRecordsWritten = flushWhenRecordsWritten;
        this.flushWhenTimeElapsedMs = flushWhenTimeElapsedMs;
    }

    public void updateStatistics(long numRecords, long numSize, long currentTimeMs) {
        this.currentBytesWritten += numSize;
        this.currentRecordsWritten += numRecords;
        this.currentTimeElapsedMs += currentTimeMs;

        this.totalBytesWritten += numSize;
        this.totalRecordsWritten += numRecords;
        this.totalTimeElapsedMs += currentTimeMs;
        LOGGER.trace("Current Batch - BytesWritten: {}, RecordsWritten: {}", this.currentBytesWritten,
                this.currentRecordsWritten);
        LOGGER.trace("Total - BytesWritten: {}, RecordsWritten: {}", this.totalBytesWritten, this.totalRecordsWritten);
    }

    public void reset() {
        this.currentBytesWritten = 0;
        this.currentRecordsWritten = 0;
        this.currentTimeElapsedMs = 0;
    }

    public boolean doRoll() {
        return this.currentBytesWritten >= this.flushWhenBytesWritten ||
                this.currentRecordsWritten >= this.flushWhenRecordsWritten;
    }

    public long getTotalBytesWritten() {
        return this.totalBytesWritten;
    }

    public long getTotalRecordsWritten() {
        return this.totalRecordsWritten;
    }

    public long getTotalTimeElapsedMs() {
        return this.totalTimeElapsedMs;
    }

    public long getCurrentBytesWritten() {
        return this.currentBytesWritten;
    }

    public long getCurrentRecordsWritten() {
        return this.currentRecordsWritten;
    }

    public long getCurrentTimeElapsedMs() {
        return this.currentTimeElapsedMs;
    }

    public long getFlushWhenBytesWritten() {
        return this.flushWhenBytesWritten;
    }

    public long getFlushWhenRecordsWritten() {
        return this.flushWhenRecordsWritten;
    }

    public long getFlushWhenTimeElapsedMs() {
        return this.flushWhenTimeElapsedMs;
    }
}
