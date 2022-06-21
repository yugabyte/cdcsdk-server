package com.yugabyte.cdcsdk.server;

import javax.enterprise.context.Dependent;
import javax.inject.Inject;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

@Dependent
public class Metrics {
    @Inject
    MeterRegistry meterRegistry;

    public static final String bytesWritten = "cdcsdk.sink.total.bytesWritten";
    public static final String recordsWritten = "cdcsdk.sink.total.recordsWritten";

    public Counter get(String counterName) {
        return meterRegistry.counter(bytesWritten);
    }

    public void apply(long numRecords, long numSize) {
        meterRegistry.counter(bytesWritten).increment(numSize);
        meterRegistry.counter(recordsWritten).increment(numRecords);
    }

    public MeterRegistry registry() {
        return meterRegistry;
    }
}
