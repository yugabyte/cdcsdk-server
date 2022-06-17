package com.yugabyte.cdcsdk.sink.s3.streams;

import java.io.OutputStream;

import org.joda.time.DateTime;

public interface OutputStreamFactory {
    public OutputStream create(DateTime dateTime);
}
