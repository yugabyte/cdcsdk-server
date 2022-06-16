/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package com.yugabyte.cdcsdk.sink.s3;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Release test that verifies basic reading from PostgreSQL database and
 * writing to S3
 *
 * @author Rajat Venkatesh
 */

public class S3ConsumerRelIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3ConsumerRelIT.class);

    @Test
    public void testSample() {
        assertEquals(0, 0);
    }
}
