package com.yugabyte.cdcsdk.sink.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class RollerTest {

    Roller roller;

    @BeforeEach
    void setupRoller() {
        roller = new Roller(200, 10000L, 10000L);
    }

    @Test
    void testUpdateStatistics() {
        roller.updateStatistics(100, 100L * 1024L, 100);

        assertEquals(100L * 1024L, roller.getCurrentBytesWritten());
        assertEquals(100, roller.getCurrentRecordsWritten());
        assertEquals(100, roller.getCurrentTimeElapsedMs());

        assertFalse(roller.doRoll());
    }

    @Test
    void testUpdateStatisticsTwice() {
        roller.updateStatistics(100, 100L * 1024L, 100);
        roller.updateStatistics(100, 100L * 1024L, 100);

        assertEquals(2 * 100L * 1024L, roller.getCurrentBytesWritten());
        assertEquals(2 * 100, roller.getCurrentRecordsWritten());
        assertEquals(2 * 100, roller.getCurrentTimeElapsedMs());

        assertFalse(roller.doRoll());
    }

    @Test
    void testSizeDoRoll() {
        roller.updateStatistics(100, 201L * 1024L * 1024L * 1024L, 100);
        assertEquals(201L * 1024L * 1024L * 1024L, roller.getCurrentBytesWritten());

        assertTrue(roller.doRoll());
    }

    @Test
    void testRecordsDoRoll() {
        roller.updateStatistics(20000L, 201L * 1024L, 100);
        assertEquals(20000L, roller.getCurrentRecordsWritten());

        assertTrue(roller.doRoll());
    }

    @Test
    void testReset() {
        roller.updateStatistics(100, 100L * 1024L, 100);
        roller.reset();
        assertEquals(0, roller.getCurrentBytesWritten());
        assertEquals(0, roller.getCurrentRecordsWritten());
        assertEquals(0, roller.getCurrentTimeElapsedMs());

        assertFalse(roller.doRoll());
    }

}
