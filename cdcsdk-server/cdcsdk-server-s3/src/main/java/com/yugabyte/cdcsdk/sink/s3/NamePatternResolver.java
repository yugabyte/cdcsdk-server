package com.yugabyte.cdcsdk.sink.s3;

import java.util.UUID;
import java.util.regex.Pattern;

import org.joda.time.DateTime;

public class NamePatternResolver {
    private static final String FORMAT_VARIABLE_YEAR = "{YEAR}";
    private static final String FORMAT_VARIABLE_MONTH = "{MONTH}";
    private static final String FORMAT_VARIABLE_DAY = "{DAY}";
    private static final String FORMAT_VARIABLE_HOUR = "{HOUR}";
    private static final String FORMAT_VARIABLE_MINUTE = "{MINUTE}";
    private static final String FORMAT_VARIABLE_SECOND = "{SECOND}";
    private static final String FORMAT_VARIABLE_MILLISECOND = "{MILLISECOND}";
    private static final String FORMAT_VARIABLE_EPOCH = "{EPOCH}";
    private static final String FORMAT_VARIABLE_UUID = "{UUID}";

    public static String resolvePath(final DateTime writeDatetime, final String customPathFormat) {
        return customPathFormat
                .replaceAll(Pattern.quote(FORMAT_VARIABLE_YEAR), String.format("%s", writeDatetime.year().get()))
                .replaceAll(Pattern.quote(FORMAT_VARIABLE_MONTH),
                        String.format("%02d", writeDatetime.monthOfYear().get()))
                .replaceAll(Pattern.quote(FORMAT_VARIABLE_DAY), String.format("%02d", writeDatetime.dayOfMonth().get()))
                .replaceAll(Pattern.quote(FORMAT_VARIABLE_HOUR), String.format("%02d", writeDatetime.hourOfDay().get()))
                .replaceAll(Pattern.quote(FORMAT_VARIABLE_MINUTE),
                        String.format("%02d", writeDatetime.minuteOfHour().get()))
                .replaceAll(Pattern.quote(FORMAT_VARIABLE_SECOND),
                        String.format("%02d", writeDatetime.secondOfMinute().get()))
                .replaceAll(Pattern.quote(FORMAT_VARIABLE_MILLISECOND),
                        String.format("%04d", writeDatetime.millisOfSecond().get()))
                .replaceAll(Pattern.quote(FORMAT_VARIABLE_EPOCH), String.format("%d", writeDatetime.getMillis()))
                .replaceAll(Pattern.quote(FORMAT_VARIABLE_UUID), String.format("%s", UUID.randomUUID()))
                .replaceAll("/+", "/");
    }
}