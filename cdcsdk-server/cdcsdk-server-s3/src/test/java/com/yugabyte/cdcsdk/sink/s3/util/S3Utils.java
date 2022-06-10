package com.yugabyte.cdcsdk.sink.cloudstorage.util;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.awaitility.Awaitility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class S3Utils {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3Utils.class);

    /**
     * Wait up to {@code timeoutMs} maximum time limit for the connector to write
     * the specified
     * number of files.
     *
     * @param bucketName S3 bucket name
     * @param numFiles   expected number of files in the bucket
     * @param timeoutSec maximum time in seconds to wait
     * @return the time this method discovered the connector has written the files
     * @throws InterruptedException if this was interrupted
     */
    public static long waitForFilesInDirectory(AmazonS3 s3, String bucketName, String prefix, int numFiles,
                                               long timeoutSec)
            throws InterruptedException {
        Awaitility.await().atMost(Duration.ofSeconds(timeoutSec)).until(() -> {
            return assertFileCountInDirectory(s3, bucketName, prefix, numFiles).orElse(false);
        });
        return System.currentTimeMillis();
    }

    /**
     * Confirm that the file count in a bucket matches the expected number of files.
     *
     * @param bucketName       the name of the bucket containing the files
     * @param expectedNumFiles the number of files expected
     * @return true if the number of files in the bucket match the expected number;
     *         false otherwise
     */
    private static Optional<Boolean> assertFileCountInDirectory(AmazonS3 s3, String bucketName, String prefix,
                                                                int expectedNumFiles) {
        try {
            return Optional.of(getDirectoryFileCount(s3, bucketName, prefix) == expectedNumFiles);
        }
        catch (Exception e) {
            LOGGER.warn("Could not check file count in bucket: {}", bucketName);
            return Optional.empty();
        }
    }

    /**
     * Recursively query the bucket to get the total number of files that exist in
     * the bucket.
     *
     * @param bucketName the name of the bucket containing the files.
     * @return the number of files in the bucket
     */
    private static int getDirectoryFileCount(AmazonS3 s3, String bucketName, String prefix) {
        return getDirectoryFiles(s3, bucketName, prefix).size();
    }

    public static List<String> getDirectoryFiles(AmazonS3 s3, String bucketName, String prefix) {
        List<String> files = new ArrayList<>();

        ListObjectsV2Request request = new ListObjectsV2Request().withBucketName(bucketName).withPrefix(prefix);
        ListObjectsV2Result result;
        do {
            /*
             * Need the result object to extract the continuation token from the request as
             * each request
             * to listObjectsV2() returns a maximum of 1000 files.
             */
            result = s3.listObjectsV2(request);
            for (S3ObjectSummary summary : result.getObjectSummaries()) {
                files.add(summary.getKey());
                LOGGER.debug("Listed file: {}", summary.getKey());
            }
            String token = result.getNextContinuationToken();
            // To get the next batch of files.
            request.setContinuationToken(token);
        } while (result.isTruncated());
        return files;
    }
}
