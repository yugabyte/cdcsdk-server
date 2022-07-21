package com.yugabyte.cdcsdk.testing.util;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import com.yugabyte.cdcsdk.testing.TestHelper;

public class CdcsdkContainer {
    private final DockerImageName cdcsdkContainerImageName = DockerImageName.parse("quay.io/yugabyte/cdcsdk-server:latest");
    
    private String cdcsdkSourceConnectorClass = "io.debezium.connector.yugabytedb.YugabyteDBConnector";
    private String cdcsdkSourceDatabaseHostname = "127.0.0.1";
    private String cdcsdkSourceDatabasePort = "5433";
    private String masterPort = "7100";
    private String cdcsdkMasterAddresses = cdcsdkSourceDatabaseHostname + ":" + masterPort;
    private String cdcsdkSourceDatabaseUser = "yugabyte";
    private String cdcsdkSourceDatabaseDbname = "yugabyte";
    private String cdcsdkSourceDatabasePassword = "yugabyte";
    private String cdcsdkSourceDatabaseSnapshotMode = "never";
    private String cdcsdkSourceTableIncludeList = "";
    private String cdcsdkSourceDatabaseStreamid = "";

    private String cdcsdkSourceDatabaseServerName = "dbserver1";

    // Configurations related to CDCSDK Server
    // Use CDCSDK Server Transforms as unwrap
    private String cdcsdkServerTransformsUnwrapDropTombstones = "false";
    private String cdcsdkServerTransformsUnwrapType = "io.debezium.connector.yugabytedb.transforms.YBExtractNewRecordState";

    // Configurations related to Kafka Sink
    private String cdcsdkSinkKafkaBootstrapServers;
    private String cdcsdkSinkKafkaProducerKeySerializer = "org.apache.kafka.common.serialization.StringSerializer";
    private String cdcsdkSinkKafkaProducerValueSerializer = "org.apache.kafka.common.serialization.StringSerializer";
    private String cdcsdkSinkKafkaClientDnsLookup = "use_all_dns_ips";
    private String cdcsdkSinkKafkaAcks = "all";
    private String cdcsdkSinkKafkaSessionTimeoutMs = "45000";

    // Configurations related to S3 Sink
    // Use CDCSDK Server Transforms as FLATTEN
    private String cdcsdkSinkS3BucketName = "cdcsdk-test";
    private String cdcsdkSinkS3Region = "us-west-2";
    private String cdcsdkSinkS3Basedir = "S3ConsumerIT/";
    private String cdcsdkSinkS3Pattern = "stream_12345";
    private String cdcsdkSinkS3FlushRecords = "5";
    private String cdcsdkSinkS3FlushSizemb = "200";
    private String cdcsdkSinkS3AwsAccessKeyId = System.getenv("AWS_ACCESS_KEY_ID");
    private String cdcsdkSinkS3AwsSecretAccessKey = System.getenv("AWS_SECRET_ACCESS_KEY");
    private String cdcsdkSinkS3AwsSessionToken = System.getenv("AWS_SESSION_TOKEN");

    public CdcsdkContainer withDatabaseHostname(String databaseHostname) {
        this.cdcsdkSourceDatabaseHostname = databaseHostname;
        return this;
    }

    public CdcsdkContainer withDatabasePort(String databasePort) {
        this.cdcsdkSourceDatabasePort = databasePort;
        return this;
    }

    public CdcsdkContainer withDatabaseUser(String user) {
        this.cdcsdkSourceDatabaseUser = user;
        return this;
    }

    public CdcsdkContainer withDatabasePassword(String password) {
        this.cdcsdkSourceDatabasePassword = password;
        return this;
    }

    public CdcsdkContainer withDatabaseDbname(String databaseName) {
        this.cdcsdkSourceDatabaseDbname = databaseName;
        return this;
    }

    public CdcsdkContainer withTableIncludeList(String tableIncludeList) {
        this.cdcsdkSourceTableIncludeList = tableIncludeList;
        return this;
    }

    public CdcsdkContainer withStreamId(String dbStreamId) {
        this.cdcsdkSourceDatabaseStreamid = dbStreamId;
        return this;
    }

    public CdcsdkContainer withDatabaseServerName(String databaseServerName) {
        this.cdcsdkSourceDatabaseServerName = databaseServerName;
        return this;
    }

    public CdcsdkContainer withKafkaBootstrapServers(String bootstrapServers) {
        this.cdcsdkSinkKafkaBootstrapServers = bootstrapServers;
        return this;
    }

    private Map<String, String> getDatabaseConfigMap() {
        Map<String, String> configs = new HashMap<>();

        configs.put("CDCSDK_SOURCE_CONNECTOR_CLASS", this.cdcsdkSourceConnectorClass);
        configs.put("CDCSDK_SOURCE_DATABASE_HOSTNAME", this.cdcsdkSourceDatabaseHostname);
        configs.put("CDCSDK_SOURCE_DATABASE_PORT", this.cdcsdkSourceDatabasePort);
        configs.put("CDCSDK_SOURCE_DATABASE_MASTER_ADDRESSES", this.cdcsdkSourceDatabaseHostname + ":" + this.cdcsdkSourceDatabasePort);
        configs.put("CDCSDK_SOURCE_DATABASE_SERVER_NAME", this.cdcsdkSourceDatabaseServerName);
        configs.put("CDCSDK_SOURCE_DATABASE_DBNAME", this.cdcsdkSourceDatabaseDbname);
        configs.put("CDCSDK_SOURCE_DATABASE_USER", this.cdcsdkSourceDatabaseUser);
        configs.put("CDCSDK_SOURCE_DATABASE_PASSWORD", this.cdcsdkSourceDatabasePassword);
        configs.put("CDCSDK_SOURCE_TABLE_INCLUDE_LIST", this.cdcsdkSourceTableIncludeList);
        configs.put("CDCSDK_SOURCE_SNAPSHOT_MODE", this.cdcsdkSourceDatabaseSnapshotMode);
        configs.put("CDCSDK_SOURCE_DATABASE_STREAMID", TestHelper.getNewDbStreamId(this.cdcsdkSourceDatabaseDbname));

        return configs;
    }
    
    public Map<String, String> getConfigMapForKafka() {
        Map<String, String> configs = getDatabaseConfigMap();

        configs.put("CDCSDK_SINK_TYPE", "kafka");
        configs.put("CDCSDK_SINK_KAFKA_PRODUCER_BOOTSTRAP_SERVERS", this.cdcsdkSinkKafkaBootstrapServers);
        configs.put("CDCSDK_SINK_KAFKA_PRODUCER_KEY_SERIALIZER", this.cdcsdkSinkKafkaProducerKeySerializer);
        configs.put("CDCSDK_SINK_KAFKA_PRODUCER_VALUE_SERIALIZER", this.cdcsdkSinkKafkaProducerValueSerializer);
        configs.put("CDCSDK_SINK_KAFKA_CLIENT_DNS_LOOKUP", this.cdcsdkSinkKafkaClientDnsLookup);
        configs.put("CDCSDK_SINK_KAFKA_ACKS", this.cdcsdkSinkKafkaAcks);
        configs.put("CDCSDK_SINK_KAFKA_SESSION_TIMEOUT_MS", this.cdcsdkSinkKafkaSessionTimeoutMs);

        configs.put("CDCSDK_SERVER_TRANSFORMS", "unwrap");
        configs.put("CDCSDK_SERVER_TRANSFORMS_UNWRAP_DROP_TOMBSTONES", this.cdcsdkServerTransformsUnwrapDropTombstones);
        configs.put("CDCSDK_SERVER_TRANSFORMS_UNWRAP_TYPE", this.cdcsdkServerTransformsUnwrapType);

        return configs;
    }

    public Map<String, String> getConfigMapForS3() {
        Map<String, String> configs = getDatabaseConfigMap();

        configs.put("CDCSDK_SINK_TYPE", "s3");

        configs.put("CDCSDK_SINK_S3_BUCKET_NAME", this.cdcsdkSinkS3BucketName);
        configs.put("CDCSDK_SINK_S3_REGION", this.cdcsdkSinkS3Region);
        configs.put("CDCSDK_SINK_S3_BASEDIR", this.cdcsdkSinkS3Basedir);
        configs.put("CDCSDK_SINK_S3_PATTERN", this.cdcsdkSinkS3Pattern);
        configs.put("CDCSDK_SINK_S3_FLUSH_RECORDS", this.cdcsdkSinkS3FlushRecords);
        configs.put("CDCSDK_SINK_S3_FLUSH_SIZEMB", this.cdcsdkSinkS3FlushSizemb);

        configs.put("CDCSDK_SERVER_TRANSFORMS", "FLATTEN");
        configs.put("CDCSDK_SINK_S3_AWS_ACCESS_KEY_ID", this.cdcsdkSinkS3AwsAccessKeyId);
        configs.put("CDCSDK_SINK_S3_AWS_SECRET_ACCESS_KEY", this.cdcsdkSinkS3AwsSecretAccessKey);
        configs.put("CDCSDK_SINK_S3_AWS_SESSION_TOKEN", this.cdcsdkSinkS3AwsSessionToken);

        return configs;
    }

    public GenericContainer<?> buildForKafkaSink() {
        GenericContainer<?> cdcsdkContainer = new GenericContainer<>(cdcsdkContainerImageName);
        cdcsdkContainer.withEnv(getConfigMapForKafka());
        cdcsdkContainer.withExposedPorts(8080);
        cdcsdkContainer.waitingFor(Wait.forLogMessage(".*BEGIN RECORD PROCESSING.*\\n", 1));
        cdcsdkContainer.withStartupTimeout(Duration.ofSeconds(120));

        return cdcsdkContainer;
    }

    public GenericContainer<?> buildForS3Sink() {
        GenericContainer<?> cdcsdkContainer = new GenericContainer<>(cdcsdkContainerImageName);
        cdcsdkContainer.withEnv(getConfigMapForKafka());
        cdcsdkContainer.withExposedPorts(8080);
        cdcsdkContainer.waitingFor(Wait.forLogMessage(".*BEGIN RECORD PROCESSING.*\\n", 1));
        cdcsdkContainer.withStartupTimeout(Duration.ofSeconds(120));

        return cdcsdkContainer;
    }
}