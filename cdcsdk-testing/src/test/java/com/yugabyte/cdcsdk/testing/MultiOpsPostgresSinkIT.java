package com.yugabyte.cdcsdk.testing;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;

import io.debezium.testing.testcontainers.DebeziumContainer;

public class MultiOpsPostgresSinkIT {
  private static final String createTableSql = "CREATE TABLE IF NOT EXISTS test_table (id int primary key, first_name varchar(30), last_name varchar(50), days_worked double precision)";
  private static final String dropTableSql = "DROP TABLE test_table";

  private static KafkaContainer kafkaContainer;
  private static GenericContainer<?> cdcsdkContainer;
  private static DebeziumContainer kafkaConnectContainer;
  private static PostgreSQLContainer<?> pgContainer;

  
}
