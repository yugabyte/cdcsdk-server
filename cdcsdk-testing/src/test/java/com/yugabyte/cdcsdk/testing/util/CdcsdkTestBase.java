package com.yugabyte.cdcsdk.testing.util;

import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;

import io.debezium.testing.testcontainers.DebeziumContainer;

/**
 * Base class for common test related configurations.
 * 
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class CdcsdkTestBase {
  private static Network containerNetwork;

  // Some basic containers required across all tests
  private static KafkaContainer kafka;
  private static DebeziumContainer kafkaConnect;
  private static GenericContainer<?> cdcsdk;

  @BeforeAll
  public static void beforeAll() throws Exception {
    // Initialize the Docker container network
    containerNetwork = Network.newNetwork();

    kafka = new KafkaContainer(TestImages.KAFKA)
        .withNetworkAliases("kafka")
        .withNetwork(containerNetwork);
    
    kafkaConnect = new DebeziumContainer(TestImages.KAFKA_CONNECT)

  }

}
