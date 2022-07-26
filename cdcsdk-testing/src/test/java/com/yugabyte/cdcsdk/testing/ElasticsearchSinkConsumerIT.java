package com.yugabyte.cdcsdk.testing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;

public class ElasticsearchSinkConsumerIT {
  private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchSinkConsumerIT.class);

  private static KafkaContainer kafkaContainer;
}