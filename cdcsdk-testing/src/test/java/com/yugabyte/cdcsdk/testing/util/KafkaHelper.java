package com.yugabyte.cdcsdk.testing.util;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Helper class to facilitate Kafka related operations
 * 
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class KafkaHelper {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaHelper.class);

    private static String bootstrapServers = "127.0.0.1:9092";

    /**
     * Set the bootstrap servers address where the Kafka process is running
     * 
     * @param bootstrapServersHostPort the comma separated values of the bootstrap
     *                                 servers in the form host:port
     */
    public static void setBootstrapServers(String bootstrapServersHostPort) {
        bootstrapServers = bootstrapServersHostPort;
    }

    /**
     * Get a {@link KafkaConsumer} instance with the provided bootstrap servers.
     * 
     * @param bootstrapServers the comma separated values of the bootstrap servers
     *                         in the form host:port
     * @return the {@link KafkaConsumer} instance
     */
    public static KafkaConsumer<String, JsonNode> getKafkaConsumer(String bootstrapServers) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("group.id", "testapp");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.connect.json.JsonDeserializer");
        return new KafkaConsumer<>(props);
    }

    /**
     * Wrapper function around {@link #getKafkaConsumer(String)} using preset value
     * of bootstrap servers
     * 
     * @return the {@link KafkaConsumer} instance
     */
    public static KafkaConsumer<String, JsonNode> getKafkaConsumer() {
        return getKafkaConsumer(bootstrapServers);
    }

    /**
     * Delete the topics provided in the list
     * 
     * @param bootstrapServerHostPorts comma separated values of the bootstrap
     *                                 servers in the form host:port
     * @param topicsToBeDeleted        list of topics to be deleted
     * @throws Exception
     */
    public static void deleteTopicInKafka(String bootstrapServersHostPorts, List<String> topicsToBeDeleted) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServersHostPorts);
        AdminClient adminClient = AdminClient.create(props);

        DeleteTopicsResult res = adminClient.deleteTopics(TopicCollection.ofTopicNames(topicsToBeDeleted));
        if (LOGGER.isDebugEnabled()) {
            Map<String, ?> mp = res.topicNameValues();
            for (Map.Entry<String, ?> element : mp.entrySet()) {
                LOGGER.debug("Deleting Kafka topic: " + element.getKey());
            }
        }
    }

    /**
     * Wrapper function around {@link #deleteTopicInKafka(String, List)} using
     * preset bootstrap servers
     * 
     * @param topicsToBeDeleted the list of topics to be deleted
     */
    public static void deleteTopicInKafka(List<String> topicsToBeDeleted) {
        deleteTopicInKafka(bootstrapServers, topicsToBeDeleted);
    }

    /**
     * Wrapper function around {@link #deleteTopicInKafka(String, List)} using
     * preset bootstrap servers
     * 
     * @param topicName name of the topic to be deleted
     */
    public static void deleteTopicInKafka(String topicName) {
        deleteTopicInKafka(bootstrapServers, Arrays.asList(topicName));
    }

    /**
     * Wait till the records appear in the provided Kafka topic
     * 
     * @param consumer the {@link KafkaConsumer} instance
     * @param topics   list of topics
     * @return {@literal true} if there records in Kafka, {@literal false} otherwise
     * @throws Exception if something goes wrong
     */
    public static boolean waitTillKafkaHasRecords(KafkaConsumer<String, JsonNode> consumer, List<String> topics)
            throws Exception {
        consumer.subscribe(topics);
        consumer.seekToBeginning(consumer.assignment());
        ConsumerRecords<String, JsonNode> records = consumer.poll(Duration.ofSeconds(15));

        return records.count() != 0;
    }
}