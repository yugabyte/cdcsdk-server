package com.yugabyte.cdcsdk.testing;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.Double;
import java.lang.Integer;
import java.lang.String;
import java.net.InetAddress;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.*;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class PostgresSinkConsumerIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresSinkConsumerIT.class);
    private static String HOST_ADDRESS = "127.0.0.1";
    private static String BOOTSTRAP_SERVER = "127.0.0.1:9092";
    private KafkaConsumer<String, JsonNode> consumer;
    private static List<Map<String, Object>> expected_data = new ArrayList<>();
    private static int recordsInserted = 5;

    @BeforeAll
    public static void beforeClass() throws Exception {
        HOST_ADDRESS = InetAddress.getLocalHost().getHostAddress();
        TestHelper.setHost(HOST_ADDRESS);
        BOOTSTRAP_SERVER = HOST_ADDRESS + ":9092";
        // String createTableSql = "CREATE TABLE IF NOT EXISTS test_table (id int primary key, first_name varchar(30), last_name varchar(50), days_worked double precision)";
        // TestHelper.execute(createTableSql);
        String deleteFromTableSql = "DELETE FROM test_table";
        TestHelper.execute(deleteFromTableSql);

        // Initialise expected_data.

        for (int i = 0; i < recordsInserted; i++) {
            Map<String, Object> expected_record = new LinkedHashMap<String, Object>();
            expected_record.put("id", new Integer(i));
            expected_record.put("first_name", new String("first_" + i));
            expected_record.put("last_name", new String("last_" + i));
            expected_record.put("days_worked", new Double(23.45));
            expected_data.add(expected_record);
        }

        // Insert records in YB.

        for (int i = 0; i < recordsInserted; ++i) {
            String insertSql = String.format("INSERT INTO test_table VALUES (%d, '%s', '%s', %f);", i, "first_" + i,
                    "last_" + i, 23.45);
            TestHelper.execute(insertSql);
        }
    }

    @Test
    public void testAutomationOfKafkaAssertions() throws Exception {

        // Verify data on Kafka.

        setKafkaConsumerProperties();
        consumer.subscribe(Arrays.asList("dbserver1.public.test_table"));
        long expectedtime = System.currentTimeMillis() + 10000;
        int flag = 0;
        Thread.sleep(5000);
        while (System.currentTimeMillis() < expectedtime) {
            consumer.seekToBeginning(consumer.assignment());
            ConsumerRecords<String, JsonNode> records = consumer.poll(15);
            // assertEquals(records.count(), recordsInserted);
            List<Map<String, Object>> kafkaRecords = new ArrayList<>();
            for (ConsumerRecord<String, JsonNode> record : records) {
                ObjectMapper mapper = new ObjectMapper();
                if (record.value() != null) {
                    JsonNode jsonNode = record.value().get("payload");
                    Map<String, Object> result = mapper.convertValue(jsonNode, new TypeReference<Map<String, Object>>() {
                    });
                    kafkaRecords.add(result);
                }
            }
            Iterator<Map<String, Object>> it = expected_data.iterator();
            int recordsAsserted = 0;

            for (Map<String, Object> kafkaRecord : kafkaRecords) {
                assertEquals(it.next(), kafkaRecord);
                ++recordsAsserted;
                if (recordsAsserted == recordsInserted) {
                    flag = 1;
                    break;
                }
            }
            if (flag == 1) {
                break;
            }
        }
    }

    @Test
    public void testAutomationOfPostgresAssertions() throws Exception {

        // Verify data on Postgres.

        Class.forName("org.postgresql.Driver");
        Connection conn = DriverManager.getConnection("jdbc:postgresql://" + HOST_ADDRESS + ":5432/postgres", "postgres",
                "postgres");
        Statement stmt = conn.createStatement();
        Thread.sleep(10000);
        ResultSet rs = stmt.executeQuery("select * from sink");
        List<Map<String, Object>> postgresRecords = new ArrayList<>();
        while (rs.next()) {
            Map<String, Object> result = new LinkedHashMap<String, Object>();
            result.put("id", rs.getInt("id"));
            result.put("first_name", rs.getString("first_name"));
            result.put("last_name", rs.getString("last_name"));
            result.put("days_worked", rs.getDouble("days_worked"));
            postgresRecords.add(result);
        }

        Iterator<Map<String, Object>> it = expected_data.iterator();

        int recordsAsserted = 0;
        for (Map<String, Object> postgresRecord : postgresRecords) {
            assertEquals(it.next(), postgresRecord);
            ++recordsAsserted;
            if (recordsAsserted == recordsInserted) {
                break;
            }
        }

    }

    private void setKafkaConsumerProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVER);
        props.put("group.id", "myapp");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.connect.json.JsonDeserializer");
        consumer = new KafkaConsumer<>(props);
    }
}
