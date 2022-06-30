package com.yugabyte.cdcsdk.testing;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;
import java.util.List;

import org.apache.kafka.clients.consumer.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgresConsumerIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresConsumerIT.class);

    @BeforeEach
    public void createTable() throws Exception {
        // String createTableSql = "CREATE TABLE IF NOT EXISTS test_table (id INT, name VARCHAR(255))";
        // TestHelper.execute(createTableSql);
    }

    @AfterEach
    public void dropTable() throws Exception {
        // String dropTableSql = "DROP TABLE IF EXISTS test_table";
        // TestHelper.execute(dropTableSql);
    }

    @Test
    public void testAutomation() throws Exception {
        System.out.println("Waiting for sometime...");

        int recordsInserted = 5;
        for (int i = 0; i < recordsInserted; ++i) {
            String insertSql = String.format("INSERT INTO test_table VALUES (%d, '%s', '%s', %f);", i, "first_" + i, "last_" + i, 23.45);
            TestHelper.execute(insertSql);
        }

        // Properties props = new Properties();
        // props.put("bootstrap.servers", "10.150.1.22:9092");
        // props.put("group.id", "myapp");
        // props.put("enable.auto.commit", "true");
        // props.put("auto.commit.interval.ms", "1000");
        // props.put("session.timeout.ms", "30000");
        // props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        // consumer.subscribe(Arrays.asList("dbserver1.public.test_table"));
        // System.out.println("Consumer created");
        // while (true) {
        // consumer.seekToBeginning(consumer.assignment());
        // ConsumerRecords<String, String> records = consumer.poll(100);
        // System.out.println("Record count " + records.count());
        // for (ConsumerRecord<String, String> record : records) {
        // System.out.println("Key-value read" + record.key() + " " + record.value());
        // }
        // }

        Class.forName("org.postgresql.Driver");
        Connection conn = DriverManager.getConnection("jdbc:postgresql://10.150.1.22:5432/postgres", "postgres", "postgres");
        Statement stmt = conn.createStatement();
        System.out.println("Connected to the PostgreSQL server successfully.");
        Thread.sleep(10000);
        ResultSet rs = stmt.executeQuery("select * from sink");
        List<String> allLines = new ArrayList<>();
        while (rs.next()) {
            String line = rs.getInt("id") + "\t"
                    + rs.getString("first_name") + "\t"
                    + rs.getString("last_name") + "\t"
                    + rs.getDouble("days_worked");
            allLines.add(line);
        }

        List<String> expected_data = List.of("0" + "\t" + "first_0" + "\t" + "last_0" + "\t" + "23.45",
                "1" + "\t" + "first_1" + "\t" + "last_1" + "\t" + "23.45",
                "2" + "\t" + "first_2" + "\t" + "last_2" + "\t" + "23.45",
                "3" + "\t" + "first_3" + "\t" + "last_3" + "\t" + "23.45",
                "4" + "\t" + "first_4" + "\t" + "last_4" + "\t" + "23.45");

        Iterator<String> expected = expected_data.iterator();

        int recordsAsserted = 0;
        for (String line : allLines) {
            assertEquals(expected.next(), line);
            ++recordsAsserted;
            if (recordsAsserted == recordsInserted) {
                break;
            }
        }
    }
}
