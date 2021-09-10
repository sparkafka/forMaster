package org.example;

import org.apache.kafka.clients.consumer.*;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class CustomDeserializeTest {
    final private static String bootServers = "node1:9092,node2:9092,node3:9092,node4:9092";
    final private static String topic = "CustomTest";

    public static void main(String[] args) {
        Properties prop2 = new Properties();
        prop2.setProperty("bootstrap.servers", bootServers);
        prop2.setProperty("group.id","CustomTest1");
        prop2.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        prop2.setProperty("value.deserializer", "org.example.CustomValueDeserializer");

        Consumer<String, CustomValue> consumer = new KafkaConsumer<>(prop2);
        consumer.subscribe(Collections.singletonList(topic));
        System.out.println("Start");
        for (int i=1;i<=10000;i++){
            ConsumerRecords<String, CustomValue> records = consumer.poll(Duration.ofMillis(10));
            for (ConsumerRecord<String, CustomValue> record : records) {
                System.out.println("key: " + record.key() + " window: " + record.value().window + " value: " + record.value().value);
                System.out.println(record.timestamp());
            }
            try {
                Thread.sleep(1000);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }
}
