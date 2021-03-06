package org.example;

import java.util.Properties;
import org.apache.kafka.clients.producer.*;

public class OneNodeGener {
    private static String topicName = "one-source";
    private static String bootServers = "node1:9092,node2:9092,node3:9092,node4:9092";
    public static void main(String[] args) {
        Properties conf = new Properties();
        conf.setProperty("bootstrap.servers", bootServers);
        conf.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        conf.setProperty("value.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");

        Producer<String, Integer> producer = new KafkaProducer<>(conf);

        String key;
        int value;
        //String value;
        for (int i = 1;i<=3;i++) {
            for (int j = 1; j <= 100; j++) {
                key = String.valueOf(i);
                value = i;
                System.out.println(key + " " + value);
                ProducerRecord<String, Integer> record = new ProducerRecord<>(topicName, key, value);
                producer.send(record);

                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        ProducerRecord<String, Integer> record = new ProducerRecord<>(topicName, "4", 0);
        producer.send(record);
        producer.close();
    }
}
