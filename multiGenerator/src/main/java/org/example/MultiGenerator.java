package org.example;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.Random;

public class MultiGenerator {
    private static String topicName = "dataSource";
    private static String bootServers = "node1:9092,node2:9092,node3:9092,node4:9092";

    public static void main(String[] args) {
        Random rand = new Random();

        Properties conf = new Properties();
        conf.setProperty("bootstrap.servers", bootServers);
        conf.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        conf.setProperty("value.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");

        Producer<String, Integer> producer = new KafkaProducer<>(conf);

        String key;
        int value;
        //String value;
        for (int i = 1; i <= 3; i++) {
            for (int j = 1; j <= 100; j++) {
                key = String.valueOf(i);
                value = i;

                System.out.println("window:"+key+" value:"+value);
                ProducerRecord<String, Integer> record = new ProducerRecord<>(topicName, rand.nextInt(4), key, value);
                producer.send(record);

                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        for(int i=0;i<4;i++) {
            ProducerRecord<String, Integer> record = new ProducerRecord<>(topicName, i, "4", 0);
            producer.send(record);
        }

        producer.close();
    }
}

