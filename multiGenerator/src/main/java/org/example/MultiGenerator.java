package org.example;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.Random;

public class MultiGenerator {
    final private static String topicName = "dataSource";
    final private static String bootServers = "node0:9092,node1:9092,node2:9092,node3:9092";

    final private static String DATA = "D";

    public static void main(String[] args) {
        Random rand = new Random();

        Properties conf = new Properties();
        conf.setProperty("bootstrap.servers", bootServers);
        conf.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        conf.setProperty("value.serializer", "org.example.CustomValueSerializer");

        Producer<String, CustomValue> producer = new KafkaProducer<>(conf);

        String key;
        CustomValue value;
        for (int j = 1; j <= 50; j++) {
            key = DATA;
            value = new CustomValue(1, 1);

            System.out.println("window:" + value.window + " value:" + value.value);
            ProducerRecord<String, CustomValue> record = new ProducerRecord<>(topicName, rand.nextInt(4), key, value);
            producer.send(record);

            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        for (int j = 1; j <= 10; j++) {
            key = DATA;
            value = new CustomValue(2, 2);

            System.out.println("window:" + value.window + " value:" + value.value);

            ProducerRecord<String, CustomValue> record = new ProducerRecord<>(topicName, rand.nextInt(4), key, value);
            producer.send(record);

            key = DATA;
            value = new CustomValue(1, 1);

            System.out.println("window:" + value.window + " value:" + value.value);
            record = new ProducerRecord<>(topicName, rand.nextInt(4), key, value);
            producer.send(record);
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        for (int i = 2; i <= 3; i++) {
            for (int j = 1; j <= 50; j++) {
                key = DATA;
                value = new CustomValue(i, i);

                System.out.println("window:" + value.window + " value:" + value.value);

                ProducerRecord<String, CustomValue> record = new ProducerRecord<>(topicName, rand.nextInt(4), key, value);
                producer.send(record);

                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        for (int i = 0; i < 4; i++) {
            ProducerRecord<String, CustomValue> record = new ProducerRecord<>(topicName, i, DATA, new CustomValue(2, 0));
            System.out.println("window:" + "2" + " value:" + 0);
            producer.send(record);
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        for (int i = 0; i < 4; i++) {
            ProducerRecord<String, CustomValue> record = new ProducerRecord<>(topicName, i, DATA, new CustomValue(4, 0));
            System.out.println("window:" + "4" + " value:" + 0);
            producer.send(record);
        }

        producer.close();
    }
}

