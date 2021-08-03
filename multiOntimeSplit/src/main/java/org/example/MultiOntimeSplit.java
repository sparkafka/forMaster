package org.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Random;

public class MultiOntimeSplit {
    private static String FIRST_ONTIME_TM = "0";

    private static String bootServers = "node1:9092,node2:9092,node3:9092,node4:9092";
    private static String dataSource = "dataSource";
    private static String onTopic1 = "ontimeTopic1";
    private static String onTopic2 = "ontimeTopic2";
    private static String pTopic = "ptimeTopic";
    private static String tmTopic = "tmTopic";

    public static void main(String[] args) {
//        Random rand = new Random();
        Properties consumeConf = new Properties();
        consumeConf.setProperty("bootstrap.servers", bootServers);
        consumeConf.setProperty("group.id", "MultiOntimeSplitter");
        consumeConf.setProperty("auto.offset.reset", "latest");
        // conf.setProperty("enable.auto.commit", "false");
        consumeConf.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumeConf.setProperty("value.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");

        Consumer<String, Integer> consumer = new KafkaConsumer<>(consumeConf);
//        consumer.assign(Collections.singletonList(new TopicPartition(dataSource, 0)));
//        consumer.assign(Collections.singletonList(new TopicPartition(dataSource, 1)));
//        consumer.assign(Collections.singletonList(new TopicPartition(dataSource, 2)));
        consumer.assign(Collections.singletonList(new TopicPartition(dataSource, 3)));

        Properties produceConf = new Properties();
        produceConf.setProperty("bootstrap.servers", bootServers);
        produceConf.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        produceConf.setProperty("value.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");

        Producer<String, Integer> producer = new KafkaProducer<>(produceConf);

        boolean onTopicSelect = true; // on-time Topic 선택
        int currentWindow = 1;
        String key;
        int value;


        for (int i = 0; i < 300; i++) {
            int recordWindow;
            ConsumerRecords<String, Integer> records = consumer.poll(Duration.ofMillis(10));
            for (ConsumerRecord<String, Integer> record : records) {
                key = record.key();
                value = record.value();
                String msgString = String.format("key:%s, value:%d", key, value);
                System.out.println(msgString);

                recordWindow = Integer.valueOf(key);
                ProducerRecord<String, Integer> producerRecord;
                // 레코드 윈도우가 현재 윈도우보다 작으면 p-time
                if (recordWindow < currentWindow) {
                    producerRecord = new ProducerRecord<>(pTopic, key, value);
                } else {
                    // 레코드 윈도우가 현재 윈도우보다 크면 triggering
                    if (recordWindow > currentWindow) {
                        // value: node number
//                        producerRecord = new ProducerRecord<>(tmTopic, FIRST_ONTIME_TM, 0);
//                        producerRecord = new ProducerRecord<>(tmTopic, FIRST_ONTIME_TM, 1);
//                        producerRecord = new ProducerRecord<>(tmTopic, FIRST_ONTIME_TM, 2);
                        producerRecord = new ProducerRecord<>(tmTopic, FIRST_ONTIME_TM, 3);

                        producer.send(producerRecord);
                        currentWindow = recordWindow; // 현재 윈도우 교체
                        System.out.println("current window change: " + currentWindow);

                        onTopicSelect = !onTopicSelect;
                    }
                    // 레코드 윈도우가 현재 윈도우보다 크거나 같으면 on-time
                    if (onTopicSelect) {
//                        producerRecord = new ProducerRecord<>(onTopic1, 0, key, value);
//                        producerRecord = new ProducerRecord<>(onTopic1, 1, key, value);
//                        producerRecord = new ProducerRecord<>(onTopic1, 2, key, value);
                        producerRecord = new ProducerRecord<>(onTopic1, 3, key, value);
//                        producerRecord = new ProducerRecord<>(onTopic1, rand.nextInt(4), key, value);
                    } else {
//                        producerRecord = new ProducerRecord<>(onTopic2, 0, key, value);
//                        producerRecord = new ProducerRecord<>(onTopic2, 1, key, value);
//                        producerRecord = new ProducerRecord<>(onTopic2, 2, key, value);
                        producerRecord = new ProducerRecord<>(onTopic2, 3, key, value);
//                        producerRecord = new ProducerRecord<>(onTopic2, rand.nextInt(4), key, value);
                    }
                    // TODO
                    // p-time split에 대한 코드

                }
                producer.send(producerRecord, (recordMetadata, e) -> {
                    if (recordMetadata != null) {
                        // 송신에 성공한 경우
                        String infoString = String.format("Success topic:%s partition:%d, offset:%d",
                                recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
                        System.out.println(infoString);
                    } else {
                        // 송신에 실패한 경우
                        String infoString = String.format("Failed:%s", e.getMessage());
                        System.err.println(infoString);
                    }
                });
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        consumer.close();
        producer.close();
    }
}
