package org.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.consumer.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class OneNodeSplit {
    private static String sourceTopic = "one-source";
    private static String onTopic = "one-on";
    private static String pTopic = "one-p";
    private static String currentWinTopic = "one-current";
    private static String bootServers = "node1:9092,node2:9092,node3:9092,node4:9092";

    public static void main(String[] args) {

        // KafkaConsumer에 필요한 설정
        Properties consumeConf = new Properties();
        consumeConf.setProperty("bootstrap.servers", bootServers);
        consumeConf.setProperty("group.id", "OneFirstSplitter");
        // conf.setProperty("enable.auto.commit", "false");
        consumeConf.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumeConf.setProperty("value.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");

        Consumer<String, Integer> sourceConsumer = new KafkaConsumer<>(consumeConf);
        sourceConsumer.subscribe(Arrays.asList(sourceTopic, currentWinTopic));

        // produce topic
        Properties proConf = new Properties();
        proConf.setProperty("bootstrap.servers", bootServers);
        proConf.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        proConf.setProperty("value.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");

        Producer<String, Integer> producer = new KafkaProducer<>(proConf);

        int currentWindow = 1; // 현재 윈도우
        boolean splitter = true;
        String key;
        int value;

        while (true) {
            int recordWindow;
            ConsumerRecords<String, Integer> sources = sourceConsumer.poll(10); // 10ms 동안 기다려서 poll
//            System.out.println("321");
            for (ConsumerRecord<String, Integer> source : sources) {
                key = source.key();
                value = source.value();
                String msgString = String.format("key:%s, value:%d", key, value);
                System.out.println(msgString);
                if (key=="new window"){
                    currentWindow = Integer.valueOf(value);
                    splitter = true;
                } else {
                    recordWindow = Integer.valueOf(key);
                    if (splitter) {
                        if (recordWindow == currentWindow) {
                            ProducerRecord<String, Integer> record = new ProducerRecord<>(onTopic, key, value);
                            producer.send(record, (recordMetadata, e) -> {
                                if (recordMetadata != null) {
                                    String infoString = String.format("Success topic:%s, partition:%d",
                                            recordMetadata.topic(), recordMetadata.partition());
                                    System.out.println(infoString);
                                } else {
                                    String infoString = String.format("Failed:%d", e.getMessage());
                                    System.err.println(infoString);
                                }
                            });
                        } else {
                            if (recordWindow > currentWindow) splitter = false;
                            ProducerRecord<String, Integer> record = new ProducerRecord<>(pTopic, key, value);
                            producer.send(record, (recordMetadata, e) -> {
                                if (recordMetadata != null) {
                                    String infoString = String.format("Success topic:%s, partition:%d",
                                            recordMetadata.topic(), recordMetadata.partition());
                                    System.out.println(infoString);
                                } else {
                                    String infoString = String.format("Failed:%d", e.getMessage());
                                    System.err.println(infoString);
                                }
                            });

                        }
                    } else { // splitter == false
                        ProducerRecord<String, Integer> record = new ProducerRecord<>(pTopic, key, value);
                        producer.send(record, (recordMetadata, e) -> {
                            if (recordMetadata != null) {
                                String infoString = String.format("Success topic:%s, partition:%d",
                                        recordMetadata.topic(), recordMetadata.partition());
                                System.out.println(infoString);
                            } else {
                                String infoString = String.format("Failed:%d", e.getMessage());
                                System.err.println(infoString);
                            }
                        });
                    }
                }
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
