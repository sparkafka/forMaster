package org.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.consumer.*;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;

public class OneNodeSplit {
    private static String sourceTopic = "one-source";
    private static String onTopic1 = "one-on1";
    private static String onTopic2 = "one-on2";
    private static String pTopic = "one-p";
    private static String trigTopic = "one-trig";
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
        //sourceConsumer.subscribe(Arrays.asList(sourceTopic, currentWinTopic));
        sourceConsumer.subscribe(Collections.singletonList(sourceTopic));

        // produce topic
        Properties proConf = new Properties();
        proConf.setProperty("bootstrap.servers", bootServers);
        proConf.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        proConf.setProperty("value.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");

        Producer<String, Integer> producer = new KafkaProducer<>(proConf);

        int currentWindow = 1; // 현재 윈도우
        // boolean splitter = true;
        boolean onSelector = true;
        String key;
        int value;

        try {
            while (true) {
                int recordWindow;
                ConsumerRecords<String, Integer> sources = sourceConsumer.poll(Duration.ofMillis(1)); // 10ms 동안 기다려서 poll
//            System.out.println("321");
                for (ConsumerRecord<String, Integer> source : sources) {
                    key = source.key();
                    value = source.value();
                    String msgString = String.format("key:%s, value:%d", key, value);
                    System.out.println(msgString);

                    recordWindow = Integer.valueOf(key);
                    ProducerRecord<String, Integer> record;
                    // 레코드 윈도우가 현재 윈도우보다 작으면 p-time
                    if (recordWindow < currentWindow) {
                        record = new ProducerRecord<>(pTopic, key, value);
                    } else {
                        // 레코드 윈도우가 현재 윈도우보다 크면 triggering
                        if (recordWindow > currentWindow) {
                            // value: node number
                            record = new ProducerRecord<>(trigTopic, "first on-time tm", 1);
                            producer.send(record);
                            currentWindow = recordWindow; // 현재 윈도우 교체
                            System.out.println("current window change: "+currentWindow);
                            onSelector = !onSelector;
                        }
                        // 레코드 윈도우가 현재 윈도우보다 크거나 같으면 on-time
                        if (onSelector) {
                            record = new ProducerRecord<>(onTopic1, key, value);
                        } else {
                            record = new ProducerRecord<>(onTopic2, key, value);
                        }
                    }
                    // 데이터 전송
                    producer.send(record);
                }

                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

        } finally {
            sourceConsumer.close();
            producer.close();
        }
    }
}
