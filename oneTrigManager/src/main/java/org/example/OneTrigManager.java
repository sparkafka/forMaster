package org.example;


import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;

// 4개 노드에서 들어왔을때
// 다음 윈도우 데이터 처음 들어왔을때
// 4개의 다음 윈도우 데이터 들어왔을때
// 일단 하나로만
public class OneTrigManager {
    private static int TRIGGER_COUNT = 1;

    private static String trigTopic = "one-trig";
    private static String prodTopic = "one-process";
    private static String pSplitTopic = "one-psplit";
    private static String bootServers = "node1:9092,node2:9092,node3:9092,node4:9092";

    public static void main(String[] args) {
        Properties consumeConf = new Properties();
        consumeConf.setProperty("bootstrap.servers", bootServers);
        consumeConf.setProperty("group.id", "OneFirstSplitter");
        // conf.setProperty("enable.auto.commit", "false");
        consumeConf.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumeConf.setProperty("value.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        Consumer<String, Integer> tmConsumer = new KafkaConsumer<>(consumeConf);
        tmConsumer.subscribe(Collections.singletonList(trigTopic));

        Properties proConf = new Properties();
        proConf.setProperty("bootstrap.servers", bootServers);
        proConf.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        proConf.setProperty("value.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");

        Producer<String, Integer> tmProducer = new KafkaProducer<>(proConf);

        // on-time, p-time 구분해서 tm 만들자
        // true: on-time, false: p-time
        boolean trigger = true;

        // 메시지는 다 여기서 처리할까?
        // 0: first on-time process, 1: late on-time process,
        // 2: p-time split,
        int msg = 0;
        int countFirstOnTime = 0;
        int countLateOnTime = 0;
        int countPTimeSplit = 0;
        try {
            while (true) {
                ConsumerRecords<String, Integer> tms = tmConsumer.poll(Duration.ofMillis(1));
                for (ConsumerRecord<String, Integer> tm : tms) {
                    String msgString = String.format("key:%s, value:%d, topic:%s, partition:%d, offset:%d",
                            tm.key(), tm.value(), tm.topic(), tm.partition(), tm.offset());
                    System.out.println(msgString);
                    if (tm.key().equals("first on-time tm")) {
                        msg = 0;
                        countFirstOnTime++;
                    } else if (tm.key().equals("later on-time tm")) {
                        msg = 1;
                        countLateOnTime++;
                    } else if (tm.key().equals("p-time split tm")) {
                        msg = 2;
                        countPTimeSplit++;
                    }
                }
                // System.out.println("count: "+count);
                if (msg == 0) {
                    if (countFirstOnTime >= TRIGGER_COUNT) {
                        System.out.println("count: " + countFirstOnTime);
                        ProducerRecord<String, Integer> record = new ProducerRecord<>(prodTopic, "first on-time tm", 1);
                        tmProducer.send(record);
                        countFirstOnTime = 0;
                    }
                } else if (msg == 1) {

                } else if (msg == 2) {
                    if (countPTimeSplit >= TRIGGER_COUNT) {
                        System.out.println("count: " + countPTimeSplit);
                        ProducerRecord<String, Integer> record = new ProducerRecord<>(prodTopic, "first on-time tm", 1);
                        tmProducer.send(record);
                        countPTimeSplit = 0;
                    }
                }
                /*try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }*/
            }
        } finally {
            tmConsumer.close();
            tmProducer.close();
        }
    }
}
