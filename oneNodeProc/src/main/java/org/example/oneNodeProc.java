package org.example;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;

import java.lang.reflect.Array;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;

public class oneNodeProc {
    private static String trigTopic = "one-process";
    private static String onTopic1 = "one-on1";
    private static String onTopic2 = "one-on2";
    private static String managerTopic = "one-trig";
    private static String resultTopic = "one-on-result";
    private static String bootServers = "node1:9092,node2:9092,node3:9092,node4:9092";

    public static void main(String[] args) {
        Properties consumeConf = new Properties();
        consumeConf.setProperty("bootstrap.servers", bootServers);
        consumeConf.setProperty("group.id", "OneProcess");
        //consumeConf.setProperty("enable.auto.commit", "false");
        consumeConf.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumeConf.setProperty("value.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");

        Consumer<String, Integer> consumer = new KafkaConsumer<>(consumeConf);

        Properties proConf = new Properties();
        proConf.setProperty("bootstrap.servers", bootServers);
        proConf.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        proConf.setProperty("value.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        Producer<String, Integer> producer = new KafkaProducer<>(proConf);

        // 구독하는데 키-값 확인해서 노드 구분
        // tm topic 구독하다가 tm 들어오면 on time topic 구독
        boolean trigSwitch = false;
        boolean onSelector = true;
        boolean secondSwitch = false;
        boolean pTrigger = false; // p-time split trigger
        //boolean procCom = false; // process complete
        try {
            while (true) {
                consumer.subscribe(Collections.singletonList(trigTopic));
                ConsumerRecords<String, Integer> tms = consumer.poll(Duration.ofMillis(10));
                for (ConsumerRecord<String, Integer> tm : tms) {
                    String msgString = String.format("key:%s, value:%d, topic:%s, partition:%d, offset:%d",
                            tm.key(), tm.value(), tm.topic(), tm.partition(), tm.offset());
                    System.out.println(msgString);
                    if (tm.key().equals("first on-time tm")) {
                        HashMap<String, Integer> resultMap = new HashMap<>();
                        if (onSelector) {
                            consumer.subscribe(Collections.singletonList(onTopic1));
                        } else {
                            consumer.subscribe(Collections.singletonList(onTopic2));
                        }
                        ConsumerRecords<String, Integer> records = consumer.poll(Duration.ofMillis(10));
                        while (records.isEmpty()){
                            records = consumer.poll(Duration.ofMillis(10));
                        }
                        for (ConsumerRecord<String, Integer> record : records) {
                            String recordString = String.format("key:%s, value:%d, topic:%s, partition:%d, offset:%d",
                                    record.key(), record.value(), record.topic(), record.partition(), record.offset());
                            System.out.println(recordString);
                            if (!resultMap.containsKey(record.key())) {
                                resultMap.put(record.key(), 1);
                            } else {
                                resultMap.put(record.key(), resultMap.get(record.key()) + 1);
                            }
                        }

                        resultMap.forEach((window, count) -> {
                            System.out.println(window + ": " + count);
//                                ProducerRecord<String, Integer> results = new ProducerRecord<>(resultTopic, window, count);
//                                producer.send(results);
                        });

                        onSelector = !onSelector;
                    }
                }
                /*if (!trigSwitch) {
                    consumer.subscribe(Collections.singletonList(trigTopic));
                    ConsumerRecords<String, Integer> tms = consumer.poll(Duration.ofMillis(1));
                    for (ConsumerRecord<String, Integer> tm : tms) {
                        String msgString = String.format("key:%s, value:%d, topic:%s, partition:%d, offset:%d",
                                tm.key(), tm.value(), tm.topic(), tm.partition(), tm.offset());
                        System.out.println(msgString);
                        if (tm.key().equals("first on-time tm")) {
                            trigSwitch = true;
                            secondSwitch = false;
                            pTrigger = true;
                        }
                        *//*if (record.key() == "first on-time") {
                            pTrigger = true;
                            procCom = false;
                        } else if (record.key() == "late on-time") {
                            pTrigger = false;
                            procCom = true;
                        }*//*
                    }
                } else if (trigSwitch) {
                    consumer.subscribe(Collections.singletonList(onTopic));
                    // 키에 해당하는 데이터의 수 count
                    HashMap<String, Integer> resultMap = new HashMap<>();

                    ConsumerRecords<String, Integer> records = consumer.poll(Duration.ofMillis(1));

//                    if (records.isEmpty()){
//                        continue;
//                    }
                    if (!records.isEmpty()) {
                        for (ConsumerRecord<String, Integer> record : records) {
                            String msgString = String.format("key:%s, value:%d, topic:%s, partition:%d, offset:%d",
                                    record.key(), record.value(), record.topic(), record.partition(), record.offset());
                            System.out.println(msgString);
                            if (!resultMap.containsKey(record.key())) {
                                resultMap.put(record.key(), 1);
                            } else {
                                resultMap.put(record.key(), resultMap.get(record.key()) + 1);
                            }
                            if (trigSwitch = true) {
                                trigSwitch = false;
                            }
                        }

                        resultMap.forEach((window, count) -> {
                            System.out.println(window + ": " + count);
                            ProducerRecord<String, Integer> results = new ProducerRecord<>(resultTopic, window, count);
                            producer.send(results);
                        });
                    }*/


//                    if (pTrigger){
//                        ProducerRecord<String, Integer> ptm = new ProducerRecord<>(managerTopic, "p-time split tm", 1);
//                        producer.send(ptm);
//                        pTrigger = false;
//                    }
                    /* else if (procCom){
                        ProducerRecord<String, Integer> currentWin = new ProducerRecord<>(currentWinTopic,
                                "new window", Integer.valueOf(window) + 1);
                        producer.send(currentWin);
                    }*/
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } finally {
            consumer.close();
            producer.close();
        }
    }
}
