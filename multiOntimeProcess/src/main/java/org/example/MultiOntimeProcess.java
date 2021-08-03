package org.example;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;

public class MultiOntimeProcess {
    private static String FIRST_ONTIME_TM = "0";

    private static String bootServers = "node1:9092,node2:9092,node3:9092,node4:9092";
    private static String ontimeTriggerTopic = "ontimeTriggerTopic";
    private static String onTopic1 = "ontimeTopic1";
    private static String onTopic2 = "ontimeTopic2";


    public static void main(String[] args) {
        Properties consumeConf = new Properties();
        consumeConf.setProperty("bootstrap.servers", bootServers);
        consumeConf.setProperty("group.id", "MultiOntimeProcess");
        //consumeConf.setProperty("auto.offset.reset", "latest");
        //consumeConf.setProperty("enable.auto.commit", "false");
        consumeConf.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumeConf.setProperty("value.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");

        Consumer<String, Integer> consumer = new KafkaConsumer<>(consumeConf);

        Properties produceConf = new Properties();
        produceConf.setProperty("bootstrap.servers", bootServers);
        produceConf.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        produceConf.setProperty("value.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");

        Producer<String, Integer> producer = new KafkaProducer<>(produceConf);

        boolean onTopicSelect = true;
        boolean trigSwitch = false;

        for (int i = 0; i < 300; i++) {
            consumer.subscribe(Collections.singletonList(ontimeTriggerTopic));
//            consumer.assign(Collections.singletonList(new TopicPartition(ontimeTriggerTopic, 0)));
//            consumer.assign(Collections.singletonList(new TopicPartition(ontimeTriggerTopic, 1)));
//            consumer.assign(Collections.singletonList(new TopicPartition(ontimeTriggerTopic, 2)));
//            consumer.assign(Collections.singletonList(new TopicPartition(ontimeTriggerTopic, 3)));
            ConsumerRecords<String, Integer> tms = consumer.poll(Duration.ofMillis(10));
            for (ConsumerRecord<String, Integer> tm : tms) {
                String msgString = String.format("key:%s, value:%d, topic:%s, partition:%d, offset:%d",
                        tm.key(), tm.value(), tm.topic(), tm.partition(), tm.offset());
                System.out.println(msgString);
                if (tm.key().equals(FIRST_ONTIME_TM)) {
                    System.out.println("First on-time processing");
                    if (onTopicSelect) {
                        consumer.subscribe(Collections.singletonList(onTopic1));
//                        consumer.assign(Collections.singletonList(new TopicPartition(onTopic1, 0)));
//                        consumer.assign(Collections.singletonList(new TopicPartition(onTopic1, 1)));
//                        consumer.assign(Collections.singletonList(new TopicPartition(onTopic1, 2)));
//                        consumer.assign(Collections.singletonList(new TopicPartition(onTopic1, 3)));
                    } else {
                        consumer.subscribe(Collections.singletonList(onTopic2));
//                        consumer.assign(Collections.singletonList(new TopicPartition(onTopic2, 0)));
//                        consumer.assign(Collections.singletonList(new TopicPartition(onTopic2, 1)));
//                        consumer.assign(Collections.singletonList(new TopicPartition(onTopic2, 2)));
//                        consumer.assign(Collections.singletonList(new TopicPartition(onTopic2, 3)));

                    }
                    HashMap<String, Integer> resultMap = new HashMap<>();
                    ConsumerRecords<String, Integer> records = consumer.poll(Duration.ofMillis(10));
                    while (records.isEmpty()) {
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
//                        ProducerRecord<String, Integer> results = new ProducerRecord<>(resultTopic, window, count);
//                        producer.send(results);
                    });
                    onTopicSelect = !onTopicSelect;
                }
            }

            /*if (!trigSwitch) {
                consumer.subscribe(Collections.singletonList(ontimeTriggerTopic));
//                consumer.assign(Collections.singletonList(new TopicPartition(ontimeTriggerTopic, 0)));
//                consumer.assign(Collections.singletonList(new TopicPartition(ontimeTriggerTopic, 1)));
//                consumer.assign(Collections.singletonList(new TopicPartition(ontimeTriggerTopic, 2)));
//                consumer.assign(Collections.singletonList(new TopicPartition(ontimeTriggerTopic, 3)));

                ConsumerRecords<String, Integer> tms = consumer.poll(Duration.ofMillis(1));
                for (ConsumerRecord<String, Integer> tm : tms) {
                    if (tm.key().equals(FIRST_ONTIME_TM)) {
                        System.out.println("First on-time processing");
                        trigSwitch = true;
//                        secondSwitch = false;
//                        pTrigger = true;
                    }
                }
            } else {
                if (onTopicSelect) {
                    consumer.subscribe(Collections.singletonList(onTopic1));
//                    consumer.assign(Collections.singletonList(new TopicPartition(onTopic1, 0)));
//                    consumer.assign(Collections.singletonList(new TopicPartition(onTopic1, 1)));
//                    consumer.assign(Collections.singletonList(new TopicPartition(onTopic1, 2)));
//                    consumer.assign(Collections.singletonList(new TopicPartition(onTopic1, 3)));
                } else {
                    consumer.subscribe(Collections.singletonList(onTopic2));
//                    consumer.assign(Collections.singletonList(new TopicPartition(onTopic2, 0)));
//                    consumer.assign(Collections.singletonList(new TopicPartition(onTopic2, 1)));
//                    consumer.assign(Collections.singletonList(new TopicPartition(onTopic2, 2)));
//                    consumer.assign(Collections.singletonList(new TopicPartition(onTopic2, 3)));
                }
                HashMap<String, Integer> resultMap = new HashMap<>();

                ConsumerRecords<String, Integer> records = consumer.poll(Duration.ofMillis(1));
                if (!records.isEmpty()) {
                    for (ConsumerRecord<String, Integer> record : records) {
                        if (!resultMap.containsKey(record.key())) {
                            resultMap.put(record.key(), 1);
                        } else {
                            resultMap.put(record.key(), resultMap.get(record.key()) + 1);
                        }
                    }
                    resultMap.forEach((window, count) -> {
                        System.out.println(window + ": " + count);
//                        ProducerRecord<String, Integer> results = new ProducerRecord<>(resultTopic, window, count);
//                        producer.send(results);
                    });
                    trigSwitch = false;
                    onTopicSelect = !onTopicSelect;
                }
            }*/
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}

