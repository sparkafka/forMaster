package org.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;

// Store messages in memory and send the messages to process topic
public class MultiOntimeSplit {
//            final private static int nodeNum = 0;
//    final private static int nodeNum = 1;
//    final private static int nodeNum = 2;
    final private static int nodeNum = 3;

    final private static String FIRST_ONTIME_TM = "O";
    final private static String FIRST_ONTIME_COMPLETE = "C";
    final private static String PTIME_COMPLETE = "P";

    final private static String bootServers = "node1:9092,node2:9092,node3:9092,node4:9092";
    final private static String dataSource = "dataSource";
    final private static String ontimeTriggerTopic = "ontimeTriggerTopic";

    final private static String onTopic1 = "ontimeTopic1";
    final private static String onTopic2 = "ontimeTopic2";
    final private static String pTopic = "ptimeTopic";
    final private static String tmTopic = "tmTopic";
    final private static String onResultTopic = "ontimeResultTopic";

    // Store messages in memory
    static ArrayList<CustomValue> topic1Records = new ArrayList<>();
    static ArrayList<CustomValue> topic2Records = new ArrayList<>();

    public static Consumer<String, Integer> getConsumer() {
        Consumer<String, Integer> consumer;

        Properties consumeConf = new Properties();
        consumeConf.setProperty("bootstrap.servers", bootServers);
        consumeConf.setProperty("group.id", "MultiOntimeSplitter");
        consumeConf.setProperty("max.poll.records", "1");
        consumeConf.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumeConf.setProperty("value.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        consumer = new KafkaConsumer<>(consumeConf);
//        consumer.subscribe(Arrays.asList(dataSource, ontimeTriggerTopic));
        List<TopicPartition> partitions = Arrays.asList(
                new TopicPartition(dataSource, nodeNum),
                new TopicPartition(ontimeTriggerTopic, nodeNum)/*,
                new TopicPartition(ontimeTriggerTopic+nodeNum, 1),
                new TopicPartition(ontimeTriggerTopic+nodeNum, 2),
                new TopicPartition(ontimeTriggerTopic+nodeNum, 3)*/);
        consumer.assign(partitions);
        return consumer;
    }

    public static Producer<String, Integer> getProducer() {
        Properties produceConf = new Properties();
        produceConf.setProperty("bootstrap.servers", bootServers);
        produceConf.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        produceConf.setProperty("value.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");

        return new KafkaProducer<>(produceConf);
    }

    public static int count(ArrayList<CustomValue> records) {
        return records.size();
    }

    public static void main(String[] args) {
//        Random rand = new Random();
        Consumer<String, Integer> consumer = getConsumer();

        Producer<String, Integer> producer = getProducer();

        boolean onTopicSelect = true; // on-time Topic 선택 true:1, false:2
        boolean ptimeWatermark = false; // p-time watermark
        boolean onTrigger = false;  // on-time 처리중

        int currentWindow = 1;
        String key;
        int value;

        for (int i = 0; i < 30000; i++) {
            int recordWindow;
            ConsumerRecords<String, Integer> records = consumer.poll(Duration.ofMillis(10));
            for (ConsumerRecord<String, Integer> record : records) {
                ProducerRecord<String, Integer> producerRecord;
                String msgString = String.format("key:%s, value:%d, topic:%s, partition:%d, offset:%d",
                        record.key(), record.value(), record.topic(), record.partition(), record.offset());
                System.out.println(msgString);
                // 첫 on-time 프로세스
                if (record.key().equals(FIRST_ONTIME_TM)) {
                    String resultWindow;
                    int resultCount;
                    // Immediate processing and send to on-time result topic
                    if (onTopicSelect) {
                        if (ptimeWatermark) {
                            if (!topic2Records.isEmpty()) {
                                System.out.println("Topic2 Triggering");
                                onTrigger = true;
                                resultWindow = topic2Records.get(0).window();
                                resultCount = count(topic2Records);
                                topic2Records.clear();
                                ProducerRecord<String, Integer> ontimeResultRecord = new ProducerRecord<>(
                                        onResultTopic, resultWindow, resultCount);
                                producer.send(ontimeResultRecord);
                                producerRecord = new ProducerRecord<>(tmTopic, nodeNum, FIRST_ONTIME_COMPLETE, Integer.valueOf(resultWindow));
                                producer.send(producerRecord);
                                System.out.println("window: " + resultWindow + " count: " + resultCount);
                            }
                        } else {
                            if (!topic1Records.isEmpty()) {
                                System.out.println("Topic1 Triggering");
                                onTrigger = true;
                                ptimeWatermark = true;
                                resultWindow = topic1Records.get(0).window();
                                resultCount = count(topic1Records);
                                topic1Records.clear();
                                ProducerRecord<String, Integer> ontimeResultRecord = new ProducerRecord<>(
                                        onResultTopic, resultWindow, resultCount);
                                producer.send(ontimeResultRecord);
                                producerRecord = new ProducerRecord<>(tmTopic, nodeNum, FIRST_ONTIME_COMPLETE, Integer.valueOf(resultWindow));
                                producer.send(producerRecord);
                                System.out.println("window: " + resultWindow + " count: " + resultCount);
                            }
                        }
                    } else {
                        if (ptimeWatermark) {
                            if (!topic1Records.isEmpty()) {
                                System.out.println("Topic1 Triggering");
                                onTrigger = true;
                                resultWindow = topic1Records.get(0).window();
                                resultCount = count(topic1Records);
                                topic1Records.clear();
                                ProducerRecord<String, Integer> ontimeResultRecord = new ProducerRecord<>(
                                        onResultTopic, resultWindow, resultCount);
                                producer.send(ontimeResultRecord);
                                producerRecord = new ProducerRecord<>(tmTopic, nodeNum, FIRST_ONTIME_COMPLETE, Integer.valueOf(resultWindow));
                                producer.send(producerRecord);
                                System.out.println("window: " + resultWindow + " count: " + resultCount);
                            }
                        } else {
                            if (!topic2Records.isEmpty()) {
                                System.out.println("Topic2 Triggering");
                                onTrigger = true;
                                ptimeWatermark = true;
                                resultWindow = topic2Records.get(0).window();
                                resultCount = count(topic2Records);
                                topic2Records.clear();
                                ProducerRecord<String, Integer> ontimeResultRecord = new ProducerRecord<>(
                                        onResultTopic, resultWindow, resultCount);
                                producer.send(ontimeResultRecord);
                                producerRecord = new ProducerRecord<>(tmTopic, nodeNum, FIRST_ONTIME_COMPLETE, Integer.valueOf(resultWindow));
                                producer.send(producerRecord);
                                System.out.println("window: " + resultWindow + " count: " + resultCount);
                            }
                        }
                    }
                } else if (record.key().equals(FIRST_ONTIME_COMPLETE)) {
                    System.out.println("First Ontime complete!");
                    onTrigger = false;
                } else if (record.key().equals(PTIME_COMPLETE)) {
                    // When the first processing ends
                    System.out.println("Processing complete!");
                    ptimeWatermark = false;
                    currentWindow = record.value();
                } else {
                    key = record.key();
                    value = record.value();

                    recordWindow = Integer.valueOf(key);
                    // 레코드 윈도우가 현재 윈도우보다 작으면 p-time
                    if (recordWindow < currentWindow) {
                        producerRecord = new ProducerRecord<>(pTopic, key, value);
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
                    } else {
                        // 레코드 윈도우가 현재 윈도우보다 크면 트리거 메시지 전송
                        if (recordWindow > currentWindow) {
                            if (!onTrigger) {
                                // value: next window
                                System.out.println("Send TM");
                                producerRecord = new ProducerRecord<>(tmTopic, nodeNum, FIRST_ONTIME_TM, recordWindow);
                                producer.send(producerRecord);
                            }
                            if (!ptimeWatermark) {
                                onTopicSelect = !onTopicSelect;
                                ptimeWatermark = true;
                            }
                        }
                        // if p-time watermark is true, send messages to ptime topic
                        if (ptimeWatermark) {
                            if (recordWindow == currentWindow) {
                                producerRecord = new ProducerRecord<>(pTopic, key, value);
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
                            } else {
                                if (onTopicSelect) {
                                    System.out.println("ptime topic1 " + key + " " + value);
                                    topic1Records.add(new CustomValue(key, value));
                                } else {
                                    System.out.println("ptime topic2 " + key + " " + value);
                                    topic2Records.add(new CustomValue(key, value));
                                }
                            }
                        } else { // p-time watermark is false
                            // 레코드 윈도우가 현재 윈도우보다 크거나 같으면 on-time
                            if (onTopicSelect) {
                                System.out.println("ontime topic1 " + key + " " + value);
                                topic1Records.add(new CustomValue(key, value));
                            } else {
                                System.out.println("ontime topic2 " + key + " " + value);
                                topic2Records.add(new CustomValue(key, value));
                            }
                        }
                    }
                }
            }
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        consumer.close();
        producer.close();
    }
}
