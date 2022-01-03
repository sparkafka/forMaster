package org.example;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class MultiWm0Process {
//    final private static int nodeNum = 1;
//    final private static int nodeNum = 2;
//    final private static int nodeNum = 3;
    final private static int nodeNum = 4;

    final private static String DATA = "D";
    final private static String WM0_RESULT = "R0";
    final private static String FIRST_ONTIME_TM = "F";
    final private static String FIRST_ONTIME_COMPLETE = "C";
    final private static String PTIME_COMPLETE = "P";
    final private static String WM0 = "W0";
    final private static String WM1 = "W1";
    final private static String WM0_COMPLETE = "C0";
    final private static String WM1_COMPLETE = "C1";

    final private static String bootServers = "node1:9092,node2:9092,node3:9092,node4:9092";
    final private static String ontimeTriggerTopic = "ontimeTriggerTopic";
    final private static String onTopic1 = "ontimeTopic1";
    final private static String onTopic2 = "ontimeTopic2";
    final private static String onResultTopic = "ontimeResultTopic";
    final private static String tmTopic = "tmTopic";

    public static void main(String[] args) throws InterruptedException {
        Properties consumeConf = new Properties();
        consumeConf.setProperty("bootstrap.servers", bootServers);
        consumeConf.setProperty("group.id", "MultiWmProcess");
        consumeConf.setProperty("max.poll.records", "1");
        consumeConf.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumeConf.setProperty("value.deserializer", "org.example.CustomValueDeserializer");

        Consumer<String, CustomValue> consumer = new KafkaConsumer<>(consumeConf);

        Properties produceConf = new Properties();
        produceConf.setProperty("bootstrap.servers", bootServers);
        produceConf.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        produceConf.setProperty("value.serializer", "org.example.CustomValueSerializer");

        Producer<String, CustomValue> producer = new KafkaProducer<>(produceConf);

        // consumer assign
        List<TopicPartition> partitions = Collections.singletonList(
                new TopicPartition(onTopic1, nodeNum - 1));
        consumer.assign(partitions);

        // Store messages in memory
        ArrayList<CustomValue> topicRecords = new ArrayList<>();

        int currentWindow = 1;
        while (true) {
            ConsumerRecords<String, CustomValue> records = consumer.poll(Duration.ofMillis(10));
            for (ConsumerRecord<String, CustomValue> record : records) {
                String msgString = String.format("key:%s, window:%d, value:%d, topic:%s, partition:%d",
                        record.key(), record.value().window, record.value().value, record.topic(), record.partition());
//                System.out.println(msgString);
                if (record.key().equals(WM0)) {
                    int delay = topicRecords.size();
                    System.out.println("delay: "+delay);
                    System.out.println("WM Processing");
                    int recordCount = 0;
                    CustomValue resultValue;
                    if (!topicRecords.isEmpty()) {
                        int itr = 0;
                        CustomValue firstValue = null;
                        while (itr < topicRecords.size()) {
                            System.out.println("stored record window: " + topicRecords.get(itr).window);
                            if (topicRecords.get(itr).window == currentWindow) {
//                                System.out.println("stored data window: "+topicRecords.get(itr).window);
                                if (recordCount == 0) {
                                    firstValue = topicRecords.get(itr);
                                }
                                topicRecords.remove(itr);
                                recordCount++;
                            } else if(topicRecords.get(itr).window<currentWindow){
                                System.out.println("wrong data:"+topicRecords.get(itr).window);
                                topicRecords.remove(itr);
                            } else {
                                itr++;
                            }
                        }
                        if(recordCount == 0){
                            resultValue = new CustomValue(currentWindow, 0);
                        } else {
                            resultValue = new CustomValue(currentWindow, recordCount, firstValue.eventTime, 0L);
                        }
                        System.out.println("window: " + currentWindow + " count: " + recordCount);
                    } else {
                        resultValue = new CustomValue(currentWindow, 0);
                    }
                    Thread.sleep(delay);
                    ProducerRecord<String, CustomValue> resultRecord = new ProducerRecord<>(onResultTopic, WM0_RESULT, resultValue);
                    producer.send(resultRecord);

                    // 완료됐다는 것을 TM에 전송
                    CustomValue tm = new CustomValue(currentWindow, nodeNum);

                    System.out.println("WM0 Complete");
                    ProducerRecord<String, CustomValue> wm0Complete = new ProducerRecord<>(tmTopic, WM0_COMPLETE, tm);
                    producer.send(wm0Complete);
//                    System.out.print("Window changed: " + currentWindow + " to ");
//                    currentWindow = record.value().window;
//                    System.out.println(currentWindow);
                } else if (record.key().equals(WM0_COMPLETE)) {
                    System.out.print("Window changed: " + currentWindow + " to ");
                    currentWindow = record.value().window;
                    System.out.println(currentWindow);
                }
                else if (record.key().equals(DATA)) {
//                    System.out.println("Data add: " + record.key() + " " + record.value().window + " " + record.value().value);
                    topicRecords.add(record.value());
                }
            }
            Thread.sleep(1);
        }
    }
}
