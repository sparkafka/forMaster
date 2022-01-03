package org.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;

// Store messages in memory and send the messages to process topic
public class MultiOntimeSplit {
//    final private static int nodeNum = 1;
//    final private static int nodeNum = 2;
//    final private static int nodeNum = 3;
    final private static int nodeNum = 4;

    final private static String DATA = "D";
    final private static String FIRST_ONTIME_TM = "F";
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

    public static Consumer<String, CustomValue> getConsumer() {
        Consumer<String, CustomValue> consumer;

        Properties consumeConf = new Properties();
        consumeConf.setProperty("bootstrap.servers", bootServers);
        consumeConf.setProperty("group.id", "MultiOntimeSplitter");
        consumeConf.setProperty("max.poll.records", "1");
        consumeConf.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumeConf.setProperty("value.deserializer", "org.example.CustomValueDeserializer");
        consumer = new KafkaConsumer<>(consumeConf);
//        consumer.subscribe(Arrays.asList(dataSource, ontimeTriggerTopic));
        List<TopicPartition> partitions = Arrays.asList(
                new TopicPartition(dataSource, nodeNum-1),
                new TopicPartition(ontimeTriggerTopic, nodeNum-1));
        consumer.assign(partitions);
        return consumer;
    }

    public static Producer<String, CustomValue> getProducer() {
        Properties produceConf = new Properties();
        produceConf.setProperty("bootstrap.servers", bootServers);
        produceConf.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        produceConf.setProperty("value.serializer", "org.example.CustomValueSerializer");

        return new KafkaProducer<>(produceConf);
    }

    public static void main(String[] args) throws InterruptedException {
        Consumer<String, CustomValue> consumer = getConsumer();

        Producer<String, CustomValue> producer = getProducer();

        boolean pTimeMark = false; // p-time watermark true: p-time, false: on-time
        boolean onTimeProcessing = false;  // on-time 처리중

        // 0: 온타임 데이터 - 피타임 프로세싱 완료
        // 1: 피타임 시작 - 아직 온타임 프로세싱 X
        // 2: 온타임 프로세싱 중
        // 3: 온타임 프로세싱 완료 - 아직 피타임
        byte status = 0;
        boolean ptime = false;
        int currentWindow = 1;
        String key;
        CustomValue cValue;

        // On time Data는 On time Topic으로, P time Data는 P time Topic으로
        while(true) {
            int recordWindow;
            ConsumerRecords<String, CustomValue> records = consumer.poll(Duration.ofMillis(10));
            for (ConsumerRecord<String, CustomValue> record : records) {
                ProducerRecord<String, CustomValue> producerRecord;
                String msgString = String.format("key:%s, window:%d, value:%d, topic:%s, partition:%d",
                        record.key(), record.value().window, record.value().value, record.topic(), record.partition());
                System.out.println(msgString);
                // 첫 on-time 프로세스
                switch (record.key()) {
                    case FIRST_ONTIME_TM:
                        // Immediate processing and send to on-time result topic
                        System.out.println("On-time Processing");
                        ptime = true;
                        ProducerRecord<String, CustomValue> ontimeTrigger = new ProducerRecord<>(
                                onTopic1,nodeNum-1 ,FIRST_ONTIME_TM, record.value());
                        producer.send(ontimeTrigger);
                        break;
                    case FIRST_ONTIME_COMPLETE:
                        System.out.println("First On-time Processing complete!");
                        ptime = false;
                        break;
                    case PTIME_COMPLETE:
                        // When the first processing ends
                        System.out.println("Entire Processing complete!");
                        ptime = false;
                        System.out.print("Window changed: " + currentWindow + " to ");
                        currentWindow = record.value().window;
                        System.out.println(currentWindow);
                        ProducerRecord<String, CustomValue> ptimeComplete = new ProducerRecord<>(
                                onTopic1, nodeNum-1 ,PTIME_COMPLETE, record.value());
                        producer.send(ptimeComplete);
                        break;
                    case DATA:
                        key = record.key();
                        cValue = record.value();

                        recordWindow = cValue.window;
                        // 레코드 윈도우가 현재 윈도우보다 작으면 p-time topic으로 보내기
                        if (recordWindow < currentWindow) {
                            System.out.println("p-time data-key: " + key + " window: " + cValue.window + " value: " + cValue.value);
                            cValue.setEventTime(record.timestamp());
                            producerRecord = new ProducerRecord<>(pTopic, DATA, cValue);
                        } else { // recordWindow >= currentWindow
                            // 레코드 윈도우가 현재 윈도우보다 크면 트리거 메시지 전송
                            if (recordWindow > currentWindow && !ptime) {

                                // value: 지금 처리할 window, 현재 노드번호
                                System.out.println("Send TM");
                                CustomValue tm = new CustomValue(recordWindow, nodeNum);
                                producerRecord = new ProducerRecord<>(tmTopic, FIRST_ONTIME_TM, tm);
                                producer.send(producerRecord);
                            }
                            // p-time 일때
                            if (ptime) {
                                // currentWindow == recordWindow면 일단 ptime topic으로 보내기
                                if (recordWindow == currentWindow) {
                                    System.out.println("p-time data-key: " + key + " window: " + cValue.window + " value: " + cValue.value);
                                    cValue.setEventTime(record.timestamp());
                                    producerRecord = new ProducerRecord<>(pTopic, DATA, cValue);
                                } else {
                                    // recordWindow > currentWindow 이면 on-time topic에 전송
                                    System.out.println("on-time data-key: " + key + " window: " + cValue.window + " value: " + cValue.value);
                                    cValue.setEventTime(record.timestamp());
                                    producerRecord = new ProducerRecord<>(onTopic1, nodeNum-1, DATA, cValue);
                                }
                            } else { // on-time 일때
                                // 레코드 윈도우가 현재 윈도우보다 크거나 같으면 on-time topic에 전송
                                System.out.println("on-time data-key: " + key + " window: " + cValue.window + " value: " + cValue.value);
                                cValue.setEventTime(record.timestamp());
                                producerRecord = new ProducerRecord<>(onTopic1, nodeNum-1, DATA, cValue);
                            }
                        }
                        producer.send(producerRecord);
                        break;
                }
            }

            Thread.sleep(1);

        }
    }
}
