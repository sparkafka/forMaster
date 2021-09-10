package org.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;

// Store messages in memory and send the messages to process topic
public class MultiOntimeSplit {
    final private static int nodeNum = 1;
//    final private static int nodeNum = 2;
//    final private static int nodeNum = 3;
//    final private static int nodeNum = 4;

    final private static String DATA = "D";
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
                new TopicPartition(dataSource, nodeNum - 1),
                new TopicPartition(ontimeTriggerTopic, nodeNum - 1)/*,
                new TopicPartition(ontimeTriggerTopic+nodeNum-1, 1),
                new TopicPartition(ontimeTriggerTopic+nodeNum-1, 2),
                new TopicPartition(ontimeTriggerTopic+nodeNum-1, 3)*/);
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

    public static int count(ArrayList<CustomValue> records) {
        return records.size();
    }

    public static void main(String[] args) {
//        Random rand = new Random();
        Consumer<String, CustomValue> consumer = getConsumer();

        Producer<String, CustomValue> producer = getProducer();

        boolean onTopicSelect = true; // on-time Topic 선택 true:1, false:2
        boolean ptimeWatermark = false; // p-time watermark true: p-time, false: on-time
        boolean onTrigger = false;  // on-time 처리중

        int currentWindow = 1;
        String key;
        CustomValue cValue;

        for (int i = 0; i < 30000; i++) {
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
                        int resultWindow;
                        int resultCount;
                        Long resultEventTime;
                        // Immediate processing and send to on-time result topic
                        // 1번 array 처리중
                        if (onTopicSelect) {
                            // 현재 p-time
                            // 1번 처리중(p-time)에 on-time data 처리 요청이 들어오면 그때까지 2번 array에 저장된 데이터 처리
                            if (ptimeWatermark) {
                                if (!topic2Records.isEmpty()) {
                                    System.out.println("Topic2 Triggering");
                                    onTrigger = true;
                                    resultWindow = topic2Records.get(0).window();
                                    resultEventTime = topic2Records.get(0).eventTime; // 첫 데이터의 eventTime
                                    // Count 하기
                                    resultCount = count(topic2Records);
                                    topic2Records.clear();

                                    CustomValue data = new CustomValue(resultWindow, resultCount, nodeNum, resultEventTime, 0L);
                                    ProducerRecord<String, CustomValue> ontimeResultRecord = new ProducerRecord<>(onResultTopic, DATA, data);
                                    producer.send(ontimeResultRecord);

                                    CustomValue tm = new CustomValue(resultWindow, nodeNum);
                                    producerRecord = new ProducerRecord<>(tmTopic, FIRST_ONTIME_COMPLETE, tm);
                                    producer.send(producerRecord);

                                    System.out.println("window: " + resultWindow + " count: " + resultCount);
                                }
                            } else { // 현재 on-time
                                if (!topic1Records.isEmpty()) {
                                    System.out.println("Topic1 Triggering");
                                    onTrigger = true; // On-time data 처리중
                                    ptimeWatermark = true; // P-time 으로 변경
                                    resultWindow = topic1Records.get(0).window();
                                    resultEventTime = topic1Records.get(0).eventTime;
                                    resultCount = count(topic1Records);
                                    topic1Records.clear();

                                    CustomValue data = new CustomValue(resultWindow, resultCount, nodeNum, resultEventTime, 0L);
                                    ProducerRecord<String, CustomValue> ontimeResultRecord = new ProducerRecord<>(onResultTopic, DATA, data);
                                    producer.send(ontimeResultRecord);

                                    CustomValue tm = new CustomValue(resultWindow, nodeNum);
                                    producerRecord = new ProducerRecord<>(tmTopic, FIRST_ONTIME_COMPLETE, tm);
                                    producer.send(producerRecord);

                                    System.out.println("window: " + resultWindow + " count: " + resultCount);
                                }
                            }
                        } else { // 2번 array 처리중
                            // 현재 p-time
                            // 2번 처리중(p-time)에 on-time data 처리 요청이 들어오면 그때까지 1번 array에 저장된 데이터 처리
                            if (ptimeWatermark) {
                                if (!topic1Records.isEmpty()) {
                                    System.out.println("Topic1 Triggering");
                                    onTrigger = true;
                                    resultWindow = topic1Records.get(0).window();
                                    resultEventTime = topic1Records.get(0).eventTime;
                                    resultCount = count(topic1Records);
                                    topic1Records.clear();

                                    CustomValue data = new CustomValue(resultWindow, resultCount, nodeNum, resultEventTime, 0L);
                                    ProducerRecord<String, CustomValue> ontimeResultRecord = new ProducerRecord<>(onResultTopic, DATA, data);
                                    producer.send(ontimeResultRecord);

                                    CustomValue tm = new CustomValue(resultWindow, nodeNum);
                                    producerRecord = new ProducerRecord<>(tmTopic, FIRST_ONTIME_COMPLETE, tm);
                                    producer.send(producerRecord);

                                    System.out.println("window: " + resultWindow + " count: " + resultCount);
                                }
                            } else { // 현재 on-time
                                if (!topic2Records.isEmpty()) {
                                    System.out.println("Topic2 Triggering");
                                    onTrigger = true;
                                    ptimeWatermark = true;
                                    resultWindow = topic2Records.get(0).window();
                                    resultEventTime = topic2Records.get(0).eventTime;
                                    // Count 하기
                                    resultCount = count(topic2Records);
                                    topic2Records.clear();

                                    CustomValue data = new CustomValue(resultWindow, resultCount, nodeNum, resultEventTime, 0L);
                                    ProducerRecord<String, CustomValue> ontimeResultRecord = new ProducerRecord<>(onResultTopic, DATA, data);
                                    producer.send(ontimeResultRecord);

                                    CustomValue tm = new CustomValue(resultWindow, nodeNum);
                                    producerRecord = new ProducerRecord<>(tmTopic, FIRST_ONTIME_COMPLETE, tm);
                                    producer.send(producerRecord);

                                    System.out.println("window: " + resultWindow + " count: " + resultCount);
                                }
                            }
                        }
                        break;
                    case FIRST_ONTIME_COMPLETE:
                        System.out.println("First Ontime complete!");
                        onTrigger = false;
                        break;
                    case PTIME_COMPLETE:
                        // When the first processing ends
                        System.out.println("Processing complete!");
                        ptimeWatermark = false;
                        currentWindow = record.value().window;
                        break;
                    default:
                        key = record.key();
                        cValue = record.value();

                        recordWindow = cValue.window;
                        // 레코드 윈도우가 현재 윈도우보다 작으면 p-time topic으로 보내기
                        if (recordWindow < currentWindow) {
                            producerRecord = new ProducerRecord<>(pTopic, DATA, cValue);
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
                        } else { // recordWindow >= currentWindow
                            // 레코드 윈도우가 현재 윈도우보다 크면 트리거 메시지 전송
                            if (recordWindow > currentWindow) {
                                // on-time 처리중이 아닐때만 tm 전송
                                if (!onTrigger) {
                                    // value: 다음에 처리할 window와 tm이라는 뜻의 -1
                                    System.out.println("Send TM");
                                    CustomValue tm = new CustomValue(recordWindow, -1);
                                    producerRecord = new ProducerRecord<>(tmTopic, FIRST_ONTIME_TM, tm);
                                    producer.send(producerRecord);
                                }
                                // p-time이 아니면 데이터를 저장중인 array 바꾸기
                                if (!ptimeWatermark) {
                                    onTopicSelect = !onTopicSelect;
                                    ptimeWatermark = true;
                                }
                            }
                            // p-time 일때
                            if (ptimeWatermark) {
                                // currentWindow == recordWindow면 일단 ptime topic으로 보내기
                                if (recordWindow == currentWindow) {
                                    producerRecord = new ProducerRecord<>(pTopic, DATA, cValue);
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
                                    // recordWindow > currentWindow 이면 on-time array에 저장
                                    if (onTopicSelect) {
                                        System.out.println("key: " + key + " topic1 " + cValue.window + " " + cValue.value);
                                        cValue.setEventTime(record.timestamp());
                                        topic1Records.add(cValue);
                                    } else {
                                        System.out.println("key: " + key + " topic2 " + cValue.window + " " + cValue.value);
                                        cValue.setEventTime(record.timestamp());
                                        topic2Records.add(cValue);
                                    }
                                }
                            } else { // on-time 일때
                                // 레코드 윈도우가 현재 윈도우보다 크거나 같으면 on-time array에 저장
                                if (onTopicSelect) {
                                    System.out.println("on-time key: " + key + " topic1 " + cValue.window + " " + cValue.value);
                                    cValue.setEventTime(record.timestamp());
                                    topic1Records.add(cValue);
                                } else {
                                    System.out.println("on-time key: " + key + " topic2 " + cValue.window + " " + cValue.value);
                                    cValue.setEventTime(record.timestamp());
                                    topic2Records.add(cValue);
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
