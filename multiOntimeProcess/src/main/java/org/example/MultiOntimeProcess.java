package org.example;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;

public class MultiOntimeProcess {
//    final private static int nodeNum = 1;
//    final private static int nodeNum = 2;
//    final private static int nodeNum = 3;
    final private static int nodeNum = 4;

    final private static String DATA = "D";
    final private static String ONTIME_RESULT = "R";
    final private static String FIRST_ONTIME_TM = "F";
    final private static String FIRST_ONTIME_COMPLETE = "C";
    final private static String PTIME_COMPLETE = "P";

    final private static String bootServers = "node1:9092,node2:9092,node3:9092,node4:9092";
    final private static String ontimeTriggerTopic = "ontimeTriggerTopic";
    final private static String onTopic1 = "ontimeTopic1";
    final private static String onTopic2 = "ontimeTopic2";
    final private static String onResultTopic = "ontimeResultTopic";
    final private static String tmTopic = "tmTopic";

    public static void main(String[] args) throws InterruptedException {
        Properties consumeConf = new Properties();
        consumeConf.setProperty("bootstrap.servers", bootServers);
        consumeConf.setProperty("group.id", "MultiOntimeProcess");
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
                new TopicPartition(onTopic1, nodeNum-1));
        consumer.assign(partitions);

        // Store messages in memory
        ArrayList<CustomValue> topic1Records = new ArrayList<>();
        // ArrayList<Pair> topic2Records = new ArrayList<>();

        int currentWindow = 1;

        while (true) {
            ConsumerRecords<String, CustomValue> records = consumer.poll(Duration.ofMillis(10));
            for (ConsumerRecord<String, CustomValue> record : records) {
                String msgString = String.format("key:%s, window:%d, value:%d, topic:%s, partition:%d",
                        record.key(), record.value().window, record.value().value, record.topic(), record.partition());
                System.out.println(msgString);
                // 프로세싱 tm
                if (record.key().equals(FIRST_ONTIME_TM)) {
                    int delay = topic1Records.size();
                    System.out.println("First on-time processing");
                    int recordCount = 0;
                    CustomValue resultValue;
                    if (!topic1Records.isEmpty()) {
//                        CustomValue firstValue = topic1Records.get(0);
                        int itr = 0;
                        CustomValue firstValue = null;
                        while (itr < topic1Records.size()) {
                            if (topic1Records.get(itr).window == currentWindow) {
                                if (recordCount == 0) {
                                    firstValue = topic1Records.get(itr);
                                }
                                topic1Records.remove(itr);
                                recordCount++;
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
                        /*if (firstValue.window != currentWindow) {
                            resultValue = new CustomValue(currentWindow, 0);
                        } else {
                            while (!topic1Records.isEmpty()) {
                                CustomValue cv = topic1Records.get(0);
                                if (cv.window != currentWindow) break;

                                recordCount += 1;
                                topic1Records.remove(0);
                            }
                            System.out.println("window: " + currentWindow + " count: " + recordCount);
                            resultValue = new CustomValue(currentWindow, recordCount, firstValue.eventTime, 0L);
                        }
                    } else {
                        resultValue = new CustomValue(currentWindow, 0);
                    }*/

                    // 처리시간 구현
                    Thread.sleep(delay);
                    // on-time Result Topic으로 결과 전송
                    ProducerRecord<String, CustomValue> resultRecord = new ProducerRecord<>(onResultTopic, ONTIME_RESULT, resultValue);
                    producer.send(resultRecord);

                    // 완료됐다는 것을 TM에 전송
                    CustomValue tm = new CustomValue(currentWindow, nodeNum);
                    ProducerRecord<String, CustomValue> onTimeComplete = new ProducerRecord<>(tmTopic, FIRST_ONTIME_COMPLETE, tm);
                    producer.send(onTimeComplete);


                } else if (record.key().equals(PTIME_COMPLETE)) {
                    System.out.print("Window changed: " + currentWindow + " to ");
                    currentWindow = record.value().window;
                    System.out.println(currentWindow);
                }
                // data
                else if (record.key().equals(DATA)) {
                    System.out.println("Data add: " + record.key() + " " + record.value().window + " " + record.value().value);
                    topic1Records.add(record.value());
                }
            }
            Thread.sleep(1);

        }
    }
}