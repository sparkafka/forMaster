package org.example;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.consumer.*;

public class PtimeProcess {
    final private static String DATA = "D";
    final private static String PTIME_RESULT = "E";
    final private static String FIRST_ONTIME_COMPLETE = "C";
    final private static String PTIME_COMPLETE = "P";

    final private static String bootServers = "node1:9092,node2:9092,node3:9092,node4:9092";
    final private static String pDataTopic = "pDataTopic";
    final private static String tmTopic = "tmTopic";
    final private static String ptimeTriggerTopic = "ptimeTriggerTopic";
    final private static String onResultTopic = "ontimeResultTopic";

    public static void main(String[] args) throws InterruptedException {
        Properties consumeConf = new Properties();
        consumeConf.setProperty("bootstrap.servers", bootServers);
        consumeConf.setProperty("group.id", "PtimeProcess");
        consumeConf.setProperty("max.poll.records", "1");
        consumeConf.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumeConf.setProperty("value.deserializer", "org.example.CustomValueDeserializer");
        Consumer<String, CustomValue> consumer = new KafkaConsumer<>(consumeConf);
        consumer.subscribe(Collections.singletonList(pDataTopic));

        Properties produceConf = new Properties();
        produceConf.setProperty("bootstrap.servers", bootServers);
        produceConf.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        produceConf.setProperty("value.serializer", "org.example.CustomValueSerializer");

        Producer<String, CustomValue> producer = new KafkaProducer<>(produceConf);

        int currentWindow = 1;

        ArrayList<CustomValue> pTimeRecords = new ArrayList<>();

        while(true){
            ConsumerRecords<String, CustomValue> records = consumer.poll(Duration.ofMillis(10));
            for (ConsumerRecord<String, CustomValue> record : records) {
                String msgString = String.format("key:%s, window:%d, value:%d, topic:%s, partition:%d",
                        record.key(), record.value().window, record.value().value, record.topic(), record.partition());
                System.out.println(msgString);
                if (record.key().equals(FIRST_ONTIME_COMPLETE)) {
                    int delay = pTimeRecords.size();
                    System.out.println("P-time processing");
                    int recordCount = 0;
                    CustomValue resultValue;
                    if (pTimeRecords.isEmpty()){
                        resultValue = new CustomValue(currentWindow, 0);
                    } else{
                        CustomValue firstValue = pTimeRecords.get(0);
                        if (firstValue.window != currentWindow) {
                            resultValue = new CustomValue(currentWindow, 0);
                        } else {
                            while (!pTimeRecords.isEmpty()) {
                                CustomValue cv = pTimeRecords.get(0);
                                if (cv.window != currentWindow) break;

                                recordCount += 1;
                                pTimeRecords.remove(0);
                            }
                            System.out.println("window: " + currentWindow + " count: " + recordCount);
                            resultValue = new CustomValue(currentWindow, recordCount, firstValue.eventTime, 0L);
                        }
                    }
                    // on-time Result Topic으로 결과 전송
                    Thread.sleep(delay+5);
                    ProducerRecord<String, CustomValue> resultRecord = new ProducerRecord<>(onResultTopic, PTIME_RESULT, resultValue);
                    producer.send(resultRecord);

                    // 완료됐다는 것을 TM에 전송
                    CustomValue tm = new CustomValue(currentWindow, 0);
                    ProducerRecord<String, CustomValue> onTimeComplete = new ProducerRecord<>(tmTopic, PTIME_COMPLETE, tm);
                    producer.send(onTimeComplete);
                }
                else if(record.key().equals(PTIME_COMPLETE)){
                    System.out.print("Window changed: " + currentWindow + " to ");
                    currentWindow = record.value().window;
                    System.out.println(currentWindow);
                }
                else if(record.key().equals(DATA)){
                    System.out.println("Data add: " + record.key() + " " + record.value().window + " " + record.value().value);
                    pTimeRecords.add(record.value());
                }
            }
            Thread.sleep(1);
        }
    }
}
