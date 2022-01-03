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
    private static final int TRIGGER_COUNT = 1;
    final private static int NODES = 1;

    private static final String FIRST_ONTIME_TM = "F";
    final private static String FIRST_ONTIME_COMPLETE = "C";
    final private static String PTIME_COMPLETE = "P";

    private static final String tmTopic = "tmTopic";
    private static final String ontimeTriggerTopic = "ontimeTriggerTopic";
    final private static String ptimeTriggerTopic = "ptimeTriggerTopic";
    final private static String onResultTrigger = "ontimeResultTrigger";

    private static final String bootServers = "node0:9092,node1:9092,node2:9092,node3:9092";

    public static void main(String[] args) throws InterruptedException {
        Properties consumeConf = new Properties();
        consumeConf.setProperty("bootstrap.servers", bootServers);
        consumeConf.setProperty("group.id", "SingleTriggerManager");
        consumeConf.setProperty("max.poll.records", "1");
        consumeConf.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumeConf.setProperty("value.deserializer", "org.example.CustomValueDeserializer");
        Consumer<String, CustomValue> consumer = new KafkaConsumer<>(consumeConf);
        consumer.subscribe(Collections.singletonList(tmTopic));

        Properties produceConf = new Properties();
        produceConf.setProperty("bootstrap.servers", bootServers);
        produceConf.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        produceConf.setProperty("value.serializer", "org.example.CustomValueSerializer");

        Producer<String, CustomValue> producer = new KafkaProducer<>(produceConf);

        // 현재 상태
        int status = 0;

        // 현재 윈도우, 다음 윈도우
        int currentWindow = 1;
        int nextWindow = 1;
        int msg = 0;

        // 받은 메시지 개수
        // countFirstOnTime 이상이면 Triggering
        int countFirstOnTime = 0;
        int countOntimeComplete = 0;


        while(true) {
            ConsumerRecords<String, CustomValue> consumerRecords = consumer.poll(Duration.ofMillis(10));
            for (ConsumerRecord<String, CustomValue> consumerRecord : consumerRecords) {
                String consumeTm = consumerRecord.key();
                int nodeWindow = consumerRecord.value().window;
                int node = consumerRecord.value().value;
                String msgString = String.format("key:%s, window:%d, node:%d", consumeTm, nodeWindow, node);
                System.out.println(msgString);
                switch (consumeTm) {
                    case FIRST_ONTIME_TM:
                        if (status == 0) {
                            msg = 1;
                            if (currentWindow < nodeWindow) {
                                System.out.println("nodeWindow: " + nodeWindow + " currentWindow: " + currentWindow);
                                countFirstOnTime++;
                            }
                            if (nextWindow < nodeWindow) {
                                nextWindow = nodeWindow;
                            }
                            if (countFirstOnTime >= TRIGGER_COUNT) {
                                status = 1;
                                System.out.println("First on-time processing");
                                CustomValue tm = new CustomValue(currentWindow, 0);
                                ProducerRecord<String, CustomValue> record = new ProducerRecord<>(
                                        ontimeTriggerTopic, 0, FIRST_ONTIME_TM, tm);
                                producer.send(record);
                                countFirstOnTime = 0;
                                System.out.println("next window " + nextWindow);
                            }
                        }
                        break;
                    case FIRST_ONTIME_COMPLETE:
                        if (status == 1) {
                            msg = 2;
                            countOntimeComplete++;
                            if (countOntimeComplete >= TRIGGER_COUNT) {
                                status = 2;
                                System.out.println("First on-time processing complete");
                                CustomValue tm = new CustomValue(currentWindow, 0);
                                ProducerRecord<String, CustomValue> record = new ProducerRecord<>(
                                        ontimeTriggerTopic, 0, FIRST_ONTIME_COMPLETE, tm);
                                producer.send(record);
                                record = new ProducerRecord<>(ptimeTriggerTopic, FIRST_ONTIME_COMPLETE, tm);
                                producer.send(record);
                                countOntimeComplete = 0;
                                System.out.println("countFirstOnTime " + countFirstOnTime);
                            }
                        }
                        break;
                    case PTIME_COMPLETE:
                        if (status == 2) {
                            msg = 3;
                            status = 0;
                            System.out.println("p-time processing complete");
                            currentWindow = nextWindow;
                            CustomValue tm = new CustomValue(currentWindow, 0);
                            ProducerRecord<String, CustomValue> record = new ProducerRecord<>(
                                    ontimeTriggerTopic, 0, PTIME_COMPLETE, tm);
                            producer.send(record);
                            record = new ProducerRecord<>(ptimeTriggerTopic, PTIME_COMPLETE, tm);
                            producer.send(record);
                        }
                        break;
                    default:
                }
            }
            Thread.sleep(1);
        }

    }
}
