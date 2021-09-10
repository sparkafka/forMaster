package org.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.consumer.*;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class MultiTriggerManager {
    private static final int ONTIME_TRIGGER_COUNT = 4;
    final private static int NODES = 4;
    private static final String FIRST_ONTIME_TM = "O";
    final private static String FIRST_ONTIME_COMPLETE = "C";
    final private static String PTIME_COMPLETE = "P";

    private static final String bootServers = "node1:9092,node2:9092,node3:9092,node4:9092";
    private static final String tmTopic = "tmTopic";
    private static final String ontimeTriggerTopic = "ontimeTriggerTopic";
    final private static String ptimeTriggerTopic = "ptimeTriggerTopic";
    final private static String onResultTrigger = "ontimeResultTrigger";

    public static void main(String[] args) {
        Properties consumeConf = new Properties();
        consumeConf.setProperty("bootstrap.servers", bootServers);
        consumeConf.setProperty("group.id", "MultiTriggerManager");
        consumeConf.setProperty("max.poll.records", "1");
        consumeConf.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumeConf.setProperty("value.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        Consumer<String, Integer> consumer = new KafkaConsumer<>(consumeConf);
        consumer.subscribe(Collections.singletonList(tmTopic));

        Properties produceConf = new Properties();
        produceConf.setProperty("bootstrap.servers", bootServers);
        produceConf.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        produceConf.setProperty("value.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");

        Producer<String, Integer> producer = new KafkaProducer<>(produceConf);

        // 현재 상태
        int status = 0;

        int currentWindow = 1;
        int nextWindow = 1;
        int msg = 0;

        // 받은 메시지 개수
        int countFirstOnTime = 0;
        int countOntimeComplete = 0;

        for (int i = 0; i < 30000; i++) {
            ConsumerRecords<String, Integer> consumerRecords = consumer.poll(Duration.ofMillis(10));
            for (ConsumerRecord<String, Integer> consumerRecord : consumerRecords) {
                String tm = consumerRecord.key();
                int nodeWindow = consumerRecord.value();
                int node = consumerRecord.partition();
                String msgString = String.format("key:%s, window:%d, node:%d", tm, nodeWindow, node);
                System.out.println(msgString);
                switch (consumerRecord.key()) {
                    case FIRST_ONTIME_TM:
                        if (status == 0) {
                            msg = 1;
                            if (currentWindow < nodeWindow) {
                                countFirstOnTime++;
                            }
                            if (nextWindow < nodeWindow) {
                                nextWindow = nodeWindow;
                            }
                        }
                        break;
                    case FIRST_ONTIME_COMPLETE:
                        if (status == 1) {
                            msg = 2;
                            countOntimeComplete++;
                        }
                        break;
                    case PTIME_COMPLETE:
                        if (status == 2) {
                            msg = 3;
                        }
                        break;
                    default:
                }
            }
            switch (msg) {
                case 1:
                    if (countFirstOnTime >= ONTIME_TRIGGER_COUNT) {
                        status = 1;
                        System.out.println("First on-time processing");
//                        ProducerRecord<String, Integer> record = new ProducerRecord<>(ontimeTriggerTopic, 0, FIRST_ONTIME_TM, currentWindow);
//                        producer.send(record);
                        for (int j = 0; j < 4; j++) {
                            ProducerRecord<String, Integer> record = new ProducerRecord<>(ontimeTriggerTopic, j, FIRST_ONTIME_TM, currentWindow);
                            producer.send(record);
                        }
                        countFirstOnTime = 0;
                    }
                    msg = 0;
                    break;
                case 2:
                    if (countOntimeComplete >= ONTIME_TRIGGER_COUNT) {
                        status = 2;
                        System.out.println("First on-time processing complete");
//                        ProducerRecord<String, Integer> record = new ProducerRecord<>(ontimeTriggerTopic, FIRST_ONTIME_COMPLETE, currentWindow);
//                        producer.send(record);
                        for (int j = 0; j < 4; j++) {
                            ProducerRecord<String, Integer> record = new ProducerRecord<>(ontimeTriggerTopic, j, FIRST_ONTIME_COMPLETE, currentWindow);
                            producer.send(record);
                        }
                        ProducerRecord<String, Integer> record = new ProducerRecord<>(ptimeTriggerTopic, FIRST_ONTIME_COMPLETE, currentWindow);
                        producer.send(record);
                        countFirstOnTime = 0;
                        countOntimeComplete = 0;
                    }
                    msg = 0;
                    break;
                case 3:
                    status = 0;
                    System.out.println("p-time processing complete");
                    currentWindow = nextWindow;
//                    ProducerRecord<String, Integer> record = new ProducerRecord<>(ontimeTriggerTopic, 0, PTIME_COMPLETE, currentWindow);
//                    producer.send(record);
                    for (int j = 0; j < 4; j++) {
                        ProducerRecord<String, Integer> record = new ProducerRecord<>(ontimeTriggerTopic, j, PTIME_COMPLETE, currentWindow);
                        producer.send(record);
                    }
                    ProducerRecord<String, Integer> record = new ProducerRecord<>(ptimeTriggerTopic, PTIME_COMPLETE, currentWindow);
                    producer.send(record);
                    record = new ProducerRecord<>(onResultTrigger, PTIME_COMPLETE, currentWindow);
                    producer.send(record);
                    msg = 0;
                    break;
                default:
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
