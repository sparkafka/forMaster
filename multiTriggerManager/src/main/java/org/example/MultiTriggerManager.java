package org.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.consumer.*;

import java.time.Duration;
import java.util.*;

public class MultiTriggerManager {
    private static final int ONTIME_TRIGGER_COUNT = 2;
    final private static int NODES = 4;
    private static final String FIRST_ONTIME_TM = "F";
    final private static String FIRST_ONTIME_COMPLETE = "C";
    final private static String PTIME_COMPLETE = "P";

    private static final String bootServers = "node1:9092,node2:9092,node3:9092,node4:9092";
    private static final String tmTopic = "tmTopic";
    private static final String ontimeTriggerTopic = "ontimeTriggerTopic";
    final private static String ptimeTriggerTopic = "ptimeTriggerTopic";
    final private static String onResultTrigger = "ontimeResultTrigger";

    public static void main(String[] args) throws InterruptedException {
        Properties consumeConf = new Properties();
        consumeConf.setProperty("bootstrap.servers", bootServers);
        consumeConf.setProperty("group.id", "MultiTriggerManager");
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

        // node를 판별할 HashSet
        HashSet<Integer> nodeSet = new HashSet<>();

        // 현재 상태
        // 0: On-time 데이터 받는중(P-time Complete)
        // 1: On-time processing
        // 2: On-time complete(P-time processing)
        int status = 0;

        // 현재 윈도우, 다음 윈도우
        int currentWindow = 1;
        int nextWindow = 1;
        int msg = 0;

        // 받은 메시지 개수
        int countFirstOnTime = 0;
        int countOntimeComplete = 0;

        while(true){
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
//                                countFirstOnTime++;
                                nodeSet.add(node);
                            }
                            if (nextWindow < nodeWindow) {
                                nextWindow = nodeWindow;
                            }
//                            if (countFirstOnTime >= ONTIME_TRIGGER_COUNT) {
                            if(nodeSet.size()==ONTIME_TRIGGER_COUNT){
                                System.out.println("nodeWindow: " + nodeWindow + " currentWindow: " + currentWindow);
                                status = 1;
                                System.out.println("First on-time processing");
                                for (int j = 0; j < NODES; j++) {
                                    CustomValue tm = new CustomValue(currentWindow, j);
                                    ProducerRecord<String, CustomValue> record = new ProducerRecord<>(
                                            ontimeTriggerTopic, j, FIRST_ONTIME_TM, tm);
                                    producer.send(record);
                                }

//                                countFirstOnTime = 0;
                                nodeSet.clear();
                                System.out.println("next window " + nextWindow);
                            }
                        }
                        break;
                    case FIRST_ONTIME_COMPLETE:
                        if (status == 1) {
                            msg = 2;
                            countOntimeComplete++;
                            if (countOntimeComplete >= NODES) {
                                status = 2;
                                System.out.println("First on-time processing complete");
                                for (int j = 0; j < NODES; j++) {
                                    CustomValue tm = new CustomValue(currentWindow, j);
                                    ProducerRecord<String, CustomValue> record = new ProducerRecord<>(
                                            ontimeTriggerTopic, j, FIRST_ONTIME_COMPLETE, tm);
                                    producer.send(record);
                                }
                                CustomValue tm = new CustomValue(currentWindow, 0);
                                ProducerRecord<String, CustomValue> record = new ProducerRecord<>(
                                        ptimeTriggerTopic, FIRST_ONTIME_COMPLETE, tm);
                                producer.send(record);
                                countOntimeComplete = 0;
                            }
                        }
                        break;
                    case PTIME_COMPLETE:
                        if (status == 2) {
                            msg = 3;
                            status = 0;
                            System.out.println("p-time processing complete");
                            currentWindow = nextWindow;
                            for (int j = 0; j < NODES; j++) {
                                CustomValue tm = new CustomValue(currentWindow, j);
                                ProducerRecord<String, CustomValue> record = new ProducerRecord<>(
                                        ontimeTriggerTopic, j, PTIME_COMPLETE, tm);
                                producer.send(record);
                            }
                            CustomValue tm = new CustomValue(currentWindow, 0);
                            ProducerRecord<String, CustomValue> record = new ProducerRecord<>(
                                    ptimeTriggerTopic, PTIME_COMPLETE, tm);
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
