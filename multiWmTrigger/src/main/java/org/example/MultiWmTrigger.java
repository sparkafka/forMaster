package org.example;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class MultiWmTrigger {
    private static final int ONTIME_TRIGGER_COUNT = 1;
    final private static int NODES = 4;
    private static final String FIRST_ONTIME_TM = "F";
    final private static String WM0_COMPLETE = "C0";
    final private static String WM1_COMPLETE = "C1";
    final private static String WM0 = "W0";
    final private static String WM1 = "W1";

    private static final String bootServers = "node1:9092,node2:9092,node3:9092,node4:9092";
    private static final String tmTopic = "tmTopic";
    private static final String ontimeTriggerTopic = "ontimeTriggerTopic";
    final private static String ptimeTriggerTopic = "ptimeTriggerTopic";
    final private static String onResultTrigger = "ontimeResultTrigger";

    public static void main(String[] args) throws InterruptedException {
        Properties consumeConf = new Properties();
        consumeConf.setProperty("bootstrap.servers", bootServers);
        consumeConf.setProperty("group.id", "MultiWmTrigger");
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

        int currentWindow = 1;
        // 완료여부만 확인하자
        int countW0Complete = 0;
        int countW1Complete = 0;

        while(true) {
            ConsumerRecords<String, CustomValue> consumerRecords = consumer.poll(Duration.ofMillis(10));
            for (ConsumerRecord<String, CustomValue> consumerRecord : consumerRecords) {
                String consumeTm = consumerRecord.key();
                int nodeWindow = consumerRecord.value().window;
                int node = consumerRecord.value().value;
                String msgString = String.format("key:%s, window:%d, node:%d", consumeTm, nodeWindow, node);
                System.out.println(msgString);
                switch (consumeTm) {
                    case WM0:
                        System.out.println("Wm0 processing");
                        currentWindow++;
                        for (int j = 0; j < NODES; j++) {
                            CustomValue tm = new CustomValue(currentWindow, j);
                            ProducerRecord<String, CustomValue> record = new ProducerRecord<>(
                                    ontimeTriggerTopic, j, WM0, tm);
                            producer.send(record);
                        }
                        break;
                    case WM1:
                        System.out.println("Wm1 processing");
                        for (int j = 0; j < NODES; j++) {
                            CustomValue tm = new CustomValue(currentWindow, j);
                            ProducerRecord<String, CustomValue> record = new ProducerRecord<>(
                                    ontimeTriggerTopic, j, WM1, tm);
                            producer.send(record);
                        }
                        break;
                    case WM0_COMPLETE:
                        countW0Complete++;
                        if (countW0Complete>=NODES) {
                            System.out.println("Wm0 complete");
                            CustomValue wm0Tm = new CustomValue(currentWindow, 0);
                            ProducerRecord<String, CustomValue> wm0Record = new ProducerRecord<>(
                                    onResultTrigger, WM0_COMPLETE, wm0Tm);
                            producer.send(wm0Record);
                            for (int j = 0; j < NODES; j++) {
                                CustomValue tm = new CustomValue(currentWindow, j);
                                ProducerRecord<String, CustomValue> record = new ProducerRecord<>(
                                        ontimeTriggerTopic, j, WM0_COMPLETE, tm);
                                producer.send(record);
                            }
                            countW0Complete=0;
                        }
                        break;
                    case WM1_COMPLETE:
                        countW1Complete++;
                        if (countW1Complete>=NODES) {
                            System.out.println("Wm1 complete");
                            CustomValue wm1Tm = new CustomValue(currentWindow, 0);
                            ProducerRecord<String, CustomValue> wm1Record = new ProducerRecord<>(
                                    onResultTrigger, WM1_COMPLETE, wm1Tm);
                            producer.send(wm1Record);
                            for (int j = 0; j < NODES; j++) {
                                CustomValue tm = new CustomValue(currentWindow, j);
                                ProducerRecord<String, CustomValue> record = new ProducerRecord<>(
                                        ontimeTriggerTopic, j, WM1_COMPLETE, tm);
                                producer.send(record);
                            }
                            countW1Complete=0;
                        }
                        break;
                }
            }
            Thread.sleep(1);
        }
    }
}
