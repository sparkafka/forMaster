package org.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.consumer.*;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class MultiTriggerManager {
    private static int ONTIME_TRIGGER_COUNT = 4;
    private static String FIRST_ONTIME_TM = "0";

    private static String bootServers = "node1:9092,node2:9092,node3:9092,node4:9092";
    private static String tmTopic = "tmTopic";
    private static String ontimeTriggerTopic = "ontimeTriggerTopic";

    public static void main(String[] args) {
        Properties consumeConf = new Properties();
        consumeConf.setProperty("bootstrap.servers", bootServers);
        consumeConf.setProperty("group.id", "MultiTriggerManager");
        consumeConf.setProperty("max.poll.records", "1");
        consumeConf.setProperty("auto.offset.reset", "latest");
        // conf.setProperty("enable.auto.commit", "false"); auto.offset.reset
        consumeConf.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumeConf.setProperty("value.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        Consumer<String, Integer> consumer = new KafkaConsumer<>(consumeConf);
        consumer.subscribe(Collections.singletonList(tmTopic));

        Properties produceConf = new Properties();
        produceConf.setProperty("bootstrap.servers", bootServers);
        produceConf.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        produceConf.setProperty("value.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");

        Producer<String, Integer> producer = new KafkaProducer<>(produceConf);

        int msg = 0;
        int countFirstOnTime = 0;
        int countLateOnTime = 0;
        int countPtimeSplit = 0;
        for (int i = 0; i < 30000; i++) {
            ConsumerRecords<String, Integer> consumerRecords = consumer.poll(Duration.ofMillis(10));
            for (ConsumerRecord<String, Integer> consumerRecord : consumerRecords) {
                String msgString = String.format("key:%s, node:%d", consumerRecord.key(), consumerRecord.value());
                System.out.println(msgString);
                if (consumerRecord.key().equals(FIRST_ONTIME_TM)) {
                    msg = 0;
                    countFirstOnTime++;
                }
            }
            if (msg == 0) { // First on-time
                if (countFirstOnTime >= ONTIME_TRIGGER_COUNT) {
                    System.out.println("First on-time processing");
                    for (int j = 0; j < 4; j++) {
                        ProducerRecord<String, Integer> record = new ProducerRecord<>(ontimeTriggerTopic, j, FIRST_ONTIME_TM, j);
                        producer.send(record);
                    }
                    countFirstOnTime = 0;
                }
            } else if (msg == 1){ // Late on-time

            } else if (msg == 2){ // P-time split

            }

            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        consumer.close();
    }
}
