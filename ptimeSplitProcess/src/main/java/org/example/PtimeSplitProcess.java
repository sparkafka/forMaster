package org.example;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

public class PtimeSplitProcess {
    final private static String DATA = "D";
    final private static String FIRST_ONTIME_COMPLETE = "C";
    final private static String PTIME_COMPLETE = "P";

    final private static String bootServers = "node1:9092,node2:9092,node3:9092,node4:9092";
    final private static String pTopic = "ptimeTopic";
    final private static String pDataTopic = "pDataTopic";
    final private static String tmTopic = "tmTopic";
    final private static String ptimeTriggerTopic = "ptimeTriggerTopic";
    final private static String ltimeTopic = "ltimeTopic";

    public static void main(String[] args) throws InterruptedException {
        Properties consumeConf = new Properties();
        consumeConf.setProperty("bootstrap.servers", bootServers);
        consumeConf.setProperty("group.id", "PtimeSplitProcess");
        consumeConf.setProperty("max.poll.records", "1");
        consumeConf.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumeConf.setProperty("value.deserializer", "org.example.CustomValueDeserializer");
        Consumer<String, CustomValue> consumer = new KafkaConsumer<>(consumeConf);
        consumer.subscribe(Arrays.asList(pTopic, ptimeTriggerTopic));

        Properties produceConf = new Properties();
        produceConf.setProperty("bootstrap.servers", bootServers);
        produceConf.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        produceConf.setProperty("value.serializer", "org.example.CustomValueSerializer");

        Producer<String, CustomValue> producer = new KafkaProducer<>(produceConf);

        int currentWindow = 1;

        CustomValue cValue;

        // p-time processing 할때
        boolean pProcessing = false;
        while (true) {
            ConsumerRecords<String, CustomValue> records = consumer.poll(Duration.ofMillis(10));
            for (ConsumerRecord<String, CustomValue> record : records) {
                ProducerRecord<String, CustomValue> producerRecord;
                String msgString = String.format("key:%s, window:%d, value:%d, topic:%s",
                        record.key(), record.value().window, record.value().value, record.topic());
                System.out.println(msgString);
                switch (record.key()) {
                    case FIRST_ONTIME_COMPLETE:
                        pProcessing = true;
                        System.out.println("P-time Processing start!");
                        ProducerRecord<String, CustomValue> ptimeTrigger = new ProducerRecord<>(
                                pDataTopic, FIRST_ONTIME_COMPLETE, record.value());
                        producer.send(ptimeTrigger);
                        break;
                    case PTIME_COMPLETE:
                        pProcessing = false;
                        System.out.println("Entire Processing complete!");
                        System.out.print("Window changed: " + currentWindow + " to ");
                        currentWindow = record.value().window;
                        System.out.println(currentWindow);
                        ProducerRecord<String, CustomValue> ptimeComplete = new ProducerRecord<>(
                                pDataTopic, PTIME_COMPLETE, record.value());
                        producer.send(ptimeComplete);
                        break;
                    default:
                        cValue = record.value();

                        int recordWindow = cValue.window;
                        if (recordWindow < currentWindow) {
                            // late time
                            System.out.println("ltime data " + cValue.window + " " + cValue.value);
                            ProducerRecord<String, CustomValue> ltimeRecord = new ProducerRecord<>(
                                    ltimeTopic, DATA, cValue);
                            producer.send(ltimeRecord);
                        } else if (pProcessing) {
                            if (recordWindow == currentWindow) {
                                System.out.println("ltime data " + cValue.window + " " + cValue.value);
                                ProducerRecord<String, CustomValue> ltimeRecord = new ProducerRecord<>(
                                        ltimeTopic, DATA, cValue);
                                producer.send(ltimeRecord);
                            }
                        } else {
                            System.out.println("ptime data " + cValue.window + " " + cValue.value);
                            ProducerRecord<String, CustomValue> ptimeRecord = new ProducerRecord<>(
                                    pDataTopic, DATA, cValue);
                            producer.send(ptimeRecord);
                        }
                }
            }
            Thread.sleep(1);
        }
    }
}