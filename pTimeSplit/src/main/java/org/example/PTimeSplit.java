package org.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.consumer.*;

import java.util.Collections;
import java.util.Properties;

public class PTimeSplit {
    private static String pTopic = "one-p";
    private static String onTopic = "one-on";
    private static String lateDataTopic = "one-late";
    private static String pSplitTopic = "one-psplit";
    private static String trigTopic = "one-trig";
    private static String bootServers = "node1:9092,node2:9092,node3:9092,node4:9092";
    public static void main(String[] args) {
        Properties consumeConf = new Properties();
        consumeConf.setProperty("bootstrap.servers", bootServers);
        consumeConf.setProperty("group.id", "OneFirstSplitter");
        // conf.setProperty("enable.auto.commit", "false");
        consumeConf.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumeConf.setProperty("value.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");

        Consumer<String, Integer> consumer = new KafkaConsumer<>(consumeConf);

        Properties proConf = new Properties();
        proConf.setProperty("bootstrap.servers", bootServers);
        proConf.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        proConf.setProperty("value.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");

        Producer<String, Integer> producer = new KafkaProducer<>(proConf);

        boolean trigSwitch = false;

        int currentWindow = 1;
        String key;
        int value;
        try{
            while(true){
                if (!trigSwitch){
                    consumer.subscribe(Collections.singletonList(pSplitTopic));
                    ConsumerRecords<String, Integer> records = consumer.poll(10);
                    for (ConsumerRecord<String, Integer> record : records) {
                        String msgString = String.format("key:%s, value:%d, topic:%s, partition:%d, offset:%d",
                                record.key(), record.value(), record.topic(), record.partition(), record.offset());
                        System.out.println(msgString);
                        if (record.key()=="new window") {
                            currentWindow = Integer.valueOf(record.value());
                        } else if (record.key() == "p-time split"){
                            trigSwitch = true;
                        }
                    }
                } else {
                    int recordWindow;
                    consumer.subscribe(Collections.singletonList(pTopic));
                    ConsumerRecords<String, Integer> records = consumer.poll(10);
                    for (ConsumerRecord<String, Integer> record : records) {
                        String msgString = String.format("key:%s, value:%d, topic:%s, partition:%d, offset:%d",
                                record.key(), record.value(), record.topic(), record.partition(), record.offset());
                        System.out.println(msgString);
                        key = record.key();
                        value = record.value();
                        recordWindow = Integer.valueOf(key);
                        if (recordWindow == currentWindow){
                            ProducerRecord<String, Integer> data = new ProducerRecord<>(onTopic, key, value);
                            producer.send(data, (recordMetadata, e) -> {
                                if (recordMetadata != null) {
                                    String infoString = String.format("Success offset:%s, partition:%d",
                                            recordMetadata.offset(), recordMetadata.partition());
                                    System.out.println(infoString);
                                } else {
                                    String infoString = String.format("Failed:%d", e.getMessage());
                                    System.err.println(infoString);
                                }
                            });
                        }
                    }
                }
            }
        } finally {
            consumer.close();
            producer.close();
        }

    }
}
