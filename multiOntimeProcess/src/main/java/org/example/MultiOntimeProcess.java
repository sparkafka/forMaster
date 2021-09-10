package org.example;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;

import java.lang.reflect.Array;
import java.time.Duration;
import java.util.*;


class Pair {
    private String key;
    private int value;

    Pair(String inputKey, int inputValue) {
        this.key = inputKey;
        this.value = inputValue;
    }

    public String getKey() {
        return this.key;
    }

    public int getValue() {
        return this.value;
    }
}

public class MultiOntimeProcess {
    private static String FIRST_ONTIME_TM = "0";

    private static String bootServers = "node1:9092,node2:9092,node3:9092,node4:9092";
    private static String ontimeTriggerTopic = "ontimeTriggerTopic";
    private static String onTopic1 = "ontimeTopic1";
    private static String onTopic2 = "ontimeTopic2";
    private static String resultTopic = "ontimeResultTopic";

    public static void main(String[] args) {
        Properties consumeConf = new Properties();
        consumeConf.setProperty("bootstrap.servers", bootServers);
        consumeConf.setProperty("group.id", "MultiOntimeProcess");
        //consumeConf.setProperty("auto.offset.reset", "latest");
        //consumeConf.setProperty("enable.auto.commit", "false");
        consumeConf.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumeConf.setProperty("value.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");

        Consumer<String, Integer> consumer = new KafkaConsumer<>(consumeConf);

        Properties produceConf = new Properties();
        produceConf.setProperty("bootstrap.servers", bootServers);
        produceConf.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        produceConf.setProperty("value.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");

        Producer<String, Integer> producer = new KafkaProducer<>(produceConf);

        // consumer assign
        consumer.subscribe(Arrays.asList(onTopic1, onTopic2, ontimeTriggerTopic));

        // Store messages in memory
        ArrayList<Pair> topic1Records = new ArrayList<>();
        ArrayList<Pair> topic2Records = new ArrayList<>();

        boolean onTopicSelect = true;
        boolean trigSwitch = false;

        for (int i = 0; i < 30000; i++) {
            ConsumerRecords<String, Integer> records = consumer.poll(Duration.ofMillis(10));
            for (ConsumerRecord<String, Integer> record : records) {
                String msgString = String.format("key:%s, value:%d, topic:%s, partition:%d, offset:%d",
                        record.key(), record.value(), record.topic(), record.partition(), record.offset());
                System.out.println(msgString);
                if (record.topic().equals(ontimeTriggerTopic)) {
                    System.out.println("First on-time processing");
                    String recordWindow;
                    int recordCount;
                    if (onTopicSelect && !topic1Records.isEmpty()) {
                        recordWindow = topic1Records.get(0).getKey();
                        recordCount = topic1Records.size();
                        topic1Records.clear();
                        System.out.println(recordWindow + ": " + recordCount);
//                        ProducerRecord<String, Integer> resultRecord = new ProducerRecord<>(resultTopic, recordWindow, recordCount);
//                        producer.send(resultRecord);
                        onTopicSelect = !onTopicSelect;
                    } else if (!onTopicSelect && !topic2Records.isEmpty()) {
                        recordWindow = topic2Records.get(0).getKey();
                        recordCount = topic2Records.size();
                        topic2Records.clear();
                        System.out.println(recordWindow + ": " + recordCount);
//                        ProducerRecord<String, Integer> resultRecord = new ProducerRecord<>(resultTopic, recordWindow, recordCount);
//                        producer.send(resultRecord);
                        onTopicSelect = !onTopicSelect;
                    }
                } else {
                    if (record.topic().equals(onTopic1)) {
                        topic1Records.add(new Pair(record.key(), record.value()));
                    } else {
                        topic2Records.add(new Pair(record.key(), record.value()));
                    }
                }
            }

            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}