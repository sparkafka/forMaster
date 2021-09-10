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

public class PtimeSplitProcess {
    final private static String FIRST_ONTIME_COMPLETE = "C";
    final private static String PTIME_COMPLETE = "P";

    final private static String bootServers = "node1:9092,node2:9092,node3:9092,node4:9092";
    final private static String pTopic = "ptimeTopic";
    final private static String tmTopic = "tmTopic";
    final private static String ptimeTriggerTopic = "ptimeTriggerTopic";
    final private static String onResultTopic = "ontimeResultTopic";
    final private static String ltimeTopic = "ltimeTopic";

    static ArrayList<Pair> pRecords = new ArrayList<>();

    public static int count(ArrayList<Pair> records) {
        return records.size();
    }

    public static void main(String[] args) {
        Properties consumeConf = new Properties();
        consumeConf.setProperty("bootstrap.servers", bootServers);
        consumeConf.setProperty("group.id", "PtimeSplitProcess");
        consumeConf.setProperty("max.poll.records", "1");
        consumeConf.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumeConf.setProperty("value.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        Consumer<String, Integer> consumer = new KafkaConsumer<>(consumeConf);
        consumer.subscribe(Arrays.asList(pTopic, ptimeTriggerTopic));


        Properties produceConf = new Properties();
        produceConf.setProperty("bootstrap.servers", bootServers);
        produceConf.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        produceConf.setProperty("value.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");

        Producer<String, Integer> producer = new KafkaProducer<>(produceConf);

        int currentWindow = 1;

        String key;
        int value;

        boolean pProcessing = false;
        for (int i = 0; i < 30000; i++) {
            ConsumerRecords<String, Integer> records = consumer.poll(Duration.ofMillis(10));
            for (ConsumerRecord<String, Integer> record : records) {
                ProducerRecord<String, Integer> producerRecord;
                String msgString = String.format("key:%s, value:%d, topic:%s, partition:%d, offset:%d",
                        record.key(), record.value(), record.topic(), record.partition(), record.offset());
                System.out.println(msgString);
                switch (record.key()) {
                    case FIRST_ONTIME_COMPLETE:
                        pProcessing = true;
                        currentWindow = record.value();
                        if (!pRecords.isEmpty()) {
                            System.out.println("Ptime Triggering");
                            String resultWindow = pRecords.get(0).getKey();
                            int resultCount = count(pRecords);
                            pRecords.clear();
                            ProducerRecord<String, Integer> ontimeResultRecord = new ProducerRecord<>(
                                onResultTopic, resultWindow, resultCount);
                            producer.send(ontimeResultRecord);
                            producerRecord = new ProducerRecord<>(tmTopic, 0, PTIME_COMPLETE, Integer.valueOf(resultWindow));
                            producer.send(producerRecord);
                            System.out.println("window: " + resultWindow + " count: " + resultCount);
                        } else {
                            producerRecord = new ProducerRecord<>(tmTopic, 0, PTIME_COMPLETE, 0);
                            producer.send(producerRecord);
                            System.out.println("no ptime record");
                        }
                        break;
                    case PTIME_COMPLETE:
                        pProcessing = false;
                        currentWindow = record.value();
                        System.out.println("current window change: " + currentWindow);
                        break;
                    default:
                        key = record.key();
                        value = record.value();

                        int recordWindow = Integer.valueOf(key);
                        if (recordWindow < currentWindow) {
                            // late time
                            System.out.println("ltime " + key + " " + value);
                            ProducerRecord<String, Integer> ltimeRecord = new ProducerRecord<>(
                                    ltimeTopic, key, value);
                            producer.send(ltimeRecord);
                        } else if (pProcessing){
                            if (recordWindow == currentWindow){
                                System.out.println("ltime " + key + " " + value);
                                ProducerRecord<String, Integer> ltimeRecord = new ProducerRecord<>(
                                        ltimeTopic, key, value);
                                producer.send(ltimeRecord);
                            }
                        } else {
                            System.out.println("ptime " + key + " " + value);
                            pRecords.add(new Pair(key, value));
                        }
                }
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