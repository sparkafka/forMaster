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
import java.util.Collections;
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

public class OnTimeResult {
    final private static int NODES = 4;
    final private static String PTIME_COMPLETE = "P";

    private static final String bootServers = "node1:9092,node2:9092,node3:9092,node4:9092";
    final private static String onResultTopic = "ontimeResultTopic";
    final private static String onResultTrigger = "ontimeResultTrigger";
    final private static String combineResultTopic = "combineResultTopic";

    // 나중에 클래스 배열 만들어서 보관하자.
    public static void main(String[] args) {
        Properties consumeConf = new Properties();
        consumeConf.setProperty("bootstrap.servers", bootServers);
        consumeConf.setProperty("group.id", "OnTimeResult");
        consumeConf.setProperty("max.poll.records", "1");
        consumeConf.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumeConf.setProperty("value.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");

        Consumer<String, Integer> consumer = new KafkaConsumer<>(consumeConf);
        consumer.subscribe(Arrays.asList(onResultTopic,onResultTrigger));

        Properties produceConf = new Properties();
        produceConf.setProperty("bootstrap.servers", bootServers);
        produceConf.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        produceConf.setProperty("value.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");

        Producer<String, Integer> producer = new KafkaProducer<>(produceConf);

        String window = "";
        int sum = 0;
        for (int i = 0; i < 30000; i++) {
            ConsumerRecords<String, Integer> records = consumer.poll(Duration.ofMillis(10));
            for (ConsumerRecord<String, Integer> record : records) {
                String msgString = String.format("key:%s, value:%d, topic:%s, partition:%d, offset:%d",
                        record.key(), record.value(), record.topic(), record.partition(), record.offset());
                System.out.println(msgString);
                switch (record.key()){
                    case PTIME_COMPLETE:
                        System.out.println("key: "+window+", sum: "+sum);
                        ProducerRecord<String, Integer> producerRecord =
                                new ProducerRecord<>(combineResultTopic, window, sum);
                        producer.send(producerRecord);
                        window = "";
                        sum = 0;
                        break;
                    default:
                        window = record.key();
                        sum += record.value();
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
