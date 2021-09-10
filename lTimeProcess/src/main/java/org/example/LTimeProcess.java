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
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class LTimeProcess {
    final private static String bootServers = "node1:9092,node2:9092,node3:9092,node4:9092";
    final private static String ltimeTopic = "ltimeTopic";
    final private static String lResultTopic = "lResultTopic";

    public static void main(String[] args) {
        Properties consumeConf = new Properties();
        consumeConf.setProperty("bootstrap.servers", bootServers);
        consumeConf.setProperty("group.id", "LTimeProcess");
        consumeConf.setProperty("max.poll.records", "1");
        consumeConf.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumeConf.setProperty("value.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");

        Consumer<String, Integer> consumer = new KafkaConsumer<>(consumeConf);
        consumer.subscribe(Collections.singletonList(ltimeTopic));

        Properties produceConf = new Properties();
        produceConf.setProperty("bootstrap.servers", bootServers);
        produceConf.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        produceConf.setProperty("value.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");

        Producer<String, Integer> producer = new KafkaProducer<>(produceConf);

        for (int i = 0; i < 30000; i++) {
            Map<String, Integer> lResults = new HashMap<>();
            ConsumerRecords<String, Integer> records = consumer.poll(Duration.ofMillis(10));
            for (ConsumerRecord<String, Integer> record : records) {
                String msgString = String.format("key:%s, value:%d, topic:%s, partition:%d, offset:%d",
                        record.key(), record.value(), record.topic(), record.partition(), record.offset());
                System.out.println(msgString);
                if (lResults.containsKey(record.key())) {
                    lResults.put(record.key(), lResults.get(record.key()) + record.value());
                } else {
                    lResults.put(record.key(), record.value());
                }
            }
            lResults.forEach((key, value) -> {
                System.out.println("window: "+key+"sum: "+value);
//                ProducerRecord<String, Integer> producerRecord = new ProducerRecord<>(lResultTopic, key, value);
//                producer.send(producerRecord);
                lResults.remove(key);
            });

            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        consumer.close();
        producer.close();
    }
}
