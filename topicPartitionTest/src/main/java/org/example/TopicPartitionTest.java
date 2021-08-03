package org.example;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class TopicPartitionTest {
    private static String bootServers = "node1:9092,node2:9092,node3:9092,node4:9092";
    private static String testTopic = "topicPartitionTest";
    public static void main(String[] args) {
        Properties consumeConf = new Properties();
        consumeConf.setProperty("bootstrap.servers", bootServers);
        consumeConf.setProperty("group.id", "OneFirstSplitter");
        // conf.setProperty("enable.auto.commit", "false");
        consumeConf.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumeConf.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        Consumer<String, String> consumer = new KafkaConsumer<>(consumeConf);
        consumer.assign(Collections.singletonList(new TopicPartition(testTopic, 3)));

        for(int i=0;i<100;i++){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10));
            for(ConsumerRecord<String, String> record:records){
                String msgString = String.format("key:%s, value:%s, topic:%s, partition:%d, offset:%d",
                        record.key(), record.value(), record.topic(), record.partition(), record.offset());
                System.out.println(msgString);
            }
            try {
                Thread.sleep(1000); // 1초 지연
            } catch (InterruptedException ex) {
                ex.printStackTrace();
            }
        }
        consumer.close();
    }
}
