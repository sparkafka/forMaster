package org.example;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class MultiTopicTest {
    private static String bootServers = "node0:9092,node1:9092,node2:9092,node3:9092";
    private static String topic1 = "test1";
    private static String topic2 = "test2";
    public static void main(String[] args) {
        Properties consumeConf = new Properties();
        consumeConf.setProperty("bootstrap.servers", bootServers);
        consumeConf.setProperty("group.id", "OneProcess");
        //consumeConf.setProperty("enable.auto.commit", "false");
        consumeConf.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumeConf.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        Consumer<String, String> consumer = new KafkaConsumer<>(consumeConf);


        boolean fromswitch = true;

        for(int i=0;i<300;i++) {
            if (fromswitch) {
                //consumer.subscribe(Arrays.asList(topic1));
                consumer.subscribe(Collections.singletonList(topic1));
                // System.out.println("111");
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1));
                for (ConsumerRecord<String, String> record : records) {
                    String msgString = String.format("key:%s, value:%s, topic:%s, partition:%d, offset:%d",
                            record.key(), record.value(), record.topic(), record.partition(), record.offset());
                    System.out.println(msgString);
                    if (record.value().equals("1")){
                        consumer.subscribe(Collections.singletonList(topic2));
                    }

                    ConsumerRecords<String, String> msgs = consumer.poll(Duration.ofMillis(10));
                    while(msgs.isEmpty()){
                        msgs=consumer.poll(Duration.ofMillis(10));
                    }
                    for (ConsumerRecord<String, String> msg : msgs) {
                        System.out.println(msg.value());
                    }
                    /*TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                    OffsetAndMetadata oam = new OffsetAndMetadata(record.offset() + 1);
                    Map<TopicPartition, OffsetAndMetadata> commitInfo = Collections.singletonMap(tp, oam);
                    consumer.commitSync(commitInfo);
                    fromswitch=false;*/
                }
            }
            else{
                consumer.subscribe(Collections.singletonList(topic2));
                System.out.println("222");
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1));
                for (ConsumerRecord<String, String> record : records) {
                    String msgString = String.format("value:%s, topic:%s, partition:%d, offset:%d",
                            record.value(), record.topic(), record.partition(), record.offset());
                    System.out.println(msgString);
                    /*TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                    OffsetAndMetadata oam = new OffsetAndMetadata(record.offset() + 1);
                    Map<TopicPartition, OffsetAndMetadata> commitInfo = Collections.singletonMap(tp, oam);
                    consumer.commitSync(commitInfo);
                    fromswitch=true;*/
                }

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
