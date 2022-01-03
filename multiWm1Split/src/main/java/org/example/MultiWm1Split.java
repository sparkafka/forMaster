package org.example;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class MultiWm1Split {
//    final private static int nodeNum = 1;
//    final private static int nodeNum = 2;
//    final private static int nodeNum = 3;
    final private static int nodeNum = 4;

    final private static String DATA = "D";
    final private static String WM0 = "W0";
    final private static String WM1 = "W1";
    final private static String WM0_COMPLETE = "C0";
    final private static String WM1_COMPLETE = "C1";

    final private static String bootServers = "node1:9092,node2:9092,node3:9092,node4:9092";
    final private static String dataSource = "dataSource";
    final private static String ontimeTriggerTopic = "ontimeTriggerTopic";

    final private static String onTopic1 = "ontimeTopic1";

    public static Consumer<String, CustomValue> getConsumer() {
        Consumer<String, CustomValue> consumer;

        Properties consumeConf = new Properties();
        consumeConf.setProperty("bootstrap.servers", bootServers);
        consumeConf.setProperty("group.id", "MultiWmSplit");
        consumeConf.setProperty("max.poll.records", "1");
        consumeConf.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumeConf.setProperty("value.deserializer", "org.example.CustomValueDeserializer");
        consumer = new KafkaConsumer<>(consumeConf);
//        consumer.subscribe(Arrays.asList(dataSource, ontimeTriggerTopic));
        List<TopicPartition> partitions = Arrays.asList(
                new TopicPartition(dataSource, nodeNum - 1),
                new TopicPartition(ontimeTriggerTopic, nodeNum - 1));
        consumer.assign(partitions);
        return consumer;
    }

    public static Producer<String, CustomValue> getProducer() {
        Properties produceConf = new Properties();
        produceConf.setProperty("bootstrap.servers", bootServers);
        produceConf.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        produceConf.setProperty("value.serializer", "org.example.CustomValueSerializer");

        return new KafkaProducer<>(produceConf);
    }

    public static void main(String[] args) throws InterruptedException {
        Consumer<String, CustomValue> consumer = getConsumer();

        Producer<String, CustomValue> producer = getProducer();

        int currentWindow = 1;
        String key;
        CustomValue cValue;
        while (true) {
            int recordWindow;
            ConsumerRecords<String, CustomValue> records = consumer.poll(Duration.ofMillis(10));
            for (ConsumerRecord<String, CustomValue> record : records) {
                ProducerRecord<String, CustomValue> producerRecord;
                String msgString = String.format("key:%s, window:%d, value:%d, topic:%s, partition:%d",
                        record.key(), record.value().window, record.value().value, record.topic(), record.partition());
                System.out.println(msgString);
                // WM0
                if(record.key().equals(WM0)) {
                    System.out.println("WM0 Processing");
                    ProducerRecord<String, CustomValue> wmTrigger;
                    wmTrigger = new ProducerRecord<>(onTopic1, nodeNum - 1, record.key(), record.value());
                    producer.send(wmTrigger);
                } else if(record.key().equals(WM1)){
                    System.out.println("WM1 Processing");
                    /*System.out.print("Window changed: " + currentWindow + " to ");
                    currentWindow = record.value().window;
                    System.out.println(currentWindow);*/
                    ProducerRecord<String, CustomValue> wmTrigger;
                    wmTrigger = new ProducerRecord<>(onTopic1, nodeNum - 1, record.key(), record.value());
                    producer.send(wmTrigger);
                } else if(record.key().equals(WM0_COMPLETE)){
                    System.out.println("WM0 complete!");
                    ProducerRecord<String, CustomValue> wmComplete = new ProducerRecord<>(
                            onTopic1, nodeNum - 1, record.key(), record.value());
                    producer.send(wmComplete);
                } else if(record.key().equals(WM1_COMPLETE)){
                    System.out.println("WM1 complete!");
                    System.out.print("Window changed: " + currentWindow + " to ");
                    currentWindow = record.value().window;
                    System.out.println(currentWindow);
                    ProducerRecord<String, CustomValue> wmComplete = new ProducerRecord<>(
                            onTopic1, nodeNum - 1, record.key(), record.value());
                    producer.send(wmComplete);
                } else if (record.key().equals(DATA)) {
                    System.out.println("Send data");
                    key = record.key();
                    cValue = record.value();
                    cValue.setEventTime(System.currentTimeMillis());
                    System.out.println("Event Time: "+cValue.eventTime);
                    recordWindow = cValue.window;
                    if (recordWindow < currentWindow) {
                        System.out.println("Late data");
                    } else {
                        System.out.println("On time data");
                        ProducerRecord<String, CustomValue> data = new ProducerRecord<>(
                                onTopic1, nodeNum - 1, DATA, record.value());
                        producer.send(data);
                    }
                }
            }
            Thread.sleep(1);
        }
    }
}
