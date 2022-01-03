package org.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.consumer.*;

import java.time.Duration;
import java.util.*;

public class LTimeProcess {
    final private static String LATE_RESULT = "L";

    final private static String bootServers = "node1:9092,node2:9092,node3:9092,node4:9092";
    final private static String ltimeTopic = "ltimeTopic";
    final private static String combineResultTopic = "combineResultTopic";

    public static void main(String[] args) {
        Properties consumeConf = new Properties();
        consumeConf.setProperty("bootstrap.servers", bootServers);
        consumeConf.setProperty("group.id", "LTimeProcess");
        consumeConf.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumeConf.setProperty("value.deserializer", "org.example.CustomValueDeserializer");

        Consumer<String, CustomValue> consumer = new KafkaConsumer<>(consumeConf);
        consumer.subscribe(Collections.singletonList(ltimeTopic));

        Properties produceConf = new Properties();
        produceConf.setProperty("bootstrap.servers", bootServers);
        produceConf.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        produceConf.setProperty("value.serializer", "org.example.CustomValueSerializer");

        Producer<String, CustomValue> producer = new KafkaProducer<>(produceConf);

        for (int i = 0; i < 30000; i++) {
            Map<Integer, CustomValue> lResults = new HashMap<>();
            int lWindow;
            CustomValue lResult;
            ConsumerRecords<String, CustomValue> records = consumer.poll(Duration.ofMillis(10));
            for (ConsumerRecord<String, CustomValue> record : records) {
                String msgString = String.format("key:%s, window:%d, value:%d, topic:%s",
                        record.key(), record.value().window, record.value().value, record.topic());
                System.out.println(msgString);
                lWindow = record.value().window;
                lResult = record.value();
                if (lResults.containsKey(lWindow)) {
                    CustomValue tmp = lResults.get(lWindow);
                    tmp.value += 1;
                    tmp.endTime = System.currentTimeMillis();
                    lResults.put(tmp.window, tmp);
                } else {
                    lResults.put(lWindow, new CustomValue(lWindow, 1, lResult.eventTime, System.currentTimeMillis()));
                }
            }
            lResults.forEach((key, value) -> {
                System.out.println("window: " + key + " count sum: " + value.value);
                ProducerRecord<String, CustomValue> producerRecord = new ProducerRecord<>(combineResultTopic, LATE_RESULT, value);
                producer.send(producerRecord);
            });

            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        consumer.close();
        producer.close();
    }
}
