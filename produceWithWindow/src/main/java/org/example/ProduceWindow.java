package org.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

public class ProduceWindow {
    private static String produceTopicName = "dataSource";
    private static String consumeTopicName = "current-window";
    private static String bootServers = "node0:9092,node1:9092,node2:9092,node3:9092";

    public static void main(String[] args) {
        int currentWindow = 1;
        Properties produceConf = new Properties();
        Random rand = new Random();

        produceConf.setProperty("bootstrap.servers", bootServers);
        produceConf.setProperty("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        produceConf.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // key: current window, value: current value
        Producer<Integer,String> producer = new KafkaProducer<>(produceConf);
        
        Properties consumeConf = new Properties();
        consumeConf.setProperty("bootstrap.servers",bootServers);
        consumeConf.setProperty("group.id", "WindowConsumeGroup");
        consumeConf.setProperty("enable.auto.commit", "false"); // offset commit
        consumeConf.setProperty("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        consumeConf.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        Consumer<Integer, String> consumer = new KafkaConsumer<Integer, String>(consumeConf);
        consumer.subscribe(Collections.singletonList(consumeTopicName));

        // consume 요청 했는데 데이터를 가져오지 못했으면 빈 컬렉션 반환
        ConsumerRecords<Integer, String> records;


        for (int i=1;i<=100;i++) {
            /*records = consumer.poll(10);
            if (records != null){
                for(ConsumerRecord<Integer, String> record : records){
                    String windowValue = record.value();
                    currentWindow = Integer.valueOf(windowValue);
                    System.out.println("Current Window: " + currentWindow);
                    // 처리가 완료된 메시지의 오프셋을 커밋
                    TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                    OffsetAndMetadata oam = new OffsetAndMetadata(record.offset() + 1);
                    Map<TopicPartition, OffsetAndMetadata> commitInfo = Collections.singletonMap(tp, oam);
                    consumer.commitSync(commitInfo);
                }
            }*/
            String value = String.valueOf(i);
            ProducerRecord<Integer, String> producerRecord = new ProducerRecord<>(produceTopicName, currentWindow, value);
            producer.send(producerRecord, (recordMetadata, e) -> {
                if (recordMetadata != null) {
                    String infoString = String.format("Success partition:%d, offset:%d",
                            recordMetadata.partition(), recordMetadata.offset());
                    System.out.println(infoString);
                } else {
                    String infoString = String.format("Failed:%s", e.getMessage());
                    System.err.println(infoString);
                }
            });
            try{
                Thread.sleep(10);
            } catch (InterruptedException e){
                e.printStackTrace();
            }
        }

        consumer.close();
        producer.close();
        
    }
}
