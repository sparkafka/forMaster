package org.example;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;

public class KeyConsumeTest {
    private static String consumeTopicName = "test-gener";
    private static String produceTopicName = "test-check";
    private static String bootServers = "node1:9092,node2:9092,node3:9092,node4:9092";

    public static void main(String[] args) {
        // KafkaConsumer에 필요한 설정
        Properties confConsume = new Properties();
        confConsume.setProperty("bootstrap.servers",bootServers);
        confConsume.setProperty("group.id", "TestGeneratorGroup");
        confConsume.setProperty("enable.auto.commit", "false"); // offset commit
        confConsume.setProperty("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        confConsume.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        Properties confProduce = new Properties();
        confProduce.setProperty("bootstrap.servers",bootServers);
        confProduce.setProperty("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        confProduce.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Consumer<Integer, String> consumer = new KafkaConsumer<>(confConsume);
        consumer.subscribe(Collections.singletonList(consumeTopicName));

        Producer<Integer, String> producer = new KafkaProducer<>(confProduce);

        int delayKey=1;
        String delayValue;
        for(int count = 0; count < 1000; count++){
            // 메시지를 수신하여 콘솔에 표시
            ConsumerRecords<Integer, String> records = consumer.poll(1);
            for(ConsumerRecord<Integer, String> record: records) {
                String msgString = String.format("value:%s, topic:%s, partition:%d",
                        record.value(), record.topic(), record.partition());
                System.out.println(msgString);

                // delayKey와 key가 다르면 메시지 produce
                if (record.key() > delayKey){
                    delayKey = record.key();
                    delayValue = String.valueOf(record.partition());
                    ProducerRecord<Integer, String> producerRecord = new ProducerRecord<>(produceTopicName,
                            delayKey, delayValue);
                    producer.send(producerRecord);
                }
                // 처리가 완료된 메시지의 오프셋을 커밋
                TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                OffsetAndMetadata oam = new OffsetAndMetadata(record.offset() + 1);
                Map<TopicPartition, OffsetAndMetadata> commitInfo = Collections.singletonMap(tp, oam);
                consumer.commitSync(commitInfo);
            }
            try {
                Thread.sleep(1000); // 1초 지연
            } catch (InterruptedException ex) {
                ex.printStackTrace();
            }
        }

        consumer.close();
        producer.close();
    }
}
