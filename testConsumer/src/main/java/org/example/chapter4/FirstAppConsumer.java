package org.example.chapter4;

import java.time.Duration;
import java.util.*;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

public class FirstAppConsumer {
    private static String topicName = "dataSource";
    private static String ontimeTriggerTopic = "ontimeTriggerTopic";

    public static void main(String[] args) {

        // KafkaConsumer에 필요한 설정
        Properties conf = new Properties();
        conf.setProperty("bootstrap.servers", "node1:9092,node2:9092,node3:9092,node4:9092");
        conf.setProperty("group.id", "FirstAppConsumerGroup");
        //conf.setProperty("enable.auto.commit", "false");
        conf.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        conf.setProperty("value.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");

        // 카프카 클러스터에서 메시지를 수신(Consume)하는 객체 생성
        Consumer<String, Integer> consumer = new KafkaConsumer<>(conf);

        // 구독(subscribe)하는 Topic 등록
        consumer.subscribe(Collections.singletonList(topicName));

        for (int count = 0; count < 30000; count++) {
            // 메시지를 수신하여 콘솔에 표시
            ConsumerRecords<String, Integer> records = consumer.poll(10);
            for (ConsumerRecord<String, Integer> record : records) {
                String msgString = String.format("key:%s, value:%d, partition:%d offset:%d",
                        record.key(), record.value(), record.partition(), record.offset());
                System.out.println(msgString);
            }

                /*// 처리가 완료된 메시지의 오프셋을 커밋
                TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                OffsetAndMetadata oam = new OffsetAndMetadata(record.offset() + 1);
                Map<TopicPartition, OffsetAndMetadata> commitInfo = Collections.singletonMap(tp, oam);
                consumer.commitSync(commitInfo);*/
            try {
                Thread.sleep(10); // 1초 지연
            } catch (InterruptedException ex) {
                ex.printStackTrace();
            }
        }

        // KafkaConsumer를 닫고 종료
        consumer.close();

    }
}
