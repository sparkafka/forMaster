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

    public static void main(String[] args) {
        // KafkaConsumer에 필요한 설정
        Properties conf = new Properties();
        conf.setProperty("bootstrap.servers","node1:9092,node2:9092,node3:9092");
        conf.setProperty("group.id", "TestGeneratorGroup");
        conf.setProperty("enable.auto.commit", "false"); // offset commit
        conf.setProperty("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        conf.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        Consumer<Integer, String> consumer = new KafkaConsumer<>(conf);
        consumer.subscribe(Collections.singletonList(consumeTopicName));

        for(int count = 0; count < 1000; count++){
            // 메시지를 수신하여 콘솔에 표시
            ConsumerRecords<Integer, String> records = consumer.poll(1);
            for(ConsumerRecord<Integer, String> record: records) {
                String msgString = String.format("key:%d, value:%s, topic:%s, partition:%s",
                        record.key(), record.value(), record.topic(), record.partition());
                System.out.println(msgString);

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
    }
}
