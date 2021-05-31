package org.example.chapter4;

import org.apache.kafka.clients.producer.*;
import java.util.Properties;

public class FirstAppProducer {
    private static String topicName = "first-app";

    public static void main(String[] args) {

        // KafkaProducer에 필요한 설정
        Properties conf = new Properties();
        conf.setProperty("bootstrap.servers", "node1:9092,node2:9092,node3:9092");
        conf.setProperty("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        conf.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        //  카프카 클러스터에 메시지를 송신(produce)하는 객체 생성
        Producer<Integer, String> producer = new KafkaProducer<>(conf);

        int key;
        String value;
        for(int i = 1; i <= 100; i++){
            // key, value 생성
            key = i;
            value = String.valueOf(i);

            // 송신 메시지 생성
            ProducerRecord<Integer, String> record = new ProducerRecord<>(topicName, key, value);

            // 메시지를 송신하고 Ack를 받았을 때 실행할 작업(Callback) 등록
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(recordMetadata != null) {
                        // 송신에 성공한 경우
                        String infoString = String.format("Success partition:%d, offset:%d",
                                recordMetadata.partition(), recordMetadata.offset());
                        System.out.println(infoString);
                    }
                    else {
                        // 송신에 실패한 경우
                        String infoString = String.format("Failed:%s", e.getMessage());
                        System.err.println(infoString);
                    }
                }
            });
        }

        // KafkaProducer를 닫고 종료
        producer.close();
    }
}
