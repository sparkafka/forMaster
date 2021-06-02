package org.example;

import java.util.Properties;

import org.apache.kafka.clients.producer.*;

public class DataGenerator {
    private static String topicName = "test-gener";

    public static void main(String[] args) {
        // KafkaProducer에 필요한 설정
        Properties conf = new Properties();
        conf.setProperty("bootstrap.servers", "node1:9092,node2:9092,node3:9092");
        conf.setProperty("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        conf.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //  카프카 클러스터에 메시지를 송신(produce)하는 객체 생성
        Producer<Integer, String> producer = new KafkaProducer<>(conf);

        int key;
        String value;

        // key = 1일 때
        for (int i = 1; i <= 90; i++) {
            key = 1;
            value = String.valueOf(1);

            ProducerRecord<Integer, String> producerRecord = new ProducerRecord<>(topicName, key, value);

            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (recordMetadata != null) {
                        String infoString = String.format("On time data\nSuccess partition:%d, offset:%d",
                                recordMetadata.partition(), recordMetadata.offset());
                        System.out.println(infoString);
                    } else {
                        String infoString = String.format("Failed:%d", e.getMessage());
                        System.err.println(infoString);
                    }
                }
            });
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        // key = 2 ~ 9
        for (int i = 2; i <= 10; i++) {
            int j;
            // 처음 10개: 번갈아가며 produce
            for (j = 1; j <= 10; j++) {
                key = i;
                value = String.valueOf(i);

                ProducerRecord<Integer, String> producerRecord = new ProducerRecord<>(topicName, key, value);

                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (recordMetadata != null) {
                            String infoString = String.format("On time data\nSuccess partition:%d, offset:%d",
                                    recordMetadata.partition(), recordMetadata.offset());
                            System.out.println(infoString);
                        } else {
                            String infoString = String.format("Failed:%d", e.getMessage());
                            System.err.println(infoString);
                        }
                    }
                });
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                key = i - 1;
                value = String.valueOf(key);

                producerRecord = new ProducerRecord<>(topicName, key, value);

                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (recordMetadata != null) {
                            String infoString = String.format("Delayed data\nSuccess partition:%d, offset:%d",
                                    recordMetadata.partition(), recordMetadata.offset());
                            System.out.println(infoString);
                        } else {
                            String infoString = String.format("Failed:%d", e.getMessage());
                            System.err.println(infoString);
                        }
                    }
                });
            }
            for (;j<=90;j++){
                key = i;
                value = String.valueOf(i);

                ProducerRecord<Integer, String> producerRecord = new ProducerRecord<>(topicName, key, value);

                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (recordMetadata != null) {
                            String infoString = String.format("On time data\nSuccess partition:%d, offset:%d",
                                    recordMetadata.partition(), recordMetadata.offset());
                            System.out.println(infoString);
                        } else {
                            String infoString = String.format("Failed:%d", e.getMessage());
                            System.err.println(infoString);
                        }
                    }
                });
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        // key = 10
        for (int i = 91;i<=100;i++){
            key = 10;
            value = String.valueOf(key);
            ProducerRecord<Integer, String> producerRecord = new ProducerRecord<>(topicName, key, value);

            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (recordMetadata != null) {
                        String infoString = String.format("On time data\nSuccess partition:%d, offset:%d",
                                recordMetadata.partition(), recordMetadata.offset());
                        System.out.println(infoString);
                    } else {
                        String infoString = String.format("Failed:%d", e.getMessage());
                        System.err.println(infoString);
                    }
                }
            });
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }

        producer.close();
    }
}
