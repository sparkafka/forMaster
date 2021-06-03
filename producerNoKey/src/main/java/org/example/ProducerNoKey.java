package org.example;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.Random;

public class ProducerNoKey {
    private static String topicName = "test-gener";

    public static void main(String[] args) {
        Properties conf = new Properties();
        Random rand = new Random();

        conf.setProperty("bootstrap.servers", "node1:9092,node2:9092,node3:9092");
        conf.setProperty("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        conf.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<Integer,String> producer = new KafkaProducer<>(conf);

        int key;
        String value;
        int partition;

        // key = 1
        for (int i = 1; i <= 90; i++) {
            key = 1;
            value = String.valueOf(1);

            // 파티션 랜덤 선택
            partition = rand.nextInt(4);
            ProducerRecord<Integer, String> record = new ProducerRecord<>(topicName,partition,key,value);

            producer.send(record, new Callback() {
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
                partition = rand.nextInt(4);
                ProducerRecord<Integer, String> record = new ProducerRecord<>(topicName,partition,key,value);

                producer.send(record, new Callback() {
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

                partition = rand.nextInt(4);
                record = new ProducerRecord<>(topicName,partition,key,value);

                producer.send(record, new Callback() {
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
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            // 나머지 출력
            for (; j <= 90; j++) {
                key = i;
                value = String.valueOf(i);

                partition = rand.nextInt(4);
                ProducerRecord<Integer, String> record = new ProducerRecord<>(topicName,partition,key,value);

                producer.send(record, new Callback() {
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
            partition = rand.nextInt(4);
            ProducerRecord<Integer, String> record = new ProducerRecord<>(topicName,partition,key,value);

            producer.send(record, new Callback() {
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
        /*for (int i=1;i<=5;i++) {
            for (int j = 1; j <= 5; j++) {
                key = j;
                value = String.valueOf(key);

                // 파티션 랜덤 선택
                partition = rand.nextInt(4);
                ProducerRecord<Integer, String> record = new ProducerRecord<>(topicName,partition,key,value);

                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (recordMetadata != null) {
                            String infoString = String.format("Success partition:%d, offset:%d",
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
        }*/
        producer.close();
    }
}
