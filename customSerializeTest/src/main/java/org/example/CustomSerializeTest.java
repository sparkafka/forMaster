package org.example;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class CustomSerializeTest {
    final private static String bootServers = "node0:9092,node1:9092,node2:9092,node3:9092";
    final private static String topic = "dataSource";

    public static void main(String[] args) throws Exception{
        Properties prop1 = new Properties();
        prop1.setProperty("bootstrap.servers", bootServers);
        prop1.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop1.setProperty("value.serializer", "org.example.CustomValueSerializer");

        Producer<String, CustomValue> producer = new KafkaProducer<>(prop1);

        ProducerRecord<String, CustomValue> record = new ProducerRecord<>(topic, "1", new CustomValue(1,1));

//        Person person = new Person("James", 22);
//        ProducerRecord<String, Person> record = new ProducerRecord<>(topic, "1", person);

        for (int i=0;i<100;i++) {
            producer.send(record, (recordMetadata, e) -> {
                System.out.println("Success");
            });
            try {
                Thread.sleep(100);
            }catch (Exception e){
                e.printStackTrace();
            }
        }

        System.out.println("Completed.");
        producer.close();


    }
}
