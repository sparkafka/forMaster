package org.example;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.util.Collections;
import java.util.Properties;

public class StreamTest2 {
    private static String bootServers = "node1:9092,node2:9092,node3:9092,node4:9092";
    private static String consumeTopicName = "input-stream";
    public static void main(String[] args) {
        int currentWindow = 1;

        Properties conf = new Properties();
        conf.put(StreamsConfig.APPLICATION_ID_CONFIG, "streaming-test-2");
        conf.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootServers);
        conf.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        conf.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

//        Properties confConsume = new Properties();
//        confConsume.setProperty("bootstrap.servers",bootServers);
//        confConsume.setProperty("group.id", "TestGeneratorGroup");
//        confConsume.setProperty("enable.auto.commit", "false"); // offset commit
//        confConsume.setProperty("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
//        confConsume.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

//        Consumer<Integer, String> consumer = new KafkaConsumer<>(confConsume);
//        consumer.subscribe(Collections.singletonList(consumeTopicName));

        final StreamsBuilder builder = new StreamsBuilder();
        KStream<Integer, String> stream = builder.stream("input-stream", Consumed.with(
                Serdes.Integer(), Serdes.String()));


        //stream.print(Printed.toSysOut());

        stream.foreach((key, value) -> System.out.println(key + " " + value));
        KStream<Integer, String>[] streamArr = stream.branch(
                (key, value) -> key.equals(currentWindow),
                (key, value) -> true
        );
        streamArr[0].to("on-time-stream");
        //streamArr[1].
        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, conf);

        streams.start();

    }
}
