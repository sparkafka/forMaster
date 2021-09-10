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
    static int currentWindow = 1;

    public static void main(String[] args) {
        Properties conf = new Properties();
        conf.put(StreamsConfig.APPLICATION_ID_CONFIG, "streaming-test-2");
        conf.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootServers);
        conf.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        conf.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final StreamsBuilder builder = new StreamsBuilder();
        KStream<Integer, String> stream = builder.stream("input-stream");


        //stream.print(Printed.toSysOut());

        stream.foreach((key, value) ->
        {
            if (currentWindow == 1) {
                System.out.println(key + " " + value);
                currentWindow = 2;
                System.out.println(currentWindow);
            }
        });
        stream.to("output-stream");

        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, conf);

        streams.start();

    }
}
