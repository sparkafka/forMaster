package org.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.util.Properties;

public class StreamTest1 {
    public static void main(String[] args) throws Exception{
        Properties conf = new Properties();
        // StreamsConfig.APPLICATION_ID_CONFIG : 카프카 클러스터 내의 스트림즈 애플리케이션을 구분할 때 사용하는 값으로 유일해야합니다.
        // application id가 consumer의 group.id 역할
        /*StreamsConfig.BOOTSTRAP_SERVERS_CONFIG : 스트림 애플리케이션이 접근할 브로커의 ip와 port를 입력해줍니다.
                StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG : 토픽에서 다룰 데이터의 'key' 형식을 지정해줍니다.
        StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG : 토픽에서 다룰 데이터의 'value' 형식을 지정해줍니다.*/
        conf.put(StreamsConfig.APPLICATION_ID_CONFIG, "streaming-test-1");
        conf.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "node1:9092,node2:9092,node3:9092,node4:9092");
        conf.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        conf.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // topology 생성을 위한 StreamsBuilder
        // input-stream -> output-stream
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream("input-stream").to("output-stream");

        // topology 생성
        final Topology topology = builder.build();
        System.out.println(topology.describe());

        final KafkaStreams streams = new KafkaStreams(topology, conf);

        streams.start();
        // streams.close();

    }
}
