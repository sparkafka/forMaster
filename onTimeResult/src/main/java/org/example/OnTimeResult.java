package org.example;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class OnTimeResult {
    final private static int NODES = 4;

    final private static String DATA = "D";
    final private static String PTIME_COMPLETE = "P";
    final private static String ONTIME_RESULT = "R";
    final private static String PTIME_RESULT = "E";

    private static final String bootServers = "node1:9092,node2:9092,node3:9092,node4:9092";
    final private static String onResultTopic = "ontimeResultTopic";
    final private static String onResultTrigger = "ontimeResultTrigger";
    final private static String combineResultTopic = "combineResultTopic";

    final private static String url = "jdbc:mysql://localhost:3306/mydb?serverTimezone=UTC&useSSL=false";
    final private static String userName = "root";
    final private static String password = "root";

    // 나중에 클래스 배열 만들어서 보관하자.
    public static void main(String[] args) throws SQLException, InterruptedException {

        Properties consumeConf = new Properties();
        consumeConf.setProperty("bootstrap.servers", bootServers);
        consumeConf.setProperty("group.id", "OnTimeResult");
        consumeConf.setProperty("max.poll.records", "1");
        consumeConf.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumeConf.setProperty("value.deserializer", "org.example.CustomValueDeserializer");

        Consumer<String, CustomValue> consumer = new KafkaConsumer<>(consumeConf);
        consumer.subscribe(Arrays.asList(onResultTopic, onResultTrigger));

        Properties produceConf = new Properties();
        produceConf.setProperty("bootstrap.servers", bootServers);
        produceConf.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        produceConf.setProperty("value.serializer", "org.example.CustomValueSerializer");

        Producer<String, CustomValue> producer = new KafkaProducer<>(produceConf);

        Connection con = DriverManager.getConnection(url, userName, password);
        // Multi0: 원래 모델: 모든 노드에서 Tm이 들어왔을때
        // Multi1: 하나 들어오면 바로 Triggering
        // Multi2: 노드 수(4)만큼 들어오면 Triggering
        // Multi3: 절반의 노드(2)에 들어오면 Triggering
        String SQL = "insert into OnResultNoDelay33(windowNum, value, processTime)" +
                " values(?, ?, ?)";
        PreparedStatement pstmt = con.prepareStatement(SQL);

        CustomValue onTimeResultData = null;

        int resultWindow = 0;
        int resultCount = 0;
        int resultOntime = 0;
        int resultPtime = 0;
        long resultEventTime = 0L;

        while (true) {
            ConsumerRecords<String, CustomValue> records = consumer.poll(Duration.ofMillis(10));
            for (ConsumerRecord<String, CustomValue> record : records) {
                String msgString = String.format("key:%s, window:%d, value:%d, time:%d",
                        record.key(), record.value().window, record.value().value, record.value().eventTime);
                System.out.println(msgString);
                if (record.key().equals(ONTIME_RESULT)) {
                    if (resultEventTime == 0L) {
                        resultWindow = record.value().window;
                        resultOntime += record.value().value;
                        resultEventTime = record.value().eventTime;
                    } else {
                        // result Window 다른 경우 생각 X
                        resultOntime += record.value().value;
                        if (resultEventTime > record.value().eventTime) resultEventTime = record.value().eventTime;
                    }
                } else if (record.key().equals(PTIME_RESULT)) {
                    resultPtime+=record.value().value;
                    if (record.value().eventTime != 0L && resultEventTime>record.value().eventTime) {
                        System.out.println("p time record");
                        System.out.println(record.value().window + " " + record.value().value + " " + record.value().eventTime);
                        resultEventTime = record.value().eventTime;
                    }
                    resultCount = resultOntime+resultPtime;
                    long resultEndTime = System.currentTimeMillis();
//                    onTimeResultData.endTime = record.timestamp();
                    CustomValue resultValue = new CustomValue(resultWindow, resultCount, resultEventTime, resultEndTime);

//                    ProducerRecord<String, CustomValue> producerRecord =
//                            new ProducerRecord<>(combineResultTopic, DATA, onTimeResultData);
//                    producer.send(producerRecord);

                    int processTime = (int) (resultEndTime - resultEventTime);
                    System.out.println("window: " + resultValue.window + ", Ontime count: " + resultOntime +
                            ", Ptime Count: "+resultPtime+", Event Time: " + resultValue.eventTime +
                            ", End Time: " + resultValue.endTime + ", Process Time: " + processTime);
                    // mysql db에 insert
                    pstmt.setInt(1, resultValue.window);
                    pstmt.setInt(2, resultValue.value);
                    pstmt.setInt(3, processTime);

                    // insert문 실행
                    pstmt.executeUpdate();

                    resultOntime = 0;
                    resultPtime = 0;
                    resultEventTime= 0L;
                }

            }
            Thread.sleep(1);
        }
    }
}
