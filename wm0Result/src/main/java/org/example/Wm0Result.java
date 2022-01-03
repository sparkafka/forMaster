package org.example;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Wm0Result {
    final private static int NODES = 4;

    final private static String DATA = "D";
    final private static String PTIME_COMPLETE = "P";
    final private static String WM0_RESULT = "R0";
    final private static String WM1_RESULT = "R1";
    final private static String WM0_COMPLETE = "C0";
    final private static String WM1_COMPLETE = "C1";

    private static final String bootServers = "node1:9092,node2:9092,node3:9092,node4:9092";
    final private static String onResultTopic = "ontimeResultTopic";
    final private static String onResultTrigger = "ontimeResultTrigger";
    final private static String combineResultTopic = "combineResultTopic";

    final private static String url = "jdbc:mysql://localhost:3306/mydb?serverTimezone=UTC&useSSL=false";
    final private static String userName = "root";
    final private static String password = "root";

    public static void main(String[] args) throws SQLException {
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
        String SQL = "insert into Wm1Ten1(windowNum, value, processTime)" +
                " values(?, ?, ?)";
        PreparedStatement pstmt = con.prepareStatement(SQL);

        int wm0Window = 0;
        int wm0Count = 0;
        int wm1Window = 0;
        int wm1Count = 0;
        long wm0EventTime = 0L;
        long wm1EventTime = 0L;

        int wm0Results = 0;
        int wm1Results = 0;


        while (true) {
            ConsumerRecords<String, CustomValue> records = consumer.poll(Duration.ofMillis(10));
            for (ConsumerRecord<String, CustomValue> record : records) {
                String msgString = String.format("key:%s, window:%d, value:%d, time:%d",
                        record.key(), record.value().window, record.value().value, record.value().eventTime);
                System.out.println(msgString);
                if (record.key().equals(WM0_RESULT)) {
//                    wm0Results++;
                    if (wm0EventTime == 0L) {
                        wm0Window = record.value().window;
                        wm0Count += record.value().value;
                        wm0EventTime = record.value().eventTime;
                    } else {
                        wm0Count += record.value().value;
                        if (wm0EventTime > record.value().eventTime) wm0EventTime = record.value().eventTime;
                    }
                    /*if (wm0Results >= NODES) {
                        System.out.println("WM0 Complete");
                        long wm0EndTime = System.currentTimeMillis();
                        CustomValue wm0ResultValue = new CustomValue(wm0Window, wm0Count, wm0EventTime, wm0EndTime);
                        int wm0ProcessTime = (int) (wm0EndTime - wm0EventTime);
                        System.out.println("Window: " + wm0ResultValue.window + ", Count: " + wm0Count +
                                ", Event Time: " + wm0EventTime +
                                ", End Time: " + wm0EndTime + ", Process Time: " + wm0ProcessTime);

//                     mysql db에 insert
                        pstmt.setInt(1, wm0ResultValue.window);
                        pstmt.setInt(2, wm0ResultValue.value);
                        pstmt.setInt(3, wm0ProcessTime);

//                     insert문 실행
//                    pstmt.executeUpdate();

                        wm0Window = 0;
                        wm0Count = 0;
                        wm0EventTime = 0L;
                        wm0Results = 0;
                    }*/
                } else if (record.key().equals(WM1_RESULT)) {
//                    wm1Results++;
                    if (wm1EventTime == 0L) {
                        wm1Window = record.value().window;
                        wm1Count += record.value().value;
                        wm1EventTime = record.value().eventTime;
                    } else {
                        wm1Count += record.value().value;
                        if (wm1EventTime > record.value().eventTime) wm1EventTime = record.value().eventTime;
                    }
                    /*if (wm1Results >= NODES) {
                        System.out.println("WM1 Complete");
                        long wm1EndTime = System.currentTimeMillis();
                        CustomValue wm1ResultValue = new CustomValue(wm1Window, wm1Count + wm0Count, wm0EventTime, wm1EndTime);
                        int wm1ProcessTime = (int) (wm1EndTime - wm0EventTime);
                        System.out.println("Window: " + wm1ResultValue.window + ", Count: " + wm1ResultValue.value +
                                ", Event Time: " + wm0EventTime +
                                ", End Time: " + wm1EndTime + ", Process Time: " + wm1ProcessTime);

                        // mysql db에 insert
                        pstmt.setInt(1, wm1ResultValue.window);
                        pstmt.setInt(2, wm1ResultValue.value);
                        pstmt.setInt(3, wm1ProcessTime);

                        // insert문 실행
//                        pstmt.executeUpdate();

                        wm0Window = 0;
                        wm0Count = 0;
                        wm0EventTime = 0L;

                        wm1Window = 0;
                        wm1Count = 0;
                        wm1EventTime = 0L;
                        wm1Results = 0;
                    }*/
                }
                else if (record.key().equals(WM0_COMPLETE)){
                    System.out.println("WM0 Complete");
                    long wm0EndTime = System.currentTimeMillis();
                    CustomValue wm0ResultValue = new CustomValue(wm0Window, wm0Count, wm0EventTime, wm0EndTime);
                    int wm0ProcessTime = (int) (wm0EndTime - wm0EventTime);
                    System.out.println("Window: " + wm0ResultValue.window + ", Count: " + wm0Count +
                            ", Event Time: " + wm0EventTime +
                            ", End Time: " + wm0EndTime + ", Process Time: " + wm0ProcessTime);
//                     mysql db에 insert
                    pstmt.setInt(1, wm0ResultValue.window);
                    pstmt.setInt(2, wm0ResultValue.value);
                    pstmt.setInt(3, wm0ProcessTime);

//                     insert문 실행
//                    pstmt.executeUpdate();
//
//                    wm0Window = 0;
//                    wm0Count = 0;
//                    wm0EventTime = 0L;
                } else if (record.key().equals(WM1_COMPLETE)){
                    System.out.println("WM1 Complete");
                    long wm1EndTime = System.currentTimeMillis();
                    CustomValue wm1ResultValue = new CustomValue(wm1Window, wm1Count+wm0Count, wm0EventTime, wm1EndTime);
                    int wm1ProcessTime = (int) (wm1EndTime - wm0EventTime);
                    System.out.println("Window: " + wm1ResultValue.window + ", Count: " + wm1ResultValue.value +
                            ", Event Time: " + wm0EventTime +
                            ", End Time: " + wm1EndTime + ", Process Time: " + wm1ProcessTime);
                    // mysql db에 insert
                    pstmt.setInt(1, wm1ResultValue.window);
                    pstmt.setInt(2, wm1ResultValue.value);
                    pstmt.setInt(3, wm1ProcessTime);

                    // insert문 실행
                    pstmt.executeUpdate();

                    wm0Window = 0;
                    wm0Count = 0;
                    wm0EventTime = 0L;

                    wm1Window = 0;
                    wm1Count = 0;
                    wm1EventTime = 0L;
                }

            }
        }
    }
}
