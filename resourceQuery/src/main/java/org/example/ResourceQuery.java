package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.sql.*;
import java.util.Properties;
import java.util.Random;

public class ResourceQuery {
    final private static int NODES = 4;
    final private static int oneday = 86400;
    final private static long startOf2 = 1325463000;
    final private static String url = "jdbc:mysql://localhost:3306/mydb?serverTimezone=UTC&useSSL=false";
    final private static String userName = "root";
    final private static String password = "root";

    final private static String topicName = "dataSource";
    private static final String tmTopic = "tmTopic";
    final private static String bootServers = "node1:9092,node2:9092,node3:9092,node4:9092";

    final private static String DATA = "D";
    final private static String WM0 = "W0";
    final private static String WM1 = "W1";

    public static void main(String[] args) throws SQLException, InterruptedException {
        Connection con = DriverManager.getConnection(url, userName, password);

        String SQL = "SELECT * FROM DelayedData_ten_imme order by eventTime";
        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery(SQL);
        Random rand = new Random();

        Properties conf = new Properties();
        conf.setProperty("bootstrap.servers", bootServers);
        conf.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        conf.setProperty("value.serializer", "org.example.CustomValueSerializer");

        Producer<String, CustomValue> producer = new KafkaProducer<>(conf);

        CustomValue customValue;
        int winNum, value;
        long startTime, endTime;
        long preEventTime = 0L;

        // 10%: 1325462700 + 8640 + 8640
        // 5%: 1325462700 + 4320 + 4320
        long wm0 = startOf2;
        long wm1 = wm0 + oneday/20;
        int currentWin = 1;
        long basis = startOf2;

        if (rs.next()) {
            winNum = rs.getInt(1);
            value = rs.getInt(2);
            startTime = rs.getLong(3);
            endTime = rs.getLong(4);

            preEventTime = startTime;
            System.out.println("window: " + winNum + " value: " + value + " startTime: " + startTime + " endTime: " + endTime);
            customValue = new CustomValue(winNum, value, startTime, endTime);
            ProducerRecord<String, CustomValue> record = new ProducerRecord<>(topicName, rand.nextInt(4), DATA, customValue);
            producer.send(record);
        }

        while(rs.next()){
            winNum = rs.getInt(1);
            value = rs.getInt(2);
            startTime = rs.getLong(3);
            endTime = rs.getLong(4);

            long dataDelay = startTime - preEventTime;

            Thread.sleep(dataDelay/30);
            if(startTime>=basis){
                currentWin++;
                basis += oneday;
            }
//            Thread.sleep(5);

            System.out.println("window: "+winNum+" value: "+value+" startTime: "+startTime+" endTime: "+endTime);

            customValue = new CustomValue(winNum, value, startTime, endTime);
            ProducerRecord<String, CustomValue> record = new ProducerRecord<>(topicName, rand.nextInt(4), DATA, customValue);
            producer.send(record);
            preEventTime = startTime;
            if(startTime>=wm0){
                System.out.println("Wm0");
                /*for(int i=0;i<NODES;i++){
                    record = new ProducerRecord<>(topicName,i, WM0, new CustomValue(currentWin, 0));
                    producer.send(record);
                }*/
                record = new ProducerRecord<>(tmTopic, WM0, new CustomValue(currentWin, 0));
                producer.send(record);
                wm0 += oneday;
            } else if(startTime>=wm1){
                System.out.println("Wm1");
                /*for(int i=0;i<NODES;i++){
                    record = new ProducerRecord<>(topicName,i, WM1, new CustomValue(currentWin, 0));
                    producer.send(record);
                }*/
                record = new ProducerRecord<>(tmTopic, WM1, new CustomValue(currentWin, 0));
                producer.send(record);
                wm1 += oneday;
            }
        }
        /*for (int j = 1; j <= 100; j++) {
            customValue = new CustomValue(1, j, 0L, 0L);
            ProducerRecord<String, CustomValue> record = new ProducerRecord<>(topicName, rand.nextInt(4), DATA, customValue);
            producer.send(record);
            Thread.sleep(10);
        }
        ProducerRecord<String, CustomValue> record = new ProducerRecord<>(topicName, 1, DATA,
                new CustomValue(2, 1));
        producer.send(record);
        for (int j = 1; j <= 100; j++) {
            customValue = new CustomValue(1, j, 0L, 0L);
            record = new ProducerRecord<>(topicName, rand.nextInt(4), DATA, customValue);
            producer.send(record);
            Thread.sleep(10);
        }*/
        /*for (int i = 2; i <= 4; i++) {
            for (int j = 1; j <= 100; j++) {
                customValue = new CustomValue(i, j, 0L, 0L);
                ProducerRecord<String, CustomValue> record = new ProducerRecord<>(topicName, rand.nextInt(4), DATA, customValue);
                producer.send(record);
                customValue = new CustomValue(i-1, j, 0L, 0L);
                record = new ProducerRecord<>(topicName, rand.nextInt(4), DATA, customValue);
                producer.send(record);
                Thread.sleep(10);
            }
        }*/
        producer.close();
        rs.close();
        stmt.close();
        con.close();
    }
}
