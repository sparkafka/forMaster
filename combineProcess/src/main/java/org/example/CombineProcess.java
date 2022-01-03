package org.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.consumer.*;

import java.sql.*;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class CombineProcess {
    final private static String ON_RESULT = "R";    // Result
    final private static String LATE_RESULT = "L";  // Late

    final private static String bootServers = "node1:9092,node2:9092,node3:9092,node4:9092";
    final private static String combineResultTopic = "combineResultTopic";

    final private static String url = "jdbc:mysql://localhost:3306/mydb?serverTimezone=UTC&useSSL=false";
    final private static String userName = "root";
    final private static String password = "root";

    public static void main(String[] args) throws SQLException {
        Properties consumeConf = new Properties();
        consumeConf.setProperty("bootstrap.servers", bootServers);
        consumeConf.setProperty("group.id", "CombineProcess");
        consumeConf.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumeConf.setProperty("value.deserializer", "org.example.CustomValueDeserializer");

        Consumer<String, CustomValue> consumer = new KafkaConsumer<>(consumeConf);
        consumer.subscribe(Collections.singletonList(combineResultTopic));

        Connection con = DriverManager.getConnection(url, userName, password);

        // INSERT Query
        String insertQuery = "insert into FinalResult(windowNum, value, eventTime, endTime, processTime, inputCount) " +
                "values(?, ?, ?, ?, ?, ?)";
        PreparedStatement insertStmt = con.prepareStatement(insertQuery);

        // SELECT Query
        String selectQuery = "select eventTime, inputCount from FinalResult where windowNum=? order by inputCount DESC limit 1";
        PreparedStatement selectStmt = con.prepareStatement(selectQuery);
        ResultSet rs;
        for (int i = 0; i < 30000; i++) {
            ConsumerRecords<String, CustomValue> records = consumer.poll(Duration.ofMillis(10));
            for (ConsumerRecord<String, CustomValue> record : records) {
                CustomValue recordValue = record.value();
                String msgString = String.format("key:%s, window:%d, value:%d, topic:%s, time:%d",
                        record.key(), recordValue.window, recordValue.value, record.topic(), record.timestamp());
                System.out.println(msgString);

                if(record.key().equals(ON_RESULT)){
                    int processTime = (int) (recordValue.endTime - recordValue.eventTime);
                    insertStmt.setInt(1, recordValue.window);
                    insertStmt.setInt(2, recordValue.value);
                    insertStmt.setLong(3, recordValue.eventTime);
                    insertStmt.setLong(4, recordValue.endTime);
                    insertStmt.setInt(5, processTime);
                    insertStmt.setInt(6, 1);

                    insertStmt.executeUpdate();

                } else {
                    selectStmt.setInt(1, recordValue.window);
                    rs = selectStmt.executeQuery();
                    while(rs.next()){
                        long preEventTime = rs.getLong(1);
                        int preInputCount = rs.getInt(2);

                        int processTime = (int) (recordValue.endTime - preEventTime);
                        insertStmt.setInt(1, recordValue.window);
                        insertStmt.setInt(2, recordValue.value);
                        insertStmt.setLong(3, preEventTime);
                        insertStmt.setLong(4, recordValue.endTime);
                        insertStmt.setInt(5, processTime);
                        insertStmt.setInt(6, preInputCount+1);

                        insertStmt.executeUpdate();
                    }
                    rs.close();
                }
            }

        }
        selectStmt.close();
        insertStmt.close();
        con.close();
        consumer.close();
    }
}
