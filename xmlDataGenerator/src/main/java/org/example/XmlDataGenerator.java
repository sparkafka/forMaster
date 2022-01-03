package org.example;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

public class XmlDataGenerator {

    final private static String url = "jdbc:mysql://localhost:3306/mydb?serverTimezone=UTC&useSSL=false";
    final private static String userName = "root";
    final private static String password = "root";
    final private static int criteria = 288 * 3;

    public static void main(String[] args) throws ParserConfigurationException, IOException, SAXException, SQLException {
        File file = new File("/home/kau-sw00/Downloads/cc_customer_11.xml");

        CustomValue value;

        Connection con = DriverManager.getConnection(url, userName, password);
        String SQL = "insert into DelayedTen_3D(windowNum, value, eventTime, endTime) values(?, ?, ?, ?)";
        PreparedStatement pstmt = con.prepareStatement(SQL);

        // 빌더 팩토리 생성
        DocumentBuilderFactory builderFactory = DocumentBuilderFactory.newInstance();
        // 빌더 생성
        DocumentBuilder builder = builderFactory.newDocumentBuilder();
        // 빌더를 통해 XML 문서를 파싱해서 Document 객체로 가져온다
        Document document = builder.parse(file);
        // 문서 구조 normalize
        document.getDocumentElement().normalize();

        System.out.println("Root Element: " + document.getDocumentElement().getNodeName());
        NodeList blockList = document.getElementsByTagName("IntervalReading"); // IntervalReading 노드들의 모음
        System.out.println(blockList.getLength());
        ArrayList<CustomValue> dataList = new ArrayList<>();
        // IntervalBlock 개수


        int readingNum = 0;
//        for (int i = 1; i < (blockList.getLength() / ONEDAY); i++) {
        for (int i = 0; i < blockList.getLength(); i++) {
            int sum = 0;

            Node blockNode = blockList.item(i);
            System.out.println("\nCurrent Element :" + blockNode.getNodeName());
            if (blockNode.getNodeType() == Node.ELEMENT_NODE) {
                Element blockElement = (Element) blockNode;

                NodeList readingChild = blockElement.getChildNodes();
                long dataGenerateTime = 0;
                for (int k = 0; k < readingChild.getLength(); k++) {
                    Node readingChildNode = readingChild.item(k);
                    if (readingChildNode.getNodeType() == Node.ELEMENT_NODE) {
                        Element readingChildElement = (Element) readingChildNode;
                        System.out.println("Current Element :" + readingChildElement.getNodeName());
                        if (readingChildElement.getNodeName().equals("timePeriod")) {
                            int duration = Integer.parseInt(readingChildElement.getElementsByTagName("duration").item(0).getTextContent());
                            int startTime = Integer.parseInt(readingChildElement.getElementsByTagName("start").item(0).getTextContent());
                            System.out.println("duration : " + duration);
                            System.out.println("start : " + startTime);
                            dataGenerateTime = duration + startTime;
                        } else {
                            int currentValue = Integer.parseInt(readingChildElement.getTextContent());
                            sum += currentValue;
//                            value = new CustomValue(i / criteria + 1, currentValue, dataGenerateTime, 0L);
                            /*if (i%ONEDAY < 259) {
                                value = new CustomValue(i / ONEDAY + 1, currentValue, dataGenerateTime, 0L);
                            } else {
                                value = new CustomValue(i / ONEDAY + 1, currentValue, dataGenerateTime + 8850, 0L);
                            }*/
                            /*if (i%ONEDAY < 216) {
                                value = new CustomValue(i / ONEDAY + 1, currentValue, dataGenerateTime, 0L);
                            } else {
                                value = new CustomValue(i / ONEDAY + 1, currentValue, dataGenerateTime + 21750, 0L);
                            }*/
                            /*if (i%criteria < 1814) {
                                value = new CustomValue(i / criteria + 1, currentValue, dataGenerateTime, 0L);
                            } else {
                                value = new CustomValue(i / criteria + 1, currentValue, dataGenerateTime + 60750, 0L);
                            }*/
                            if (i%criteria < 778) {
                                value = new CustomValue(i / criteria + 1, currentValue, dataGenerateTime, 0L);
                            } else {
                                value = new CustomValue(i / criteria + 1, currentValue, dataGenerateTime + 25950750, 0L);
                            }
                            System.out.println("window:" + value.window + " value:" + value.value + " generate Time: " + dataGenerateTime);
                            dataList.add(value);
                        }
                    }
                }
                /*for (int j = 0; j < blockChild.getLength(); j++) {
                    // 처음 interval, 다음 12개 IntervalReading
                    Node blockChildNode = blockChild.item(j);
                    if (blockChildNode.getNodeType() == Node.ELEMENT_NODE) {
                        Element blockChildElement = (Element) blockChildNode;
                        System.out.println("Current Element :" + blockChildElement.getNodeName());
                        if (blockChildElement.getNodeName().equals("interval")) {
                            // getElementsByTagName: node에 상관 없이 태그 이름으로만 찾기
                            System.out.println("duration : " + blockChildElement.getElementsByTagName("duration").item(0).getTextContent());
                            System.out.println("start : " + blockChildElement.getElementsByTagName("start").item(0).getTextContent());
                        } else {
                            NodeList readingChild = blockChildElement.getChildNodes();
                            long dataGenerateTime = 0;
                            for (int k = 0; k < readingChild.getLength(); k++) {
                                Node readingChildNode = readingChild.item(k);
                                if (readingChildNode.getNodeType() == Node.ELEMENT_NODE) {
                                    Element readingChildElement = (Element) readingChildNode;
                                    System.out.println(readingChildElement.getNodeName());
                                    if (readingChildElement.getNodeName().equals("timePeriod")) {
                                        int duration = Integer.parseInt(readingChildElement.getElementsByTagName("duration").item(0).getTextContent());
                                        int startTime = Integer.parseInt(readingChildElement.getElementsByTagName("start").item(0).getTextContent());
                                        System.out.println("duration : " + duration);
                                        System.out.println("start : " + startTime);
                                        dataGenerateTime = duration + startTime;
                                    } else if (readingChildElement.getNodeName().equals("value")){
                                        int currentValue = Integer.parseInt(readingChildElement.getTextContent());
                                        sum += currentValue;

                                        key = DATA;
                                        value = new CustomValue(i, currentValue, dataGenerateTime, 0L);
                                        System.out.println("window:" + value.window + " value:" + value.value + " generate Time: " + dataGenerateTime);
                                        dataList.add(value);

                                    }

                                }
                            }
                        }

                    }
                }*/
            }
            System.out.println("Sum : " + sum);
        }
        // Comparator.comparing
        dataList.sort(Comparator.comparing(c -> c.eventTime));
        for (CustomValue customValue : dataList) {
            System.out.println("window:" + customValue.window + " value:" + customValue.value + " generate Time: " + customValue.eventTime);
            // mysql db에 insert
            pstmt.setInt(1, customValue.window);
            pstmt.setInt(2, customValue.value);
            pstmt.setLong(3, customValue.eventTime);
            pstmt.setLong(4, customValue.endTime);

            // insert문 실행
            pstmt.executeUpdate();
        }
        pstmt.close();
        con.close();
    }
}
