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

public class XmlParsingTest {
    public static void main(String[] args) throws ParserConfigurationException, IOException, SAXException{
        File file = new File("/home/kau-sw00/Downloads/Coastal_Multi_Family_12hr_Jan_1_2011_to_Jan_1_2012_RetailCustomer_3.xml");

        // 빌더 팩토리 생성
        DocumentBuilderFactory builderFactory = DocumentBuilderFactory.newInstance();

        // 빌더 생성
        DocumentBuilder builder = builderFactory.newDocumentBuilder();

        // 빌더를 통해 XML 문서를 파싱해서 Document 객체로 가져온다
        Document document = builder.parse(file);

        // 문서 구조 normalize
        document.getDocumentElement().normalize();

        System.out.println("Root Element: " + document.getDocumentElement().getNodeName());
        NodeList blockList = document.getElementsByTagName("IntervalBlock"); // IntervalBlock 노드들의 모음

        // IntervalBlock 개수
        for (int i = 0; i < 3; i++) {
            int sum = 0;
            Node blockNode = blockList.item(i);
            System.out.println("\nCurrent Element :" + blockNode.getNodeName());
            if (blockNode.getNodeType() == Node.ELEMENT_NODE) {
                Element blockElement = (Element) blockNode;

                NodeList blockChild = blockElement.getChildNodes();
                for (int j = 0; j < blockChild.getLength(); j++) {
                    // 처음 interval, 다음 12개 IntervalReading
                    Node blockChildNode = blockChild.item(j);
                    if (blockChildNode.getNodeType() == Node.ELEMENT_NODE) {
                        Element blockChildElement = (Element) blockChildNode;
                        System.out.println("Current Element :" + blockChildElement.getNodeName());
                        if (blockChildElement.getNodeName().equals("interval")) {
                            // getElementsByTagName: node에 상관 없이 태그 이름으로만 찾기
                            System.out.println("duration : " + blockChildElement.getElementsByTagName("duration").item(0).getTextContent());
                            System.out.println("start : " + blockChildElement.getElementsByTagName("start").item(0).getTextContent());
                        }
                        else {
                            NodeList readingChild = blockChildElement.getChildNodes();
                            for(int k = 0; k<readingChild.getLength();k++){
                                Node readingChildNode = readingChild.item(k);
                                if(readingChildNode.getNodeType() == Node.ELEMENT_NODE){
                                    Element readingChildElement = (Element) readingChildNode;
                                    System.out.println(readingChildElement.getNodeName());
                                    if (readingChildElement.getNodeName().equals("timePeriod")) {
                                        System.out.println("duration : " + readingChildElement.getElementsByTagName("duration").item(0).getTextContent());
                                        System.out.println("start : " + readingChildElement.getElementsByTagName("start").item(0).getTextContent());
                                    } else {
                                        sum += Integer.parseInt(readingChildElement.getTextContent());
                                        System.out.println(readingChildElement.getTextContent());
                                    }

                                }
                            }
                        }

                    }
                }
            }
            System.out.println("Sum : " + sum);
        }
    }
}
