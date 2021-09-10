package org.example;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;

public class TopicDeleteCreate {
    private static String bootServers = "node1:9092,node2:9092,node3:9092,node4:9092";

    public static void main(String[] args) {
        Properties conf = new Properties();
        conf.setProperty("bootstrap.servers", bootServers);
        try (AdminClient adminClient = AdminClient.create(conf)) {
            ArrayList<String> deleteTopics = new ArrayList<>();
            deleteTopics.add("dataSource");
            deleteTopics.add("ontimeTriggerTopic");
            deleteTopics.add("ptimeTopic");
            deleteTopics.add("tmTopic");
            deleteTopics.add("ontimeResultTopic");

            ArrayList<NewTopic> newTopics = new ArrayList<>();
            NewTopic newTopic1 = new NewTopic("dataSource",4, (short) 1);
            NewTopic newTopic2 = new NewTopic("ontimeTriggerTopic",4, (short) 1);
            NewTopic newTopic3 = new NewTopic("ptimeTopic",4, (short) 1);
            NewTopic newTopic4 = new NewTopic("tmTopic",4, (short) 1);
            NewTopic newTopic5 = new NewTopic("ontimeResultTopic",4, (short) 1);
            newTopics.add(newTopic1);
            newTopics.add(newTopic2);
            newTopics.add(newTopic3);
            newTopics.add(newTopic4);
            newTopics.add(newTopic5);

            adminClient.deleteTopics(deleteTopics);
            adminClient.createTopics(newTopics);
        }
    }
}
