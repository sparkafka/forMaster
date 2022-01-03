package org.example;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;

public class TopicDeleteCreate {
    private static String bootServers = "node0:9092,node1:9092,node2:9092,node3:9092";

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
            deleteTopics.add("ptimeTriggerTopic");
            deleteTopics.add("combineResultTopic");
            deleteTopics.add("ltimeTopic");
            deleteTopics.add("ontimeResultTrigger");

            ArrayList<NewTopic> newTopics = new ArrayList<>();
            NewTopic newTopic1 = new NewTopic("dataSource",4, (short) 1);
            NewTopic newTopic2 = new NewTopic("ontimeTriggerTopic",4, (short) 1);
            NewTopic newTopic3 = new NewTopic("ptimeTopic",4, (short) 1);
            NewTopic newTopic4 = new NewTopic("tmTopic",1, (short) 1);
            NewTopic newTopic5 = new NewTopic("ontimeResultTopic",1, (short) 1);
            NewTopic newTopic6 = new NewTopic("ptimeTriggerTopic",1, (short) 1);
            NewTopic newTopic7 = new NewTopic("combineResultTopic",1, (short) 1);
            NewTopic newTopic8 = new NewTopic("ltimeTopic",1, (short) 1);
            NewTopic newTopic9 = new NewTopic("ontimeResultTrigger",1, (short) 1);
            newTopics.add(newTopic1);
            newTopics.add(newTopic2);
            newTopics.add(newTopic3);
            newTopics.add(newTopic4);
            newTopics.add(newTopic5);
            newTopics.add(newTopic6);
            newTopics.add(newTopic7);
            newTopics.add(newTopic8);
            newTopics.add(newTopic9);

            adminClient.deleteTopics(deleteTopics);
            adminClient.createTopics(newTopics);
        }
    }
}
