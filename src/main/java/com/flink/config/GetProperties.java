package com.flink.config;

import java.io.IOException;
import java.util.Properties;


public class GetProperties {

    private static Properties properties = new Properties();

    static {
        try {
            properties.load(GetProperties.class.getClassLoader().getResourceAsStream("application.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Kafka相关
    public static String kafkaBootstrapServers = properties.getProperty("kafka.bootstrap.servers");
    public static String testTopic = properties.getProperty("testTopic.topic");
}
