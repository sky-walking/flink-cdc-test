package com.flink.sink;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * @author wangziqiang
 * @date 2022/6/20
 * @desc 输出到kafka
 */
public class SinkToKafka {

    public static SinkFunction<String> getKafkaProducer(String topicName, String brokers) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", brokers);
        return new FlinkKafkaProducer(topicName, new SimpleStringSchema(), properties);
    }
}
