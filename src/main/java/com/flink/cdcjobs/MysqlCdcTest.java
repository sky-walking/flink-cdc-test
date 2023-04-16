package com.flink.cdcjobs;

import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: 郝晨皓
 * @CreateTime: 2023-04-16  22:56
 * @Description:
 */
public class MysqlCdcTest {
    public static void main(String[] args) throws Exception {

        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().enableUnalignedCheckpoints();// 开启checkpoints

        //2.创建CDC Source
        DebeziumSourceFunction<String> mysqlSource = MySqlSource.<String>builder()
                .hostname("yourHostname")
                .port(3306)
                .databaseList("yourDatabaseName")
                .tableList("yourDatabaseName.yourTableName")
                .username("yourUsername")
                .password("yourPassword")
                .deserializer(new CustomerDeserialization())
                .build();


        //3.使用CDC Source从MySQL读取数据
        DataStreamSource<String> mysqlDS = env.addSource(mysqlSource);

        //打印到控制台
        mysqlDS.print();

        //4.写入到kafka
//        mysqlDS.addSink(SinkToKafka.getKafkaProducer(GetProperties.testTopic, GetProperties.kafkaBootstrapServers));

        //5.执行任务
        env.execute();
    }
}
