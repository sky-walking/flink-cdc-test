package com.flink.cdcjobs;

import com.ververica.cdc.connectors.sqlserver.SqlServerSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @Author: 郝晨皓
 * @CreateTime: 2023-04-16  22:56
 * @Description:
 */
public class SqlServerCdcTest {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().enableUnalignedCheckpoints();// 开启checkpoints

        String serverName = "yourHostname";
        int portNumber = 1433;
        String databaseName = "yourDatabaseName";
        String tableName = "yourSchemas.yourTableName";
        String username = "yourUsername";
        String password = "yourPassword";

        //2.创建CDC Source
        SourceFunction<String> sqlServerCdcSource = SqlServerSource.<String>builder()
                .hostname(serverName)
                .port(portNumber)
                .database(databaseName) // monitor sqlserver database
                .tableList(tableName) // monitor products table
                .username(username)
                .password(password)
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();

        //3.使用CDC Source从sql server读取数据
        DataStreamSource<String> stringDataStreamSource = env.addSource(sqlServerCdcSource);

        //4.打印
        stringDataStreamSource.print();

        env.execute();

    }
}
