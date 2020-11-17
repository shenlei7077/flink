package com.atguigu.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

public class Udsource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment ();
        env.setParallelism (1);
        //从kafka读取数据
        Properties properties = new Properties ();
        properties.setProperty ("bootstrap.servers","hadoop102:9092");
        properties.setProperty ("group.id","consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

        DataStreamSource dataStreamSource = env.addSource (new MySensor ());
        dataStreamSource.print ();
        env.execute ("Udsource");
    }
}
