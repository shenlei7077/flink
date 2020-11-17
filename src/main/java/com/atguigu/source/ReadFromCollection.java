package com.atguigu.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.Arrays;

public class ReadFromCollection {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment ();
        DataStreamSource<SensorReading> sensorReadingDataStreamSource = executionEnvironment.fromCollection (Arrays.asList (new SensorReading ("qwer", 444L, 35.1),
                new SensorReading ("asdf", 555L, 36.1)
        ));
sensorReadingDataStreamSource.print ();
        executionEnvironment.execute ("ReadFromCollection");
    }
}
