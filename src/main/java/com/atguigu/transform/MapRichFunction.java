package com.atguigu.transform;

import com.atguigu.source.SensorReading;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MapRichFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment ();
        DataStreamSource<String> source = env.readTextFile ("hello.txt");
        SingleOutputStreamOperator<SensorReading> map = source.map (new MyRichMapFunction ());
        map.print ();
        env.execute ();
    }

}
 class MyRichMapFunction extends RichMapFunction<String, SensorReading>{

     public MyRichMapFunction() {
         super ();
     }

     @Override
     public void open(Configuration parameters) throws Exception {
         super.open (parameters);
     }

     @Override
     public void close() throws Exception {
         super.close ();
     }

     @Override
     public SensorReading map(String value) throws Exception {
         String[] split = value.split (",");
         return new SensorReading (split[0], Long.parseLong (split[1]), Double.parseDouble (split[2]));
     }
 }
