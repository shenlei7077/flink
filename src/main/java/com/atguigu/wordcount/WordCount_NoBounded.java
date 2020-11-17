package com.atguigu.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCount_NoBounded {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment ec = StreamExecutionEnvironment.getExecutionEnvironment ();
        ParameterTool parameterTool = ParameterTool.fromArgs (args);
        String host = parameterTool.get ("host");
        int port = parameterTool.getInt ("port");
        DataStreamSource<String> stringDataStreamSource = ec.socketTextStream (host, port);
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = stringDataStreamSource.flatMap (new FlatMapFunction<String, Tuple2<String, Integer>> () {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = s.split (" ");
                for (String word : words) {
                    collector.collect (new Tuple2<> (word, 1));
                }
            }
        }).keyBy (0).sum (1);

        result.print ();
        //启动任务
        ec.execute ("exe");
    }
}
