package com.atguigu.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Wordcount_Bounded {
    public static void main(String[] args) throws Exception {
        //1.设置运行环境
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment ();
        //2.获取数据
        DataStreamSource<String> data = executionEnvironment.readTextFile ("hello.txt");
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = data.flatMap (new FlatMapFunction<String, Tuple2<String, Integer>> () {
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
        executionEnvironment.execute ("exe");

    }
}
