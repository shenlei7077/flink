package com.atguigu.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class Wordcount_Bctch {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment ();
        DataSource<String> line = executionEnvironment.readTextFile ("hello.txt");
        AggregateOperator<Tuple2<String, Integer>> wordcount = line.flatMap (new FlatMapFunction<String, Tuple2<String, Integer>> () {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] strings = s.split (" ");
                for (String word : strings) {
                    collector.collect (new Tuple2<String, Integer> (word, 1));
                }
            }
        }).groupBy (0).sum (1);
       wordcount.print ();
    }
}
