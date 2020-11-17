package com.atguigu.transform;

import com.atguigu.source.SensorReading;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;

import java.util.Collection;
import java.util.Collections;

public class FlinkTransform {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment ();
        env.setParallelism (1);
        DataStreamSource<String> stringDataStreamSource = env.readTextFile ("hello.txt");
        //map
//        SingleOutputStreamOperator<Integer> map = stringDataStreamSource.map (new MapFunction<String, Integer> () {
//            @Override
//            public Integer map(String s) throws Exception {
//                return s.length ();
//            }
//        });
//        map.print ();
        //mapflat
//        SingleOutputStreamOperator<String> objectSingleOutputStreamOperator = stringDataStreamSource.flatMap (new FlatMapFunction<String,String> () {
//            @Override
//            public void flatMap(String s, Collector<String> collector) throws Exception {
//                String[] split = s.split (",");
//                collector.collect (split[2]);
//            }
//        });
//        objectSingleOutputStreamOperator.print ();
        //filter
//        SingleOutputStreamOperator<String> filter = stringDataStreamSource.filter (new FilterFunction<String> () {
////            @Override
////            public boolean filter(String s) throws Exception {
////                String[] split = s.split (",");
////                return Double.parseDouble (split[2]) > 30;
////            }
////        });
////        filter.print ();
        //keyby
//        SingleOutputStreamOperator<SensorReading> map = stringDataStreamSource.map (new MapFunction<String, SensorReading> () {
//            @Override
//            public SensorReading map(String s) throws Exception {
//                String[] split = s.split (",");
//                return new SensorReading (split[0], Long.parseLong (split[1]), Double.parseDouble (split[2]));
//            }
//        });
//        //map.keyBy ("id").max ("temp").print ();
//        map.keyBy ("id").maxBy ("temp").print ();
        //reduce
        SingleOutputStreamOperator<SensorReading> map = stringDataStreamSource.map (new MapFunction<String, SensorReading> () {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] split = s.split (",");
                return new SensorReading (split[0], Long.parseLong (split[1]), Double.parseDouble (split[2]));
            }
        });
//        KeyedStream<SensorReading, Tuple> id = map.keyBy ("id");
//        SingleOutputStreamOperator<SensorReading> reduce = id.reduce (new ReduceFunction<SensorReading> () {
//            @Override
//            public SensorReading reduce(SensorReading value1, SensorReading value2) throws Exception {
//                return new SensorReading (value2.getId (), value2.getTs (), Math.max (value1.getTemp (), value2.getTemp ()));
//            }
//        });
//        reduce.print ();
        SplitStream<SensorReading> split = map.split (new OutputSelector<SensorReading> () {
            @Override
            public Iterable<String> select(SensorReading value) {
                return value.getTemp () > 30 ? Collections.singletonList ("high") : Collections.singletonList ("low");
            }
        });
//        SingleOutputStreamOperator<Tuple2<String, Double>> high = split.select ("high").map (new MapFunction<SensorReading, Tuple2<String, Double>> () {
//            @Override
//            public Tuple2<String, Double> map(SensorReading sensorReading) throws Exception {
//                return new Tuple2<String, Double> (sensorReading.getId (), sensorReading.getTemp ());
//            }
//        });
//        DataStream<SensorReading> low = split.select ("low");
//        ConnectedStreams<Tuple2<String, Double>, SensorReading> connect = high.connect (low);
//        SingleOutputStreamOperator<Object> map1 = connect.map (new CoMapFunction<Tuple2<String, Double>, SensorReading, Object> () {
//            @Override
//            public Object map1(Tuple2<String, Double> value) throws Exception {
//                return new Tuple3<> (value.f0, value.f1, "warn");
//            }
//
//            @Override
//            public Object map2(SensorReading sensorReading) throws Exception {
//                return new Tuple2<> (sensorReading.getId (), "healthy");
//            }
//        });
//        map1.print ();
//        high.print ();
//        low.print ();
        //connect
        //union
        DataStream<SensorReading> high = split.select ("high");
        DataStream<SensorReading> low = split.select ("low");
        DataStream<SensorReading> union = high.union (low);
        union.print ();
        env.execute ();
    }
}
