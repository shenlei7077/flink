package com.atguigu.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Random;

public  class MySensor implements SourceFunction<SensorReading> {
    boolean running=true;
    Random random = new Random ();
    @Override
    public void run(SourceContext sourceContext) throws Exception {
      //定义map
        HashMap<String, Double> tempMap = new HashMap<> ();
        //向map中添加基准值
        for (int i = 0; i <10 ; i++) {
            tempMap.put ("sensor_"+i,50+random.nextGaussian ()*20);

        }
        while(running){
            //遍历map
            for (String id:
                 tempMap.keySet ()) {
                //提取上一次当前传感器温度
                Double temp = tempMap.get (id);
                double newTemp=temp+random.nextGaussian ();
                sourceContext.collect (new SensorReading (id,System.currentTimeMillis (),newTemp));
                tempMap.put (id,newTemp);

            }
            Thread.sleep (2000);
        }

    }

    @Override
    public void cancel() {
    running=false;
    }
}
