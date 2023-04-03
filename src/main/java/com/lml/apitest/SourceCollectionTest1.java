package com.lml.apitest;

import cn.hutool.core.util.IdUtil;
import cn.hutool.core.util.RandomUtil;
import com.lml.apitest.pojo.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.UUID;

/**
 * @author LML
 * @date 2023/4/3
 **/
public class SourceCollectionTest1 {
    public static void main(String[] args) throws Exception {
        //创建运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //创建集合
        DataStream<SensorReading> streamSource = env.fromCollection(Arrays.asList(
                new SensorReading(IdUtil.nanoId(), 1639364511000L, 36.1),
                new SensorReading(IdUtil.nanoId(), 1639364411000L, 42.0),
                new SensorReading(IdUtil.nanoId(), 1639364311000L, 12.2),
                new SensorReading(IdUtil.nanoId(), 1639364211000L, 22.22)
        ));
        DataStream<Integer> integerDataStream = env.fromElements(RandomUtil.randomInt(), RandomUtil.randomInt(), RandomUtil.randomInt(), RandomUtil.randomInt());
        streamSource.print("streamSource");
        integerDataStream.print("integerDataStream");
        //执行
        env.execute();
    }
}
