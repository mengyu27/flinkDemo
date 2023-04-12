package com.lml.apitest;

import cn.hutool.core.thread.ThreadUtil;
import cn.hutool.core.util.IdUtil;
import cn.hutool.core.util.RandomUtil;
import com.lml.apitest.pojo.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 自定义数据源
 *
 * @author LML
 * @date 2023/4/3
 **/
public class SourceCustomTest {
    public static void main(String[] args) throws Exception {
        //创建运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<SensorReading> dateStream = env.addSource(new MySource());
        dateStream.print();
        env.execute();
    }

    public static class MySource implements SourceFunction<SensorReading> {
        boolean runFlag = true;
        static List<String> idsList = Arrays.asList(IdUtil.nanoId(8), IdUtil.nanoId(8),
                IdUtil.nanoId(8), IdUtil.nanoId(8), IdUtil.nanoId(8), IdUtil.nanoId(8),
                IdUtil.nanoId(8), IdUtil.nanoId(8));
        //test
        @Override
        public void run(SourceContext<SensorReading> ctx) throws Exception {
            while (runFlag) {
                ctx.collect(new SensorReading(idsList.get(RandomUtil.randomInt(0, 8)), System.currentTimeMillis(), RandomUtil.randomDouble(0.00, 100.00)));
                ThreadUtil.safeSleep(1000L);
            }
        }

        @Override
        public void cancel() {
            runFlag = false;
        }
    }
}
