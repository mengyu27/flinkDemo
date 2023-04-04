package com.lml.transform;

import cn.hutool.core.util.StrUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * 一些简单常用的方法
 * @author LML
 * @date 2023/4/3
 **/
public class TransformTest {
    public static void main(String[] args) throws Exception {
        //创建运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> dataStream = env.readTextFile("E:\\peoject\\flinkDemo\\src\\main\\resources\\SourceCollectionFileTest.txt");
        DataStream<Integer> dataLength = dataStream.map(new MapFunction<String,Integer>() {
            @Override
            public Integer map(String o) throws Exception {
                return o.length();
            }
        });
        DataStream<String> flatMapStream = dataStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                List<String> split = StrUtil.split(s, StrUtil.COMMA);
                split.forEach(d -> collector.collect(d));
            }
        });
        //reduce使用
        SingleOutputStreamOperator<String> reduce = dataStream.keyBy(new KeySelector<String, Object>() {
            @Override
            public Object getKey(String s) throws Exception {
                return s;
            }
        }).reduce(new ReduceFunction<String>() {
            @Override
            public String reduce(String s, String t1) throws Exception {
                return "t1t1";
            }
        });
        reduce.print();
//        dataLength.print();
//        flatMapStream.print();
        env.execute();
    }
}
