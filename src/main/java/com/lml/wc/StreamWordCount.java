package com.lml.wc;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.lang.reflect.Parameter;

/**
 * @author LML
 * @date 2023/3/30
 **/
public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        //获取流处理器运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //从启动项获取参数
//        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        DataStreamSource<String> helloTxtStream = env.socketTextStream("192.168.153.132",7777);
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = helloTxtStream.flatMap(new WordCount.MyFlatMapFunction()).keyBy(new KeySelector<Tuple2<String, Integer>, Object>() {
            @Override
            public Object getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.f0;
            }
        }).sum(1);
        sum.print();
        env.execute();

    }
}
