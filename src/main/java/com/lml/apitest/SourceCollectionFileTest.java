package com.lml.apitest;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 集合测试类-文件
 * @author LML
 * @date 2023/4/3
 **/
public class SourceCollectionFileTest {
    public static void main(String[] args) throws Exception {
        //创建运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> stringDataStream = env.readTextFile("E:\\peoject\\flinkDemo\\src\\main\\resources\\SourceCollectionFileTest.txt");
        stringDataStream.print();
        env.execute();

    }
}
