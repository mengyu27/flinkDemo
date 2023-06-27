package com.lml.transform;

import cn.hutool.json.JSONUtil;
import com.lml.apitest.pojo.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 分解流
 * @author LML
 * @date 2023/5/5
 **/
public class TransformSplitStream {
    public static void main(String[] args) {
        //创建运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> dataStream = env.readTextFile("E:\\peoject\\flinkDemo\\src\\main\\resources\\SourceCollectionFileTest.txt");
        //转换为对象流
        DataStream<SensorReading> dataStreamMap = dataStream.map(line -> JSONUtil.toBean(line, SensorReading.class));
//        DataStreamSink<SensorReading> sensorReadingDataStreamSink = dataStreamMap.sinkTo();
        DataStream<SensorReading> dataStreamMap2 = dataStream.map(line -> JSONUtil.toBean(line, SensorReading.class));
        System.out.println(dataStreamMap2);
    }
}
