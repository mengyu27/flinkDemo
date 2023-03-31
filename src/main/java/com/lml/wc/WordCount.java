package com.lml.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.SortPartitionOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.StringValue;
import org.apache.flink.util.Collector;

import java.util.Comparator;
import java.util.List;

/**
 * @author LML
 * @date 2023/3/30
 **/
public class WordCount {
    public static void main(String[] args) throws Exception {
        //创建运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> helloTxt = env.readTextFile("E:\\peoject\\flinkDemo\\src\\main\\resources\\hello.txt");
        AggregateOperator<Tuple2<String, Integer>> wordCount = helloTxt.flatMap(new MyFlatMapFunction())
                .groupBy(0)
                .sum(1);
        wordCount.print();
        System.out.println("---------------------------");
        List<Tuple2<String, Integer>> collect = wordCount.collect();
        collect.sort(Comparator.comparing(k -> k.f1));
        System.out.println(collect);
        System.out.println("---------------------------");
        SortPartitionOperator<Tuple2<String, Integer>> tuple2SortPartitionOperator = wordCount.sortPartition(1, Order.DESCENDING);
        tuple2SortPartitionOperator.print();
        System.out.println("---------------------------");
    }

    public static class MyFlatMapFunction implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            //将字符串分割成字符数组
            char[] chars = value.toCharArray();
            //每个字符单独进行统计
            for (char c : chars) {
                out.collect(new Tuple2<>(String.valueOf(c), 1));
            }
        }
    }

}
