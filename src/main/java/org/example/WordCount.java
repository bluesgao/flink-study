package org.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class WordCount {
    public static void main(String[] args) throws Exception {
        //1,创建flink执行环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Configuration config = new Configuration();
        config.setString(RestOptions.BIND_PORT, "18081");
//        config.setString(ConfigConstants.LOCAL_START_WEBSERVER, "true");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
        //2,定义数据处理逻辑
        //2.1,从socket中读取数据
        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);
        //2.2,分割单词
        DataStream<Tuple2<String, Integer>> singleWordDataStream = source.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                Arrays.stream(s.split(" ")).forEach(word -> collector.collect(Tuple2.of(word, 1)));
            }
        });

        //分组
        DataStream<Tuple2<String, Integer>> wordCountDataStream = singleWordDataStream.keyBy(v -> v.f0).sum(1);

        //2.3，将结果输出到console
        DataStreamSink<Tuple2<String, Integer>> sink = wordCountDataStream.print();

        //3, 提交作业，并触发作业执行
        env.execute();
    }
}
