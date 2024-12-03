package org.example;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.event.SensorEvent;

import java.util.List;
import java.util.Map;

public class SensorRiskDetection {
    public static void main(String[] args) throws Exception {
        // 1. 创建 Flink 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 创建输入数据流
        DataStream<SensorEvent> input = env.fromElements(
                new SensorEvent("sensor_1", 35.5, System.currentTimeMillis()),
                new SensorEvent("sensor_1", 36.0, System.currentTimeMillis() + 1000),
                new SensorEvent("sensor_1", 37.0, System.currentTimeMillis() + 2000),
                new SensorEvent("sensor_2", 33.5, System.currentTimeMillis() + 3000)
        );

        // 3. 定义模式：连续三次温度超过 35
        Pattern<SensorEvent, ?> highTempPattern = Pattern.<SensorEvent>begin("start")
                .where(new IterativeCondition<SensorEvent>() {
                    @Override
                    public boolean filter(SensorEvent sensorEvent, Context<SensorEvent> context) throws Exception {
                        return sensorEvent.getTemperature() > 35;
                    }
                }).timesOrMore(3);

        // 4. 将模式应用到输入流
        PatternStream<SensorEvent> patternStream = CEP.pattern(input, highTempPattern);


        DataStream<String> riskAlerts = patternStream.select(new PatternSelectFunction<SensorEvent, String>() {
            @Override
            public String select(Map<String, List<SensorEvent>> pattern) throws Exception {
                List<SensorEvent> matchedEvents = pattern.get("start");
                return "Detected high temperature: " + matchedEvents.toString();
            }
        });

        // 5. 打印结果
        riskAlerts.print();

        // 6. 启动程序
        env.execute("Flink CEP Example");
    }
}


