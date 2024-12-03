package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.event.LoginEvent;

import java.time.Duration;
import java.util.List;

public class LoginRiskDetection {
    public static void main(String[] args) throws Exception {
        // 1. 创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 读取输入流，添加水印（模拟事件时间处理）
        DataStream<LoginEvent> loginEvents = env.fromElements(
                new LoginEvent("user_1", "FAIL", System.currentTimeMillis()),
                new LoginEvent("user_1", "FAIL", System.currentTimeMillis() + 1000),
                new LoginEvent("user_1", "FAIL", System.currentTimeMillis() + 2000),
                new LoginEvent("user_2", "SUCCESS", System.currentTimeMillis() + 3000),
                new LoginEvent("user_1", "SUCCESS", System.currentTimeMillis() + 4000)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
        );

        // 3. 定义匹配模式（10 秒内连续 3 次失败登录）
        Pattern<LoginEvent, ?> loginFailPattern = Pattern
                .<LoginEvent>begin("firstFail").where(new IterativeCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent, Context<LoginEvent> context) throws Exception {
                        return loginEvent.getStatus().equals("FAIL");
                    }
                }).next("secondFail").where(new IterativeCondition<LoginEvent>() {

                    @Override
                    public boolean filter(LoginEvent loginEvent, Context<LoginEvent> context) throws Exception {
                        return loginEvent.getStatus().equals("FAIL");
                    }
                }).next("thirdFail").where(new IterativeCondition<LoginEvent>() {

                    @Override
                    public boolean filter(LoginEvent loginEvent, Context<LoginEvent> context) throws Exception {
                        return loginEvent.getStatus().equals("FAIL");
                    }
                }).within(Duration.ofSeconds(10));// 限制 10 秒内完成


        // 4. 将模式应用到事件流
        PatternStream<LoginEvent> patternStream = CEP.pattern(
                loginEvents.keyBy(LoginEvent::getUserId), // 按用户 ID 分组
                loginFailPattern
        );

        // 5. 处理匹配结果
        DataStream<String> riskAlerts = patternStream.select(
                (PatternSelectFunction<LoginEvent, String>) pattern -> {
                    List<LoginEvent> first = pattern.get("firstFail");
                    List<LoginEvent>  third = pattern.get("thirdFail");
                    return "High Risk: User " + first.get(0).getUserId() +
                            " had 3 consecutive login failures. Last failed at: " + third.get(0).getTimestamp();
                }
        );

        // 6. 输出风险告警
        riskAlerts.print();

        // 7. 执行程序
        env.execute("Login Risk Detection");
    }
}
