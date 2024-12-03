package org.example.event;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
// 自定义数据类型
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SensorEvent {
    private String id;
    private double temperature;
    private long timestamp;
}
