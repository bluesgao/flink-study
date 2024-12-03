package org.example.event;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

// 用户登录事件类
@Data
@NoArgsConstructor
@AllArgsConstructor
public class LoginEvent {
    private String userId;   // 用户 ID
    private String status;   // 登录状态：SUCCESS 或 FAIL
    private long timestamp;  // 登录时间戳
}