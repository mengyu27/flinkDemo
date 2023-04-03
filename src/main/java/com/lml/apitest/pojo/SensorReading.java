package com.lml.apitest.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 传感器温度类
 * @author LML
 * @date 2023/4/3
 **/
@Data
@AllArgsConstructor
@NoArgsConstructor
public class SensorReading {
    private String id;
    private Long timestamp;
    private Double temperature;
}
