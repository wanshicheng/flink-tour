package io.github.wanshicheng.flink.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class SensorRecord {
    private String id;
    private Double temperature;
    private Long timestamp;
}
