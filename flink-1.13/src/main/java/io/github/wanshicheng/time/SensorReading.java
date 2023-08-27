package io.github.wanshicheng.time;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class SensorReading {
    private String id;
    private Long timestamp;
    private Double temperature;
}
