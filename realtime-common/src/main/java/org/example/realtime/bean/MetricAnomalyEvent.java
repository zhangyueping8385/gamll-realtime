package org.example.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/** 规则检测命中的异常事件，同时写入 Kafka 和 Doris。 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MetricAnomalyEvent implements Serializable {
    private String anomalyId;
    private String metricCode;
    private String dimensionKey;
    private String dimensionsJson;
    private String stt;
    private String edt;
    private String curDate;
    private Long currentValue;
    private Double baselineValue;
    private Double changeRate;
    private Double zScore;
    private String severity;
    private String sourceTable;
    private String evidenceJson;
    private String createdAt;
    private Long ts;
}
