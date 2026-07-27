package org.example.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * 由各 DWS 任务输出的统一指标窗口事件。
 * 异常检测任务只依赖该结构，避免重复实现业务指标聚合逻辑。
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MetricWindowEvent implements Serializable {
    private String metricCode;
    private String dimensionKey;
    private String dimensionsJson;
    private String stt;
    private String edt;
    private String curDate;
    private Long value;
    private String sourceTable;
    private Long ts;
}
