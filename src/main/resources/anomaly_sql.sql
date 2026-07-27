-- 实时指标异常事件表。异常检测任务通过 Flink Doris Sink 写入。
CREATE TABLE IF NOT EXISTS gmall_realtime.ads_metric_anomaly
(
    anomaly_id      VARCHAR(64)  COMMENT '稳定的异常唯一标识',
    metric_code     VARCHAR(128) COMMENT '指标编码',
    dimension_key   VARCHAR(512) COMMENT '维度组合键',
    dimensions_json VARCHAR(4096) COMMENT '维度明细 JSON',
    stt             DATETIME COMMENT '窗口起始时间',
    edt             DATETIME COMMENT '窗口结束时间',
    cur_date        DATE COMMENT '业务日期',
    current_value   BIGINT COMMENT '当前窗口指标值',
    baseline_value  DOUBLE COMMENT '在线基线均值',
    change_rate     DOUBLE COMMENT '相对变化率',
    z_score         DOUBLE COMMENT '标准分',
    severity        VARCHAR(8) COMMENT 'P1/P2 告警等级',
    source_table    VARCHAR(128) COMMENT '来源 DWS 表',
    evidence_json   VARCHAR(4096) COMMENT '规则命中证据',
    created_at      DATETIME COMMENT '检测时间',
    ts              BIGINT COMMENT '检测时间戳'
)
ENGINE=OLAP
UNIQUE KEY(anomaly_id)
DISTRIBUTED BY HASH(anomaly_id) BUCKETS 10
PROPERTIES (
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true"
);

-- AI 归因服务写入的解释结果。模型只处理聚合指标和维度贡献，不接触用户明细。
CREATE TABLE IF NOT EXISTS gmall_realtime.ads_metric_anomaly_analysis
(
    analysis_id     VARCHAR(64) COMMENT '分析结果唯一标识',
    anomaly_id      VARCHAR(64) COMMENT '关联异常 ID',
    model_name      VARCHAR(128) COMMENT '模型名称',
    analysis_json   VARCHAR(8192) COMMENT '模型返回的结构化归因结果',
    evidence_json   VARCHAR(8192) COMMENT '提交给模型的聚合证据',
    status          VARCHAR(32) COMMENT 'COMPLETED/FAILED',
    error_message   VARCHAR(2048) COMMENT '失败原因，便于排查和重试',
    created_at      DATETIME COMMENT '分析完成时间'
)
ENGINE=OLAP
DUPLICATE KEY(analysis_id)
DISTRIBUTED BY HASH(anomaly_id) BUCKETS 10
PROPERTIES (
    "replication_num" = "1"
);

-- 已创建过旧版本分析表时执行以下迁移；IF NOT EXISTS 保证脚本可重复执行。
ALTER TABLE gmall_realtime.ads_metric_anomaly_analysis
ADD COLUMN IF NOT EXISTS error_message VARCHAR(2048) COMMENT '失败原因，便于排查和重试' AFTER status;
