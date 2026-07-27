package org.example.realtime.view.service;

import com.alibaba.fastjson.JSON;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** 为实时大屏提供异常事件、统计和 AI 归因结果。 */
@Service
public class AnomalyService {
    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private AiAttributionService aiAttributionService;

    public List<Map<String, Object>> getRecentAnomalies(int limit) {
        int safeLimit = Math.max(1, Math.min(limit, 100));
        String sql = "SELECT a.anomaly_id, a.metric_code, a.dimension_key, a.dimensions_json, a.stt, a.edt, " +
                "a.current_value, a.baseline_value, a.change_rate, a.z_score, a.severity, a.source_table, " +
                "a.evidence_json, a.created_at, x.analysis_status " +
                "FROM ads_metric_anomaly a " +
                "LEFT JOIN (" +
                "  SELECT anomaly_id, " +
                "         CASE WHEN SUM(CASE WHEN status = 'COMPLETED' THEN 1 ELSE 0 END) > 0 " +
                "              THEN 'COMPLETED' ELSE 'FAILED' END analysis_status " +
                "  FROM ads_metric_anomaly_analysis GROUP BY anomaly_id" +
                ") x ON a.anomaly_id = x.anomaly_id " +
                "ORDER BY a.ts DESC LIMIT " + safeLimit;
        List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql);
        for (Map<String, Object> row : rows) {
            parseJsonColumn(row, "dimensions_json");
            parseJsonColumn(row, "evidence_json");
        }
        return rows;
    }

    public Map<String, Object> getSummary() {
        Map<String, Object> result = new LinkedHashMap<String, Object>();
        result.put("total24h", queryLong(
                "SELECT COUNT(1) FROM ads_metric_anomaly WHERE created_at >= DATE_SUB(NOW(), INTERVAL 24 HOUR)"));
        result.put("p1Count24h", queryLong(
                "SELECT COUNT(1) FROM ads_metric_anomaly WHERE severity = 'P1' " +
                        "AND created_at >= DATE_SUB(NOW(), INTERVAL 24 HOUR)"));
        result.put("pendingAnalysis", queryLong(
                "SELECT COUNT(1) FROM ads_metric_anomaly a " +
                        "LEFT JOIN (SELECT DISTINCT anomaly_id FROM ads_metric_anomaly_analysis " +
                        "           WHERE status = 'COMPLETED') b " +
                        "ON a.anomaly_id = b.anomaly_id WHERE b.anomaly_id IS NULL"));
        result.put("aiAvailable", aiAttributionService.isAvailable());
        return result;
    }

    public Map<String, Object> getLatestAnalysis(String anomalyId) {
        List<Map<String, Object>> rows = jdbcTemplate.queryForList(
                "SELECT analysis_id, anomaly_id, model_name, analysis_json, evidence_json, status, " +
                        "error_message, created_at FROM ads_metric_anomaly_analysis " +
                        "WHERE anomaly_id = ? ORDER BY created_at DESC LIMIT 1", anomalyId);
        if (rows.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, Object> row = rows.get(0);
        parseJsonColumn(row, "analysis_json");
        parseJsonColumn(row, "evidence_json");
        return row;
    }

    public boolean analyzeNow(String anomalyId) {
        return aiAttributionService.analyzeNow(anomalyId);
    }

    private long queryLong(String sql) {
        Long value = jdbcTemplate.queryForObject(sql, Long.class);
        return value == null ? 0L : value;
    }

    private void parseJsonColumn(Map<String, Object> row, String column) {
        Object value = row.get(column);
        String actualKey = column;
        if (value == null) {
            actualKey = column.toUpperCase();
            value = row.get(actualKey);
        }
        if (value instanceof String && !((String) value).trim().isEmpty()) {
            try {
                row.put(actualKey, JSON.parse((String) value));
            } catch (Exception ignored) {
                // 历史脏数据按原字符串返回。
            }
        }
    }
}
