package org.example.realtime.view.service;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import org.springframework.web.client.RestTemplate;

import java.sql.Timestamp;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/** 对确定性异常证据进行大模型解释，不让模型直接访问数据库或用户明细。 */
@Slf4j
@Service
public class AiAttributionService {
    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Value("${ai.attribution.enabled:false}")
    private boolean enabled;

    @Value("${ai.attribution.endpoint:}")
    private String endpoint;

    @Value("${ai.attribution.api-key:}")
    private String apiKey;

    @Value("${ai.attribution.model:}")
    private String model;

    @Value("${ai.attribution.batch-size:10}")
    private int batchSize;

    @Value("${ai.attribution.max-attempts:3}")
    private int maxAttempts;

    @Value("${ai.attribution.connect-timeout-ms:5000}")
    private int connectTimeoutMs;

    @Value("${ai.attribution.read-timeout-ms:30000}")
    private int readTimeoutMs;

    @Value("${ai.attribution.json-response-format-enabled:true}")
    private boolean jsonResponseFormatEnabled;

    private final Set<String> processingIds = Collections.synchronizedSet(new HashSet<String>());

    @Scheduled(fixedDelayString = "${ai.attribution.fixed-delay-ms:60000}")
    public void analyzePendingAnomalies() {
        if (!isAvailable()) {
            return;
        }
        int safeBatchSize = Math.max(1, Math.min(batchSize, 50));
        int safeMaxAttempts = Math.max(1, Math.min(maxAttempts, 10));
        String sql = "SELECT a.* FROM ads_metric_anomaly a " +
                "LEFT JOIN (" +
                "  SELECT anomaly_id, COUNT(1) attempt_count, " +
                "         SUM(CASE WHEN status = 'COMPLETED' THEN 1 ELSE 0 END) completed_count " +
                "  FROM ads_metric_anomaly_analysis GROUP BY anomaly_id" +
                ") x ON a.anomaly_id = x.anomaly_id " +
                "WHERE a.severity IN ('P1', 'P2') " +
                "AND IFNULL(x.completed_count, 0) = 0 " +
                "AND IFNULL(x.attempt_count, 0) < " + safeMaxAttempts + " " +
                "ORDER BY a.ts DESC LIMIT " + safeBatchSize;
        for (Map<String, Object> anomaly : jdbcTemplate.queryForList(sql)) {
            analyze(anomaly);
        }
    }

    public boolean analyzeNow(String anomalyId) {
        if (!isAvailable()) {
            throw new IllegalStateException("AI 归因未启用或配置不完整");
        }
        List<Map<String, Object>> rows = jdbcTemplate.queryForList(
                "SELECT * FROM ads_metric_anomaly WHERE anomaly_id = ? LIMIT 1", anomalyId);
        if (rows.isEmpty()) {
            return false;
        }
        analyze(rows.get(0));
        return true;
    }

    public boolean isAvailable() {
        return enabled && StringUtils.hasText(endpoint) && StringUtils.hasText(apiKey) && StringUtils.hasText(model);
    }

    private void analyze(Map<String, Object> anomaly) {
        String anomalyId = stringValue(anomaly, "anomaly_id");
        if (!StringUtils.hasText(anomalyId) || !processingIds.add(anomalyId)) {
            return;
        }
        JSONObject evidence = new JSONObject();
        try {
            evidence = buildEvidence(anomaly);
            JSONObject analysis = callModel(buildPrompt(evidence));
            insertResult(anomalyId, analysis.toJSONString(), evidence.toJSONString(), "COMPLETED", "");
        } catch (Exception e) {
            log.warn("AI 归因失败，anomalyId={}: {}", anomalyId, e.getMessage());
            insertResult(anomalyId, "{}", evidence.toJSONString(), "FAILED", e.getMessage());
        } finally {
            processingIds.remove(anomalyId);
        }
    }

    private void insertResult(String anomalyId, String analysis, String evidence, String status, String error) {
        jdbcTemplate.update(
                "INSERT INTO ads_metric_anomaly_analysis " +
                        "(analysis_id, anomaly_id, model_name, analysis_json, evidence_json, status, error_message, created_at) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                UUID.randomUUID().toString(), anomalyId, model, truncateUtf8(analysis, 8000),
                truncateUtf8(evidence, 8000), status, truncateUtf8(error, 2000),
                new Timestamp(System.currentTimeMillis()));
    }

    private JSONObject buildEvidence(Map<String, Object> anomaly) {
        JSONObject evidence = new JSONObject();
        evidence.put("anomaly", anomaly);
        evidence.put("dimensionEvidence", queryDimensionEvidence(anomaly));
        evidence.put("constraints", "仅根据给定聚合指标归因；无法确认时必须说明证据不足；不得编造系统状态或用户行为。");
        return evidence;
    }

    private List<Map<String, Object>> queryDimensionEvidence(Map<String, Object> anomaly) {
        String metricCode = stringValue(anomaly, "metric_code");
        Object stt = value(anomaly, "stt");
        Object edt = value(anomaly, "edt");
        if (stt == null || edt == null) {
            return new ArrayList<Map<String, Object>>();
        }
        if ("page_view_count".equals(metricCode)) {
            return jdbcTemplate.queryForList(
                    "SELECT vc, ch, ar, is_new, pv_ct FROM dws_traffic_vc_ch_ar_is_new_page_view_window " +
                            "WHERE stt = ? AND edt = ? ORDER BY pv_ct DESC LIMIT 10", stt, edt);
        }
        if ("order_user_count".equals(metricCode)) {
            return jdbcTemplate.queryForList(
                    "SELECT province_name, order_count, order_amount FROM dws_trade_province_order_window " +
                            "WHERE stt = ? AND edt = ? ORDER BY order_count DESC LIMIT 10", stt, edt);
        }
        if ("payment_success_user_count".equals(metricCode)) {
            return jdbcTemplate.queryForList(
                    "SELECT payment_suc_unique_user_count, payment_suc_new_user_count " +
                            "FROM dws_trade_payment_suc_window WHERE stt = ? AND edt = ? LIMIT 10", stt, edt);
        }
        return new ArrayList<Map<String, Object>>();
    }

    private String buildPrompt(JSONObject evidence) {
        return "你是电商实时数仓告警归因助手。只依据给定证据返回一个 JSON 对象，" +
                "字段必须包含 summary、probableCauses、recommendedActions。" +
                "probableCauses 必须是数组，每项包含 cause、confidence、evidence；" +
                "recommendedActions 必须是字符串数组。confidence 范围为 0 到 1。" +
                "证据不足时必须明确写出，不得把猜测陈述为事实。\n" + evidence.toJSONString();
    }

    private JSONObject callModel(String prompt) {
        JSONObject request = new JSONObject();
        request.put("model", model);
        request.put("temperature", 0.1D);
        if (jsonResponseFormatEnabled) {
            request.put("response_format", Collections.singletonMap("type", "json_object"));
        }
        JSONArray messages = new JSONArray();
        messages.add(message("system", "仅分析聚合业务指标，输出严格 JSON，不输出用户明细或敏感信息。"));
        messages.add(message("user", prompt));
        request.put("messages", messages);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setBearerAuth(apiKey);
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(connectTimeoutMs);
        factory.setReadTimeout(readTimeoutMs);
        ResponseEntity<String> response = new RestTemplate(factory).postForEntity(
                endpoint, new HttpEntity<String>(request.toJSONString(), headers), String.class);
        String responseBody = response.getBody();
        JSONObject body = JSONObject.parseObject(responseBody);
        JSONArray choices = body == null ? null : body.getJSONArray("choices");
        if (choices == null || choices.isEmpty()) {
            String text = body == null ? null : body.getString("text");
            if (StringUtils.hasText(text)) {
                return validateAnalysis(text);
            }
            String errorMessage = extractModelError(body);
            throw new IllegalStateException("模型响应中未包含 choices: " + errorMessage +
                    "; response=" + truncate(responseBody, 1000));
        }
        JSONObject message = choices.getJSONObject(0).getJSONObject("message");
        if (message == null) {
            throw new IllegalStateException("模型响应缺少 message: " + truncate(responseBody, 1000));
        }
        String content = message.getString("content");
        return validateAnalysis(content);
    }

    private String extractModelError(JSONObject body) {
        if (body == null) {
            return "响应体为空或不是 JSON";
        }
        JSONObject error = body.getJSONObject("error");
        if (error != null) {
            String code = error.getString("code");
            String message = error.getString("message");
            return (StringUtils.hasText(code) ? code + ": " : "") +
                    (StringUtils.hasText(message) ? message : error.toJSONString());
        }
        String code = body.getString("code");
        String message = body.getString("message");
        if (StringUtils.hasText(code) || StringUtils.hasText(message)) {
            return (StringUtils.hasText(code) ? code + ": " : "") +
                    (StringUtils.hasText(message) ? message : "未知错误");
        }
        return body.toJSONString();
    }

    private JSONObject validateAnalysis(String content) {
        if (!StringUtils.hasText(content)) {
            throw new IllegalStateException("模型返回了空内容");
        }
        String normalized = content.trim();
        if (normalized.startsWith("```")) {
            normalized = normalized.replaceFirst("^```(?:json)?\\s*", "").replaceFirst("\\s*```$", "");
        }
        JSONObject result = JSONObject.parseObject(normalized);
        if (!StringUtils.hasText(result.getString("summary")) ||
                result.getJSONArray("probableCauses") == null ||
                result.getJSONArray("recommendedActions") == null) {
            throw new IllegalStateException("模型结果缺少必需字段");
        }
        return result;
    }

    private JSONObject message(String role, String content) {
        JSONObject message = new JSONObject();
        message.put("role", role);
        message.put("content", content);
        return message;
    }

    private Object value(Map<String, Object> row, String key) {
        Object result = row.get(key);
        return result == null ? row.get(key.toUpperCase()) : result;
    }

    private String stringValue(Map<String, Object> row, String key) {
        Object result = value(row, key);
        return result == null ? "" : String.valueOf(result);
    }

    private String truncate(String value, int maxLength) {
        if (value == null) {
            return "";
        }
        return value.length() <= maxLength ? value : value.substring(0, maxLength);
    }

    private String truncateUtf8(String value, int maxBytes) {
        if (value == null || value.isEmpty()) {
            return "";
        }
        if (value.getBytes(StandardCharsets.UTF_8).length <= maxBytes) {
            return value;
        }
        StringBuilder result = new StringBuilder();
        int usedBytes = 0;
        for (int offset = 0; offset < value.length(); ) {
            int codePoint = value.codePointAt(offset);
            String character = new String(Character.toChars(codePoint));
            int characterBytes = character.getBytes(StandardCharsets.UTF_8).length;
            if (usedBytes + characterBytes > maxBytes) {
                break;
            }
            result.append(character);
            usedBytes += characterBytes;
            offset += Character.charCount(codePoint);
        }
        return result.toString();
    }
}
