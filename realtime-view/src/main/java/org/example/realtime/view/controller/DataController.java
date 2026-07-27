package org.example.realtime.view.controller;

import lombok.extern.slf4j.Slf4j;
import org.example.realtime.view.service.DashboardService;
import org.example.realtime.view.service.AnomalyService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api")
@CrossOrigin(origins = "*")
public class DataController {

    @Autowired
    private DashboardService dashboardService;

    @Autowired
    private AnomalyService anomalyService;

    @GetMapping("/dashboard/overall")
    public ResponseEntity<Map<String, Object>> getOverallData() {
        return ResponseEntity.ok(dashboardService.getOverallData());
    }

    @GetMapping("/dashboard/trend")
    public ResponseEntity<List<Map<String, Object>>> getTrendData(@RequestParam String metric,
                                                                    @RequestParam(defaultValue = "24 HOUR") String timeRange) {
        return ResponseEntity.ok(dashboardService.getTrendData(metric, timeRange));
    }

    @GetMapping("/charts/topN")
    public ResponseEntity<List<Map<String, Object>>> getTopNData(@RequestParam String tableName,
                                                                 @RequestParam String groupBy,
                                                                 @RequestParam String aggField,
                                                                 @RequestParam String aggFunc,
                                                                 @RequestParam(defaultValue = "10") int topN) {
        return ResponseEntity.ok(dashboardService.getTopNData(tableName, groupBy, aggField, aggFunc, topN));
    }

    @GetMapping("/charts/distribution")
    public ResponseEntity<List<Map<String, Object>>> getDistributionData(@RequestParam String tableName,
                                                                           @RequestParam String groupBy,
                                                                           @RequestParam String valueField) {
        return ResponseEntity.ok(dashboardService.getDistributionData(tableName, groupBy, valueField));
    }

    @GetMapping("/dashboard/map")
    public ResponseEntity<List<Map<String, Object>>> getMapData() {
        return ResponseEntity.ok(dashboardService.getMapData());
    }

    @GetMapping("/dashboard/payment-success-rate")
    public ResponseEntity<Map<String, Object>> getPaymentSuccessRate() {
        return ResponseEntity.ok(dashboardService.getPaymentSuccessRate());
    }

    @GetMapping("/anomalies")
    public ResponseEntity<List<Map<String, Object>>> getRecentAnomalies(
            @RequestParam(defaultValue = "20") int limit) {
        return ResponseEntity.ok(anomalyService.getRecentAnomalies(limit));
    }

    @GetMapping("/anomalies/summary")
    public ResponseEntity<Map<String, Object>> getAnomalySummary() {
        return ResponseEntity.ok(anomalyService.getSummary());
    }

    @GetMapping("/anomalies/{anomalyId}/analysis")
    public ResponseEntity<Map<String, Object>> getLatestAnomalyAnalysis(@PathVariable String anomalyId) {
        return ResponseEntity.ok(anomalyService.getLatestAnalysis(anomalyId));
    }

    @PostMapping("/anomalies/{anomalyId}/analyze")
    public ResponseEntity<Map<String, Object>> analyzeAnomalyNow(@PathVariable String anomalyId) {
        boolean accepted = anomalyService.analyzeNow(anomalyId);
        Map<String, Object> result = new java.util.HashMap<String, Object>();
        result.put("accepted", accepted);
        result.put("anomalyId", anomalyId);
        return accepted ? ResponseEntity.accepted().body(result) : ResponseEntity.notFound().build();
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<String> handleException(Exception e) {
        log.error("API Error: ", e);
        return new ResponseEntity<>(e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
    }
}
