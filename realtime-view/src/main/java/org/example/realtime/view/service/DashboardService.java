package org.example.realtime.view.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class DashboardService {

    @Autowired
    private DorisQueryService dorisQueryService;

    public Map<String, Object> getOverallData() {
        try {
            Long totalUsers = dorisQueryService.executeQueryForObject("SELECT SUM(register_ct) FROM dws_user_user_register_window", Long.class);
            Long totalOrders = dorisQueryService.executeQueryForObject("SELECT SUM(order_unique_user_count) FROM dws_trade_order_window", Long.class);
            BigDecimal totalAmount = dorisQueryService.executeQueryForObject("SELECT SUM(order_amount) FROM dws_trade_province_order_window", BigDecimal.class);
            Long totalPV = dorisQueryService.executeQueryForObject("SELECT SUM(pv_ct) FROM dws_traffic_vc_ch_ar_is_new_page_view_window", Long.class);
            Long totalPayments = dorisQueryService.executeQueryForObject("SELECT SUM(payment_suc_unique_user_count) FROM dws_trade_payment_suc_window", Long.class);
            Long totalCartAdds = dorisQueryService.executeQueryForObject("SELECT SUM(cart_add_uu_ct) FROM dws_trade_cart_add_uu_window", Long.class);

            Map<String, Object> result = new HashMap<>();
            result.put("totalUsers", totalUsers != null ? totalUsers : 0L);
            result.put("totalOrders", totalOrders != null ? totalOrders : 0L);
            result.put("totalAmount", totalAmount != null ? totalAmount : BigDecimal.ZERO);
            result.put("totalPV", totalPV != null ? totalPV : 0L);
            result.put("totalPayments", totalPayments != null ? totalPayments : 0L);
            result.put("totalCartAdds", totalCartAdds != null ? totalCartAdds : 0L);

            return result;
        } catch (Exception e) {
            log.error("获取实时总览指标失败: {}", e.getMessage());
            throw new RuntimeException("获取实时总览指标失败", e);
        }
    }

    public List<Map<String, Object>> getTrendData(String metric, String timeRange) {
        try {
            String tableName, valueField;
            switch (metric) {
                case "user_register":
                    tableName = "dws_user_user_register_window";
                    valueField = "register_ct";
                    break;
                case "order_count":
                    tableName = "dws_trade_order_window";
                    valueField = "order_unique_user_count";
                    break;
                case "page_view":
                    tableName = "dws_traffic_vc_ch_ar_is_new_page_view_window";
                    valueField = "pv_ct";
                    break;
                default:
                    throw new IllegalArgumentException("不支持的指标类型: " + metric);
            }
            // Anchor the range to the latest warehouse window so historical/demo data still renders.
            // DWS tables contain multiple dimension rows per window, so aggregate them by stt.
            String sql = String.format(
                    "SELECT stt, SUM(%s) as value FROM %s " +
                            "WHERE stt >= DATE_SUB((SELECT MAX(stt) FROM %s), INTERVAL %s) " +
                            "GROUP BY stt ORDER BY stt",
                    valueField, tableName, tableName, timeRange);
            return dorisQueryService.executeQuery(sql);
        } catch (Exception e) {
            log.error("获取趋势数据失败: {}", e.getMessage());
            throw new RuntimeException("获取趋势数据失败", e);
        }
    }

    public List<Map<String, Object>> getTopNData(String tableName, String groupBy, String aggField, String aggFunc, int topN) {
        try {
            String whereClause = "";


            String sql = String.format("SELECT %s as name, %s(%s) as value FROM %s %s GROUP BY %s ORDER BY value DESC LIMIT %d",
                    groupBy, aggFunc, aggField, tableName, whereClause, groupBy, topN);
            return dorisQueryService.executeQuery(sql);
        } catch (Exception e) {
            log.error("获取TopN数据失败: {}", e.getMessage());
            throw new RuntimeException("获取TopN数据失败", e);
        }
    }

    public List<Map<String, Object>> getDistributionData(String tableName, String groupBy, String valueField) {
        try {
            String sql = String.format("SELECT %s as name, SUM(%s) as value FROM %s GROUP BY %s ORDER BY value DESC", groupBy, valueField, tableName, groupBy);
            return dorisQueryService.executeQuery(sql);
        } catch (Exception e) {
            log.error("获取分布数据失败: {}", e.getMessage());
            throw new RuntimeException("获取分布数据失败", e);
        }
    }

    public List<Map<String, Object>> getMapData() {
        try {
            String sql = "SELECT province_name as name, SUM(order_count) as value FROM dws_trade_province_order_window GROUP BY province_name";
            return dorisQueryService.executeQuery(sql);
        } catch (Exception e) {
            log.error("获取地图数据失败: {}", e.getMessage());
            throw new RuntimeException("获取地图数据失败", e);
        }
    }

    public Map<String, Object> getPaymentSuccessRate() {
        try {
            Long totalOrders = dorisQueryService.executeQueryForObject("SELECT SUM(order_unique_user_count) FROM dws_trade_order_window", Long.class);
            Long totalPayments = dorisQueryService.executeQueryForObject("SELECT SUM(payment_suc_unique_user_count) FROM dws_trade_payment_suc_window", Long.class);
            BigDecimal rate = BigDecimal.ZERO;
            if (totalOrders != null && totalOrders > 0 && totalPayments != null) {
                rate = BigDecimal.valueOf(totalPayments).divide(BigDecimal.valueOf(totalOrders), 4, RoundingMode.HALF_UP).multiply(BigDecimal.valueOf(100));
            }
            Map<String, Object> result = new HashMap<>();
            result.put("rate", rate);
            return result;
        } catch (Exception e) {
            log.error("获取支付成功率失败: {}", e.getMessage());
            throw new RuntimeException("获取支付成功率失败", e);
        }
    }
}
