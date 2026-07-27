package org.example.realtime.dws.app;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.example.realtime.base.BaseAPP;
import org.example.realtime.bean.MetricAnomalyEvent;
import org.example.realtime.bean.MetricBaselineState;
import org.example.realtime.bean.MetricWindowEvent;
import org.example.realtime.constant.Constant;
import org.example.realtime.function.DorisMapFunction;
import org.example.realtime.util.DateFormatUtil;
import org.example.realtime.util.FlinkSinkUtil;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * 消费 DWS 统一指标事件并进行在线异常检测。
 * 检测策略：最少样本数 + 相对变化率 + Z-Score；规则命中后同时写入 Doris 和 Kafka。
 */

/**
 {"metricCode": "order_user_count","dimensionKey": "TEST","dimensionsJson": "{\"test\":true}","stt": "2026-07-27 16:02:00","edt": "2026-07-27 16:02:10","curDate": "2026-07-27","value": 99,"sourceTable": "dws_trade_order_window","ts": 1785139330000}
 {"metricCode": "order_user_count","dimensionKey": "TEST","dimensionsJson": "{\"test\":true}","stt": "2026-07-27 16:02:00","edt": "2026-07-27 16:02:10","curDate": "2026-07-27","value": 98,"sourceTable": "dws_trade_order_window","ts": 1785139330000}
 {"metricCode": "order_user_count","dimensionKey": "TEST","dimensionsJson": "{\"test\":true}","stt": "2026-07-27 16:02:00","edt": "2026-07-27 16:02:10","curDate": "2026-07-27","value": 97,"sourceTable": "dws_trade_order_window","ts": 1785139330000}
 {"metricCode": "order_user_count","dimensionKey": "TEST","dimensionsJson": "{\"test\":true}","stt": "2026-07-27 16:02:00","edt": "2026-07-27 16:02:10","curDate": "2026-07-27","value": 100,"sourceTable": "dws_trade_order_window","ts": 1785139330000}
 {"metricCode": "order_user_count","dimensionKey": "TEST","dimensionsJson": "{\"test\":true}","stt": "2026-07-27 16:02:00","edt": "2026-07-27 16:02:10","curDate": "2026-07-27","value": 102,"sourceTable": "dws_trade_order_window","ts": 1785139330000}
 {"metricCode": "order_user_count","dimensionKey": "TEST","dimensionsJson": "{\"test\":true}","stt": "2026-07-27 16:02:00","edt": "2026-07-27 16:02:10","curDate": "2026-07-27","value": 40,"sourceTable": "dws_trade_order_window","ts": 1785139330000}
 {"metricCode": "order_user_count","dimensionKey": "TEST","dimensionsJson": "{\"test\":true}","stt": "2026-07-27 16:02:00","edt": "2026-07-27 16:02:10","curDate": "2026-07-27","value": 99,"sourceTable": "dws_trade_order_window","ts": 1785139330000}
 {"metricCode": "order_user_count","dimensionKey": "TEST","dimensionsJson": "{\"test\":true}","stt": "2026-07-27 16:02:00","edt": "2026-07-27 16:02:10","curDate": "2026-07-27","value": 79,"sourceTable": "dws_trade_order_window","ts": 1785139330000}
 {"metricCode": "order_user_count","dimensionKey": "TEST","dimensionsJson": "{\"test\":true}","stt": "2026-07-27 16:02:00","edt": "2026-07-27 16:02:10","curDate": "2026-07-27","value": 99,"sourceTable": "dws_trade_order_window","ts": 1785139330000}
 {"metricCode": "order_user_count","dimensionKey": "TEST","dimensionsJson": "{\"test\":true}","stt": "2026-07-27 16:02:00","edt": "2026-07-27 16:02:10","curDate": "2026-07-27","value": 200,"sourceTable": "dws_trade_order_window","ts": 1785139330000}
 {"metricCode": "order_user_count","dimensionKey": "TEST","dimensionsJson": "{\"test\":true}","stt": "2026-07-27 16:02:00","edt": "2026-07-27 16:02:10","curDate": "2026-07-27","value": 2000,"sourceTable": "dws_trade_order_window","ts": 1785139330000}
 {"metricCode": "order_user_count","dimensionKey": "TEST","dimensionsJson": "{\"test\":true}","stt": "2026-07-27 16:02:00","edt": "2026-07-27 16:02:10","curDate": "2026-07-27","value": 500,"sourceTable": "dws_trade_order_window","ts": 1785139330000}
 **/
public class DwsMetricAnomalyWindow extends BaseAPP {
    private static final int MIN_SAMPLE_COUNT = 12;
    private static final int MAX_BASELINE_SAMPLES = 60;
    private static final double MIN_CHANGE_RATE = 0.35D;
    private static final double MIN_Z_SCORE = 3D;
    private static final long ANOMALY_COOLDOWN_MS = 5 * 60 * 1000L;

    public static void main(String[] args) {
        new DwsMetricAnomalyWindow().Start(
                10031,
                1,
                "dws_metric_anomaly_window",
                Constant.TOPIC_DWS_METRIC_WINDOW
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaSource) {
        kafkaSource.print();
        SingleOutputStreamOperator<MetricWindowEvent> metricStream = kafkaSource
                .flatMap(new FlatMapFunction<String, MetricWindowEvent>() {
                    @Override
                    public void flatMap(String value, Collector<MetricWindowEvent> out) {
                        try {
                            MetricWindowEvent event = JSONObject.parseObject(value, MetricWindowEvent.class);
                            if (event != null && event.getMetricCode() != null && event.getValue() != null) {
                                out.collect(event);
                            }
                        } catch (Exception ignored) {
                            // 脏数据不影响异常检测主链路。
                        }
                    }
                });

        KeyedStream<MetricWindowEvent, String> keyedMetricStream = metricStream.keyBy(
                new KeySelector<MetricWindowEvent, String>() {
                    @Override
                    public String getKey(MetricWindowEvent value) {
                        return value.getMetricCode() + "|" + value.getDimensionKey();
                    }
                }
        );

        SingleOutputStreamOperator<MetricAnomalyEvent> anomalyStream = keyedMetricStream
                .process(new MetricAnomalyDetector());

        SingleOutputStreamOperator<String> dorisStream = anomalyStream.map(new DorisMapFunction<MetricAnomalyEvent>());
        dorisStream.print();

        dorisStream.sinkTo(FlinkSinkUtil.getDorisSink(Constant.DORIS_ADS_METRIC_ANOMALY));
        dorisStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_ADS_METRIC_ANOMALY));
    }

    private static class MetricAnomalyDetector
            extends KeyedProcessFunction<String, MetricWindowEvent, MetricAnomalyEvent> {
        private transient ValueState<MetricBaselineState> baselineState;

        @Override
        public void open(Configuration parameters) {
            baselineState = getRuntimeContext().getState(
                    new ValueStateDescriptor<MetricBaselineState>("metric-baseline", MetricBaselineState.class)
            );
        }

        @Override
        public void processElement(MetricWindowEvent event,
                                   Context context,
                                   Collector<MetricAnomalyEvent> out) throws Exception {
            MetricBaselineState baseline = baselineState.value();
            if (baseline == null) {
                baseline = new MetricBaselineState();
            }

            long currentValue = event.getValue();
            long sampleCount = baseline.getSampleCount();
            double baselineValue = baseline.getMean();
            double standardDeviation = baseline.getStandardDeviation();

            boolean anomalyDetected = false;
            long eventTimestamp = event.getTs() == null ? System.currentTimeMillis() : event.getTs();
            if (sampleCount >= MIN_SAMPLE_COUNT && baselineValue > 0D) {
                double changeRate = (currentValue - baselineValue) / baselineValue;
                double zScore = standardDeviation == 0D
                        ? 0D
                        : (currentValue - baselineValue) / standardDeviation;
                boolean largeChange = Math.abs(changeRate) >= MIN_CHANGE_RATE;
                boolean statisticallyUnusual = standardDeviation == 0D || Math.abs(zScore) >= MIN_Z_SCORE;

                boolean cooldownElapsed = eventTimestamp - baseline.getLastAnomalyTimestamp() >= ANOMALY_COOLDOWN_MS;
                if (largeChange && statisticallyUnusual && cooldownElapsed) {
                    String anomalyKey = event.getMetricCode() + "|" + event.getDimensionKey() + "|" + event.getStt();
                    JSONObject evidence = new JSONObject();
                    evidence.put("sampleCount", sampleCount);
                    evidence.put("standardDeviation", standardDeviation);
                    evidence.put("rule", "abs(changeRate)>=0.35 && (stddev=0 || abs(zScore)>=3)");

                    out.collect(MetricAnomalyEvent.builder()
                            .anomalyId(UUID.nameUUIDFromBytes(anomalyKey.getBytes(StandardCharsets.UTF_8)).toString())
                            .metricCode(event.getMetricCode())
                            .dimensionKey(event.getDimensionKey())
                            .dimensionsJson(event.getDimensionsJson())
                            .stt(event.getStt())
                            .edt(event.getEdt())
                            .curDate(event.getCurDate())
                            .currentValue(currentValue)
                            .baselineValue(baselineValue)
                            .changeRate(changeRate)
                            .zScore(zScore)
                            .severity(Math.abs(changeRate) >= 0.60D ? "P1" : "P2")
                            .sourceTable(event.getSourceTable())
                            .evidenceJson(evidence.toJSONString())
                            .createdAt(DateFormatUtil.tsToDateTime(System.currentTimeMillis()))
                            .ts(System.currentTimeMillis())
                            .build());
                    baseline.setLastAnomalyTimestamp(eventTimestamp);
                    anomalyDetected = true;
                }
            }

            // 异常点不进入基线，防止突刺把后续基线整体拉偏。
            if (!anomalyDetected) {
                baseline.add(currentValue, MAX_BASELINE_SAMPLES);
            }
            baselineState.update(baseline);
        }
    }
}
