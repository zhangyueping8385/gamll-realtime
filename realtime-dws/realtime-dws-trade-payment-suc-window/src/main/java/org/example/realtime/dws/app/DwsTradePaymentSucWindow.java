package org.example.realtime.dws.app;

import com.alibaba.fastjson.JSONObject;
import com.google.gson.JsonObject;
import io.debezium.data.Json;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.example.realtime.base.BaseAPP;
import org.example.realtime.bean.CartAddUuBean;
import org.example.realtime.bean.TradePaymentBean;
import org.example.realtime.constant.Constant;
import org.example.realtime.function.DorisMapFunction;
import org.example.realtime.util.DateFormatUtil;
import org.example.realtime.util.FlinkSinkUtil;
import org.jline.utils.Log;

import java.time.Duration;

import static org.apache.flink.api.common.state.StateTtlConfig.UpdateType.OnCreateAndWrite;


/**
 * 从Kafka读取交易域支付成功主题数据，统计支付成功独立用户数和首次支付成功用户数。
 * 1）从 Kafka支付成功明细主题读取数据
 * 2）转换数据结构
 * String 转换为 JSONObject。
 * 3）设置水位线
 * 4）按照 user_id 分组
 * 5）统计独立支付人数和新增支付人数
 * 运用 Flink 状态编程，在状态中维护用户末次支付日期。
 * 若末次支付日期为 null，则将首次支付用户数和支付独立用户数均置为 1；否则首次支付用户数置为 0，判断末次支付日期是否为当日，如果不是当日则支付独立用户数置为 1，否则置为 0。最后将状态中的支付日期更新为当日。
 * 6）开窗、聚合
 * 度量字段求和，补充窗口起始时间和结束时间字段，ts 字段置为当前系统时间戳。
 * 7）写出到Doris
 */
public class DwsTradePaymentSucWindow extends BaseAPP {
    public static void main(String[] args) {
        new DwsTradePaymentSucWindow().Start(
                10027,
                4,
                Constant.DORIS_DWS_TRADE_PAYMENT_SUC_WINDOW,
                Constant.TOPIC_DWD_TRADE_ORDER_PAY_SUC_DETAIL
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaSource) {

        //1. 转换数据结构为 JSONObject
        SingleOutputStreamOperator<JSONObject> flatMapStream = getFlatMapStream(kafkaSource);

        //2. 设置水位线
        SingleOutputStreamOperator<JSONObject> watermarkStream = getTs(flatMapStream);

        //3. 按照 user_id 分组
        KeyedStream<JSONObject, String> keyedStream = getUserId(watermarkStream);

        //4. 统计独立支付人数和新增支付人数
        SingleOutputStreamOperator<TradePaymentBean> processStream = getProcessStream(keyedStream);

        //5. 开窗、聚合
        SingleOutputStreamOperator<TradePaymentBean> windowAndReduceStream = getReduce(processStream);

        //6.转换数据结构
        SingleOutputStreamOperator<String> mappedStream = windowAndReduceStream.map(new DorisMapFunction<>());
        mappedStream.print();

        //7. 写出到Doris
        mappedStream.sinkTo(FlinkSinkUtil.getDorisSink(Constant.DORIS_DWS_TRADE_PAYMENT_SUC_WINDOW));

    }

    private static SingleOutputStreamOperator<TradePaymentBean> getReduce(SingleOutputStreamOperator<TradePaymentBean> processStream) {
        return processStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<TradePaymentBean>() {
                    @Override
                    public TradePaymentBean reduce(TradePaymentBean v1, TradePaymentBean v2) throws Exception {
                        v1.setPaymentSucUniqueUserCount(v1.getPaymentSucUniqueUserCount() + v2.getPaymentSucUniqueUserCount());
                        v1.setPaymentSucNewUserCount(v1.getPaymentSucNewUserCount() + v2.getPaymentSucNewUserCount());
                        return v1;
                    }
                }, new ProcessAllWindowFunction<TradePaymentBean, TradePaymentBean, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<TradePaymentBean, TradePaymentBean, TimeWindow>.Context context, Iterable<TradePaymentBean> iterable, Collector<TradePaymentBean> collector) throws Exception {
                        String start = DateFormatUtil.tsToDateTime(context.window().getStart());
                        String end = DateFormatUtil.tsToDateTime(context.window().getEnd());
                        String ts = DateFormatUtil.tsToDateForPartition(System.currentTimeMillis());
                        for (TradePaymentBean element : iterable) {
                            element.setStt(start);
                            element.setEdt(end);
                            element.setCurDate(ts);
                            collector.collect(element);
                        }
                    }
                });
    }

    private static SingleOutputStreamOperator<TradePaymentBean> getProcessStream(KeyedStream<JSONObject, String> keyedStream) {
        return keyedStream.process(new ProcessFunction<JSONObject, TradePaymentBean>() {
            ValueState<String> lastPayDateState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> lastPayDateDesc = new ValueStateDescriptor<>("lastPayDate", String.class);
                lastPayDateDesc.enableTimeToLive(StateTtlConfig.newBuilder(org.apache.flink.api.common.time.Time.days(1))
                        .build());
                lastPayDateState = getRuntimeContext().getState(lastPayDateDesc);

            }

            @Override
            public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, TradePaymentBean>.Context context, Collector<TradePaymentBean> collector) throws Exception {
                String lastPayDt = lastPayDateState.value();
                Long ts = jsonObject.getLong("ts");
                String curDt = DateFormatUtil.tsToDate(ts);
                // 支付成功独立用户数
                long paymentSucUniqueUserCount = 0L;
                // 支付成功新用户数
                long paymentSucNewUserCount = 0L;

                //  若末次支付日期为 null，则将首次支付用户数和支付独立用户数均置为 1；否则首次支付用户数置为 0，判断末次支付日期是否为当日，如果不是当日则支付独立用户数置为 1，否则置为 0。最后将状态中的支付日期更新为当日。
                if (lastPayDt == null) {
                    lastPayDateState.update(curDt);
                    paymentSucNewUserCount = 1L;
                    paymentSucUniqueUserCount = 1L;
                }

                if (!curDt.equals(lastPayDt)) {
                    lastPayDateState.update(curDt);
                    paymentSucUniqueUserCount = 1L;
                }

                if (paymentSucUniqueUserCount == 1) {
                    collector.collect(new TradePaymentBean("", "", "", paymentSucUniqueUserCount, paymentSucNewUserCount, ts));
                }
            }
        });
    }

    private static KeyedStream<JSONObject, String> getUserId(SingleOutputStreamOperator<JSONObject> watermarkStream) {
        return watermarkStream.keyBy(jsonObject -> jsonObject.getString("user_id"));
    }

    private static SingleOutputStreamOperator<JSONObject> getTs(SingleOutputStreamOperator<JSONObject> flatMapStream) {
        return flatMapStream.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(10L)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject jsonObject, long l) {
                return jsonObject.getLong("ts");
            }
        }));
    }

    private static SingleOutputStreamOperator<JSONObject> getFlatMapStream(DataStreamSource<String> kafkaSource) {
        return kafkaSource.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(s);
                    String user_id = jsonObject.getString("user_id");
                    String payment_time = jsonObject.getString("payment_time");
                    if (user_id != null && payment_time != null) {
                        jsonObject.put("ts", jsonObject.getLong("ts"));
                        collector.collect(jsonObject);
                    }
                } catch (Exception e) {
                    Log.info("转换数据结构为 JSONObject 失败，数据：" + s);
                }

            }
        });
    }
}
