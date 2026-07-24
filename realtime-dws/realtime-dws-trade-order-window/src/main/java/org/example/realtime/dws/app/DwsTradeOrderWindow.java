package org.example.realtime.dws.app;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.realtime.base.BaseAPP;
import org.example.realtime.bean.TradeOrderBean;
import org.example.realtime.constant.Constant;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.example.realtime.function.DorisMapFunction;
import org.example.realtime.util.DateFormatUtil;
import org.example.realtime.util.FlinkSinkUtil;
import org.jline.utils.Log;

import java.time.Duration;

/**
 * 从 Kafka订单明细主题读取数据，统计当日下单独立用户数和首次下单用户数，封装为实体类，写入Doris。
 * 1）从 Kafka订单明细主题读取数据
 * 2）转换数据结构
 * Kafka 订单明细主题的数据是通过 Kafka-Connector 从订单预处理主题读取后进行过滤获取的，Kafka-Connector 会过滤掉主题中的 null 数据，因此订单明细主题不存在为 null 的数据，直接转换数据结构即可。
 * 3）设置水位线
 * 4）按照用户 id 分组
 * 5）计算度量字段的值
 * 运用 Flink 状态编程，在状态中维护用户末次下单日期。
 * 若末次下单日期为 null，则将首次下单用户数和下单独立用户数均置为 1；否则首次下单用户数置为 0，判断末次下单日期是否为当日，如果不是当日则下单独立用户数置为 1，否则置为 0。最后将状态中的下单日期更新为当日。
 * 6）开窗、聚合
 * 度量字段求和，补充窗口起始时间和结束时间字段，ts 字段置为当前系统时间戳。
 * 7）写出到Doris。
 */
public class DwsTradeOrderWindow extends BaseAPP {
    public static void main(String[] args) {
        new DwsTradeOrderWindow().Start(10028,4, Constant.DORIS_DWS_TRADE_ORDER_WINDOW,Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaSource) {
        //1.转换数据结构
        SingleOutputStreamOperator<JSONObject> mapStream = getMap(kafkaSource);

        //2.设置水位线
        SingleOutputStreamOperator<JSONObject> watermarkStream = getTs(mapStream);

        //3.按照用户 id 分组
        KeyedStream<JSONObject, String> keyedStream = getUserId(watermarkStream);

        //4.计算度量字段的值
        SingleOutputStreamOperator<TradeOrderBean> processStream = getProcessStream(keyedStream);
//        processStream.print();

        //5.开窗、聚合
        SingleOutputStreamOperator<TradeOrderBean> reduceStream = getReduce(processStream);

        // 6.转换数据结构
        SingleOutputStreamOperator<String> mappedStream = reduceStream.map(new DorisMapFunction<>());
        mappedStream.print();

        // 7.写入 Doris
        mappedStream.sinkTo(FlinkSinkUtil.getDorisSink(Constant.DORIS_DWS_TRADE_ORDER_WINDOW));
    }

    private static SingleOutputStreamOperator<TradeOrderBean> getReduce(SingleOutputStreamOperator<TradeOrderBean> processStream) {
        return processStream
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5L)))
                .reduce(
                        new ReduceFunction<TradeOrderBean>() {
                            @Override
                            public TradeOrderBean reduce(TradeOrderBean value1,
                                                         TradeOrderBean value2) {
                                value1.setOrderUniqueUserCount(value1.getOrderUniqueUserCount() + value2.getOrderUniqueUserCount());
                                value1.setOrderNewUserCount(value1.getOrderNewUserCount() + value2.getOrderNewUserCount());
                                return value1;
                            }
                        },
                        new ProcessAllWindowFunction<TradeOrderBean, TradeOrderBean, TimeWindow>() {
                            @Override
                            public void process(Context ctx,
                                                Iterable<TradeOrderBean> elements,
                                                Collector<TradeOrderBean> out) throws Exception {
                                TradeOrderBean bean = elements.iterator().next();
                                bean.setStt(DateFormatUtil.tsToDateTime(ctx.window().getStart()));
                                bean.setEdt(DateFormatUtil.tsToDateTime(ctx.window().getEnd()));

                                bean.setCurDate(DateFormatUtil.tsToDateForPartition(System.currentTimeMillis()));

                                out.collect(bean);
                            }
                        }
                );
    }

    private static SingleOutputStreamOperator<TradeOrderBean> getProcessStream(KeyedStream<JSONObject, String> keyedStream) {
        return keyedStream
               .process(new KeyedProcessFunction<String, JSONObject, TradeOrderBean>() {

                   private ValueState<String> lastOrderDateState;

                   @Override
                   public void open(Configuration parameters) {
                       lastOrderDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("lastOrderDate", String.class));
                   }

                   @Override
                   public void processElement(JSONObject value,
                                              Context ctx,
                                              Collector<TradeOrderBean> out) throws Exception {
                       long ts = value.getLong("ts");

                       String today = DateFormatUtil.tsToDate(ts);
                       String lastOrderDate = lastOrderDateState.value();

                       long orderUu = 0L;
                       long orderNew = 0L;
                       if (!today.equals(lastOrderDate)) {
                           orderUu = 1L;
                           lastOrderDateState.update(today);

                           if (lastOrderDate == null) {
                               orderNew = 1L;
                           }

                       }
                       if (orderUu == 1) {
                           out.collect(new TradeOrderBean("", "", "", orderUu, orderNew, ts));
                       }
                   }
               });
    }

    private static KeyedStream<JSONObject, String> getUserId(SingleOutputStreamOperator<JSONObject> watermarkStream) {
        return watermarkStream.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject jsonObject) throws Exception {
                return jsonObject.getString("user_id");
            }
        });
    }

    private static SingleOutputStreamOperator<JSONObject> getTs(SingleOutputStreamOperator<JSONObject> mapStream) {
        return mapStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(10L))
                                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject jsonObject, long l) {
                                        return jsonObject.getLong("ts");
                                    }
                                })
                );
    }

    private static SingleOutputStreamOperator<JSONObject> getMap(DataStreamSource<String> kafkaSource) {
        return kafkaSource.flatMap(new FlatMapFunction<String, JSONObject>() {

            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(s);
                    String user_id = jsonObject.getString("user_id");
                    Long ts = jsonObject.getLong("ts");
                    if (user_id != null && ts != null) {
                        jsonObject.put("ts", ts * 1000);
                        collector.collect(jsonObject);
                    }
                } catch (Exception e) {
                    Log.info("转换数据结构为 JSONObject 失败，数据：" + s);
                }
            }
        });
    }
}
