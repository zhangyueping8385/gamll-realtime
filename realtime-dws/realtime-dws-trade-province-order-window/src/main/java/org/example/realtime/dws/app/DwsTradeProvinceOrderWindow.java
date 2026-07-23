package org.example.realtime.dws.app;

import com.alibaba.fastjson.JSONObject;
import io.debezium.data.Json;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.example.realtime.base.BaseAPP;
import org.example.realtime.bean.TradeProvinceOrderBean;
import org.example.realtime.bean.TradeSkuOrderBean;
import org.example.realtime.constant.Constant;
import org.example.realtime.function.DimAsyncFunction;
import org.example.realtime.function.DorisMapFunction;
import org.example.realtime.util.DateFormatUtil;
import org.example.realtime.util.FlinkSinkUtil;
import org.jline.utils.Log;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * 从Kafka读取订单明细数据，过滤null数据并按照唯一键对数据去重，统计各省份各窗口订单数和订单金额，
 * 将数据写入Doris交易域省份粒度下单各窗口汇总表。
 * 1）从Kafka下单明细主题读取数据
 * 2）过滤null数据并转换数据结构
 * 3）按照唯一键去重
 * 4）转换数据结构
 * JSONObject转换为实体类TradeProvinceOrderWindow。
 * 5）设置水位线
 * 6）按照省份ID分组
 * provinceId可以唯一标识数据。
 * 7）开窗
 * 8）聚合计算
 * 度量字段求和，并在窗口闭合后补充窗口起始时间、结束时间以及当前统计日期。
 * 9）关联省份信息
 * 补全省份名称字段。
 * 10）写出到Doris。
 */
public class DwsTradeProvinceOrderWindow extends BaseAPP {
    public static void main(String[] args) {
        new DwsTradeProvinceOrderWindow().Start(10030,4,Constant.DORIS_DWS_TRADE_PROVINCE_ORDER_WINDOW, Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);
    }


    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaSource) {
        //1.从Kafka下单明细主题读取数据
        kafkaSource.print();

        //2.过滤null数据并转换数据结构
        SingleOutputStreamOperator<JSONObject> flatMapStream = getFlatMapStream(kafkaSource);

        //3.注册水位线
        SingleOutputStreamOperator<JSONObject> watermarkStream = getTs(flatMapStream);

        //4.按照唯一键去重,转换为Bean
        KeyedStream<JSONObject, String> keyedStream = getId(watermarkStream);

        // 5. 去重
        SingleOutputStreamOperator<TradeProvinceOrderBean> mapBeanStream = getMapBeanStream(keyedStream);

        // 6. 开窗聚合
        SingleOutputStreamOperator<TradeProvinceOrderBean> reduceStream = getReduce(mapBeanStream);
//        reduceStream.print();

        //7.补全纬度信息
        SingleOutputStreamOperator<TradeProvinceOrderBean> asyncStream = getName(reduceStream);
        asyncStream.print("AsyncStream");

        //8.写出到Doris
        SingleOutputStreamOperator<String> mappedToSinkStream = asyncStream.map(new DorisMapFunction<>());

        mappedToSinkStream
                .sinkTo(FlinkSinkUtil.getDorisSink(Constant.DORIS_DWS_TRADE_PROVINCE_ORDER_WINDOW));


    }

    private static SingleOutputStreamOperator<TradeProvinceOrderBean> getName(SingleOutputStreamOperator<TradeProvinceOrderBean> reduceStream) {
        return AsyncDataStream.unorderedWait(reduceStream, new DimAsyncFunction<TradeProvinceOrderBean>() {
            @Override
            public String getId(TradeProvinceOrderBean tradeSkuOrderBean) {
                return tradeSkuOrderBean.getProvinceId();
            }

            @Override
            public String getTableName() {
                return "dim_base_province";
            }

            @Override
            public void join(TradeProvinceOrderBean tradeSkuOrderBean, JSONObject dim) {
                tradeSkuOrderBean.setProvinceName(dim.getString("name"));

            }
        }, 60, TimeUnit.SECONDS);
    }

    private static SingleOutputStreamOperator<TradeProvinceOrderBean> getReduce(SingleOutputStreamOperator<TradeProvinceOrderBean> mapBeanStream) {
        return mapBeanStream.keyBy(key -> key.getProvinceId())
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))
                .reduce((v1, v2) -> {
                    v1.setOrderAmount(v1.getOrderAmount().add(v2.getOrderAmount()));
                    v1.getOrderIdSet().addAll(v2.getOrderIdSet()); // 合并订单ID集合
                    return v1;
                }, new ProcessWindowFunction<TradeProvinceOrderBean, TradeProvinceOrderBean, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<TradeProvinceOrderBean, TradeProvinceOrderBean, String, TimeWindow>.Context context, Iterable<TradeProvinceOrderBean> iterable, Collector<TradeProvinceOrderBean> collector) throws Exception {
                        TimeWindow window = context.window();
                        String start = DateFormatUtil.tsToDateTime(window.getStart());
                        String end = DateFormatUtil.tsToDateTime(window.getEnd());
                        String curDt = DateFormatUtil.tsToDateForPartition(System.currentTimeMillis());
                        for (TradeProvinceOrderBean element : iterable) {
                            element.setStt(start);
                            element.setEdt(end);
                            element.setCurDate(curDt);
                            element.setOrderCount(Long.valueOf(element.getOrderIdSet().size()));
                            collector.collect(element);
                        }
                    }
                });
    }

    private static SingleOutputStreamOperator<TradeProvinceOrderBean> getMapBeanStream(KeyedStream<JSONObject, String> keyedStream) {
        return keyedStream.map(new RichMapFunction<JSONObject, TradeProvinceOrderBean>() {
           ValueState<BigDecimal> lastTotalAmountState;

           @Override
           public void open(Configuration parameters) throws Exception {
               ValueStateDescriptor<BigDecimal> lastTotalAmountDesc = new ValueStateDescriptor<>("last_total_amount", BigDecimal.class);
               lastTotalAmountDesc.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(30)).build());
               lastTotalAmountState = getRuntimeContext().getState(lastTotalAmountDesc);
           }

           @Override
           public TradeProvinceOrderBean map(JSONObject jsonObject) throws Exception {
               HashSet<String> hashSet = new HashSet<>();
               hashSet.add(jsonObject.getString("order_id"));

               BigDecimal lastTotalAmount = lastTotalAmountState.value();
               // 如果是第一个数据，lastTotalAmount为null，需要初始化为0
               lastTotalAmount = lastTotalAmount == null ? new BigDecimal("0") : lastTotalAmount;
               BigDecimal splitTotalAmount = jsonObject.getBigDecimal("split_total_amount");
               lastTotalAmountState.update(splitTotalAmount);

               return TradeProvinceOrderBean.builder()
                       .orderIdSet(hashSet)
                       .provinceId(jsonObject.getString("province_id"))
                       .orderDetailId(jsonObject.getString("id"))
                       .ts(jsonObject.getLong("ts"))
                       .orderAmount(splitTotalAmount.subtract(lastTotalAmount))
                       .build();
           }
       });
    }

    private static KeyedStream<JSONObject, String> getId(SingleOutputStreamOperator<JSONObject> watermarkStream) {
        return watermarkStream.keyBy(key -> key.getString("id"));
    }

    private static SingleOutputStreamOperator<JSONObject> getTs(SingleOutputStreamOperator<JSONObject> flatMapStream) {
        return flatMapStream.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(10)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
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
                    String id = jsonObject.getString("id");
                    String order_id = jsonObject.getString("order_id");
                    String province_id = jsonObject.getString("province_id");
                    long ts = jsonObject.getLong("ts");
                    if (id != null && order_id != null && province_id != null && ts != 0) {
                        jsonObject.put("ts", ts * 1000);
                        collector.collect(jsonObject);
                    }
                } catch (Exception e) {
                    Log.info("过滤异常数据", s);
                }
            }
        });
    }
}