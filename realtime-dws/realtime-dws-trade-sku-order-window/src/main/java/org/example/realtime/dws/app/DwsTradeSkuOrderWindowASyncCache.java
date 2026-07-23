package org.example.realtime.dws.app;

import com.alibaba.fastjson.JSONObject;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.example.realtime.base.BaseAPP;
import org.example.realtime.bean.TradeSkuOrderBean;
import org.example.realtime.constant.Constant;
import org.example.realtime.function.DimAsyncFunction;
import org.example.realtime.function.DorisMapFunction;
import org.example.realtime.util.DateFormatUtil;
import org.example.realtime.util.FlinkSinkUtil;
import org.example.realtime.util.HbaseUtil;
import org.example.realtime.util.RedisUtil;
import org.jline.utils.Log;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 *
 * 从Kafka订单明细主题读取数据，过滤null数据并按照唯一键对数据去重，按照SKU维度分组，统计原始金额、活动减免金额、优惠券减免金额和订单金额，并关联维度信息，将数据写入Doris交易域SKU粒度下单各窗口汇总表
 * 1）从 Kafka 订单明细主题读取数据
 * 2）转换数据结构
 * 3）按照唯一键去重
 * 4）转换数据结构
 * JSONObject转换为实体类TradeSkuOrderBean。
 * 5）按照 user_id 分组，统计下单独立用户数。
 * 运用 Flink 状态编程，记录每个用户的末次下单日期，末次下单日期为 null 或末次下单日期不等于当日则为当日下单独立用户
 * 6）设置水位线
 * 7）分组、开窗、聚合
 * 按照维度信息分组，度量字段求和，并在窗口闭合后补充窗口起始时间和结束时间。将时间戳置为当前系统时间。
 * 8）维度关联，补充与分组无关的维度字段
 * （1）关联 sku_info 表
 * 获取 sku_name，tm_id，category3_id，spu_id
 * （2）关联 spu_info 表
 * 获取 spu_name。
 * （3）关联 base_trademark 表
 * 获取 tm_name。
 * （4）关联 base_category3 表
 * 获取 name（三级品类名称），获取 category2_id。
 * （5）关联 base_categroy2 表
 * 获取 name（二级品类名称），category1_id。
 * （6）关联 base_category1 表
 * 获取 name（一级品类名称）。
 * 9）写出到Doris。
 */

public class DwsTradeSkuOrderWindowASyncCache extends BaseAPP {
    public static void main(String[] args) {
        new DwsTradeSkuOrderWindowASyncCache().Start(10029,4, Constant.DORIS_DWS_TRADE_SKU_ORDER_WINDOW_SYNC_CACHE,Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaSource) {
        //1. 从 Kafka 订单明细主题读取数据
//        kafkaSource.print();

        //2. 转换数据结构
        SingleOutputStreamOperator<JSONObject> flatmapStream = getFlatmapStream(kafkaSource);

        //3.设置水位线
        SingleOutputStreamOperator<JSONObject> watermarkStream = getTs(flatmapStream);

        //4. 按照 id 分组，统计下单独立用户数。
        KeyedStream<JSONObject, String> keyedStream = getId(watermarkStream);

        //5.状态编程，处理数据
        SingleOutputStreamOperator<TradeSkuOrderBean> processStream = getProcessStream(keyedStream);
//        processStream.print();

        //6.分组开窗聚合
        SingleOutputStreamOperator<TradeSkuOrderBean> keyedAndWindowReduceStream = getReduce(processStream);
//        keyedAndWindowReduceStream.print();


        //7. 异步IO关联维度信息，补充与分组无关的维度字段
        // 7.1 关联 sku_info 表
        SingleOutputStreamOperator<TradeSkuOrderBean> skuInfoStream = AsyncDataStream.unorderedWait(keyedAndWindowReduceStream, new DimAsyncFunction<TradeSkuOrderBean>() {
            @Override
            public String getId(TradeSkuOrderBean tradeSkuOrderBean) {
                return tradeSkuOrderBean.getSkuId();
            }

            @Override
            public String getTableName() {
                return "dim_sku_info";
            }

            @Override
            public void join(TradeSkuOrderBean tradeSkuOrderBean, JSONObject dim) {
                tradeSkuOrderBean.setCategory3Id(dim.getString("category3_id"));
                tradeSkuOrderBean.setTrademarkId(dim.getString("tm_id"));
                tradeSkuOrderBean.setSkuId(dim.getString("sku_id"));
                tradeSkuOrderBean.setSkuName("sku_name");
            }
        }, 60, TimeUnit.SECONDS);

        // 7.2 关联 spu_info 表
        SingleOutputStreamOperator<TradeSkuOrderBean> spuInfoStream = AsyncDataStream.unorderedWait(skuInfoStream, new DimAsyncFunction<TradeSkuOrderBean>() {

            @Override
            public String getId(TradeSkuOrderBean tradeSkuOrderBean) {
                return tradeSkuOrderBean.getSpuId();
            }

            @Override
            public String getTableName() {
                return "dim_spu_info";
            }

            @Override
            public void join(TradeSkuOrderBean tradeSkuOrderBean, JSONObject dim) {
                tradeSkuOrderBean.setSpuName(dim.getString("spu_name"));
            }
        }, 60, TimeUnit.SECONDS);

        // 7.3 关联 base_trademark 表
        SingleOutputStreamOperator<TradeSkuOrderBean> tmStreanm = AsyncDataStream.unorderedWait(spuInfoStream, new DimAsyncFunction<TradeSkuOrderBean>() {
            @Override
            public String getId(TradeSkuOrderBean tradeSkuOrderBean) {
                return tradeSkuOrderBean.getTrademarkId();
            }

            @Override
            public String getTableName() {
                return "dim_base_trademark";
            }

            @Override
            public void join(TradeSkuOrderBean tradeSkuOrderBean, JSONObject dim) {
                tradeSkuOrderBean.setTrademarkName(dim.getString("tm_name"));
            }
        }, 60, TimeUnit.SECONDS);

        // 7.4 关联 base_category3 表
        SingleOutputStreamOperator<TradeSkuOrderBean> c3Stream = AsyncDataStream.unorderedWait(tmStreanm, new DimAsyncFunction<TradeSkuOrderBean>() {
            @Override
            public String getId(TradeSkuOrderBean tradeSkuOrderBean) {
                return tradeSkuOrderBean.getCategory3Id();
            }

            @Override
            public String getTableName() {
                return "dim_base_category3";
            }

            @Override
            public void join(TradeSkuOrderBean tradeSkuOrderBean, JSONObject dim) {
                tradeSkuOrderBean.setCategory3Name(dim.getString("name"));
                tradeSkuOrderBean.setCategory2Id(dim.getString("category2_id"));

            }
        }, 60, TimeUnit.SECONDS);

        // 7.5 关联 base_category2 表
        SingleOutputStreamOperator<TradeSkuOrderBean> c2Stream = AsyncDataStream.unorderedWait(c3Stream, new DimAsyncFunction<TradeSkuOrderBean>() {
            @Override
            public String getId(TradeSkuOrderBean tradeSkuOrderBean) {
                return tradeSkuOrderBean.getCategory2Id();
            }

            @Override
            public String getTableName() {
                return "dim_base_category2";
            }

            @Override
            public void join(TradeSkuOrderBean tradeSkuOrderBean, JSONObject dim) {
                tradeSkuOrderBean.setCategory2Name(dim.getString("name"));
                tradeSkuOrderBean.setCategory1Id(dim.getString("category1_id"));
            }
        }, 60, TimeUnit.SECONDS);

        // 7.6 关联 base_category1 表
        SingleOutputStreamOperator<TradeSkuOrderBean> fullDimStream = AsyncDataStream.unorderedWait(c2Stream, new DimAsyncFunction<TradeSkuOrderBean>() {
            @Override
            public String getId(TradeSkuOrderBean tradeSkuOrderBean) {
                return tradeSkuOrderBean.getCategory1Id();
            }

            @Override
            public String getTableName() {
                return "dim_base_category1";
            }

            @Override
            public void join(TradeSkuOrderBean tradeSkuOrderBean, JSONObject dim) {
                tradeSkuOrderBean.setCategory1Name(dim.getString("name"));
            }
        }, 60, TimeUnit.SECONDS);
        fullDimStream.print();

        //8.格式转换。
//        SingleOutputStreamOperator<String> mappedStream = fullDimStream.map(new DorisMapFunction<>());
//        mappedStream.print();

        //9.写出到Doris
//        mappedStream.sinkTo(FlinkSinkUtil.getDorisSink(Constant.DORIS_DWS_TRADE_SKU_ORDER_WINDOW_SYNC_CACHE));


    }

    private static SingleOutputStreamOperator<TradeSkuOrderBean> getReduce(SingleOutputStreamOperator<TradeSkuOrderBean> processStream) {
        return processStream.keyBy(key -> key.getSkuId())
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))
                .reduce((v1, v2) -> {
                    v1.setOriginalAmount(v1.getOriginalAmount().add(v2.getOriginalAmount()));
                    v1.setActivityReduceAmount(v1.getActivityReduceAmount().add(v2.getActivityReduceAmount()));
                    v1.setCouponReduceAmount(v1.getCouponReduceAmount().add(v2.getCouponReduceAmount()));
                    v1.setOrderAmount(v1.getOrderAmount().add(v2.getOrderAmount()));
                    return v1;
                }, new ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>.Context context, Iterable<TradeSkuOrderBean> iterable, Collector<TradeSkuOrderBean> collector) throws Exception {
                        String start = DateFormatUtil.tsToDateTime(context.window().getStart());
                        String end = DateFormatUtil.tsToDateTime(context.window().getEnd());
                        String curDt = DateFormatUtil.tsToDateForPartition(System.currentTimeMillis());

                        for (TradeSkuOrderBean element : iterable) {
                            element.setStt(start);
                            element.setEdt(end);
                            element.setCurDate(curDt);
                            collector.collect(element);
                        }
                    }
                });
    }


    private static SingleOutputStreamOperator<TradeSkuOrderBean> getProcessStream(KeyedStream<JSONObject, String> keyedStream) {
        return keyedStream.process(new KeyedProcessFunction<String, JSONObject, TradeSkuOrderBean>() {
            MapState<String, BigDecimal> lastAmountState;

            @Override
            public void open(Configuration parameters) throws Exception {
                MapStateDescriptor<String, BigDecimal> lastAmountDesc = new MapStateDescriptor<>("last_amount", String.class, BigDecimal.class);
                lastAmountDesc.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(30)).build());
                lastAmountState = getRuntimeContext().getMapState(lastAmountDesc);
            }

            @Override
            public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, TradeSkuOrderBean>.Context context, Collector<TradeSkuOrderBean> collector) throws Exception {
                // 获取各金额的状态信息
                BigDecimal originalAmount = lastAmountState.get("originalAmount");
                BigDecimal activityReduceAmount = lastAmountState.get("activityReduceAmount");
                BigDecimal couponReduceAmount = lastAmountState.get("couponReduceAmount");
                BigDecimal orderAmount = lastAmountState.get("orderAmount");

                originalAmount = originalAmount == null ? new BigDecimal("0") : originalAmount;
                activityReduceAmount = activityReduceAmount == null ? new BigDecimal("0") : activityReduceAmount;
                couponReduceAmount = couponReduceAmount == null ? new BigDecimal("0") : couponReduceAmount;
                orderAmount = orderAmount == null ? new BigDecimal("0") : orderAmount;

                // 获取必要字段，做null检查
                BigDecimal orderPrice = jsonObject.getBigDecimal("order_price") == null ? new BigDecimal("0") : jsonObject.getBigDecimal("order_price");;
                BigDecimal skuNum = jsonObject.getBigDecimal("sku_num") == null ? new BigDecimal("0") : jsonObject.getBigDecimal("sku_num");;
                BigDecimal splitTotalAmount = jsonObject.getBigDecimal("split_total_amount") == null ? new BigDecimal("0") : jsonObject.getBigDecimal("split_total_amount");;
                BigDecimal splitActivityAmount = jsonObject.getBigDecimal("split_activity_amount") == null ? new BigDecimal("0") : jsonObject.getBigDecimal("split_activity_amount");;
                BigDecimal splitCouponAmount = jsonObject.getBigDecimal("split_coupon_amount") == null ? new BigDecimal("0") : jsonObject.getBigDecimal("split_coupon_amount");;


                // 计算当前订单金额，单价*数量
                BigDecimal curOriginalAmount = orderPrice.multiply(skuNum);

                // 构建实体类
                TradeSkuOrderBean bean = new TradeSkuOrderBean().builder()
                        .skuId(jsonObject.getString("sku_id"))
                        .orderDetailId(jsonObject.getString("id"))
                        .ts(jsonObject.getLong("ts"))
                        .originalAmount(curOriginalAmount.subtract(originalAmount))
                        .orderAmount(splitTotalAmount.subtract(orderAmount))
                        .activityReduceAmount(splitActivityAmount.subtract(activityReduceAmount))
                        .couponReduceAmount(splitCouponAmount.subtract(couponReduceAmount))
                        .build();

                // 更新状态信息
                lastAmountState.put("originalAmount", curOriginalAmount);
                lastAmountState.put("activityReduceAmount", splitActivityAmount);
                lastAmountState.put("couponReduceAmount", splitCouponAmount);
                lastAmountState.put("orderAmount", splitTotalAmount);

                collector.collect(bean);
            }
        });
    }

    private static KeyedStream<JSONObject, String> getId(SingleOutputStreamOperator<JSONObject> watermarkStream) {
        return watermarkStream.keyBy(key -> key.getString("id"));
    }

    private static SingleOutputStreamOperator<JSONObject> getTs(SingleOutputStreamOperator<JSONObject> flatmapStream) {
        return flatmapStream.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject jsonObject, long l) {
                return jsonObject.getLong("ts");
            }
        }));
    }

    private static SingleOutputStreamOperator<JSONObject> getFlatmapStream(DataStreamSource<String> kafkaSource) {
        return kafkaSource.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                try {
                    if (s != null) {
                        JSONObject jsonObj = JSONObject.parseObject(s);
                        String id = jsonObj.getString("id");
                        String sku_id = jsonObj.getString("sku_id");
                        Long ts = jsonObj.getLong("ts");

                        if (sku_id != null && id != null && ts != null) {
                            jsonObj.put("ts", ts * 1000);
                            collector.collect(jsonObj);
                        }
                    }
                } catch (Exception e) {
                    Log.info("转换数据结构异常，错误数据：" + s);
                }
            }
        });
    }
}