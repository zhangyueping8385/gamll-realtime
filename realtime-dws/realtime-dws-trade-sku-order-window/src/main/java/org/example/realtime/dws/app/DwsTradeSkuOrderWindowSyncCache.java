package org.example.realtime.dws.app;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;
import org.example.realtime.base.BaseAPP;
import org.example.realtime.bean.TradeSkuOrderBean;
import org.example.realtime.constant.Constant;
import org.example.realtime.function.DorisMapFunction;
import org.example.realtime.util.DateFormatUtil;
import org.example.realtime.util.FlinkSinkUtil;
import org.example.realtime.util.HbaseUtil;
import org.example.realtime.util.RedisUtil;
import org.jline.utils.Log;
import redis.clients.jedis.Jedis;

import java.math.BigDecimal;
import java.time.Duration;

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

public class DwsTradeSkuOrderWindowSyncCache extends BaseAPP {
    public static void main(String[] args) {
        new DwsTradeSkuOrderWindowSyncCache().Start(10029,4, Constant.DORIS_DWS_TRADE_SKU_ORDER_WINDOW_SYNC_CACHE,Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);
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


        SingleOutputStreamOperator<TradeSkuOrderBean> mapStream = getMapStream(keyedAndWindowReduceStream);
        mapStream.print();


        //9.写出到Doris
//        mappedStream.sinkTo(FlinkSinkUtil.getDorisSink(Constant.DORIS_DWS_TRADE_SKU_ORDER_WINDOW_SYNC_CACHE));

    }

    private static SingleOutputStreamOperator<TradeSkuOrderBean> getMapStream(SingleOutputStreamOperator<TradeSkuOrderBean> keyedAndWindowReduceStream) {
        return keyedAndWindowReduceStream.map(new RichMapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {
           Connection connection;
           Jedis jedis;

           @Override
           public void open(Configuration parameters) throws Exception {
               connection = HbaseUtil.getConnection();
               jedis = RedisUtil.getJedis();
           }

           @Override
           public void close() throws Exception {
               connection.close();
               jedis.close();
           }

           @Override
           public TradeSkuOrderBean map(TradeSkuOrderBean tradeSkuOrderBean) throws Exception {
               // 拼接对应的redisKey
               String redisKey = RedisUtil.getKey(Constant.HBASE_TABLE_DIM_SKU_INFO, tradeSkuOrderBean.getSkuId());

               // 读取redis数据
               String dim = jedis.get(redisKey);
               JSONObject jsonObj = new JSONObject();
               JSONObject dimSkuInfo = null;

               // 判断redis读取到的数据是否为空
               if (dim == null || dim.length() == 0) {
                   System.out.println("没有对应的redis数据" + tradeSkuOrderBean);
                   jsonObj = HbaseUtil.getRow(connection, Constant.HBASE_NAMESPACE, "dim_sku_info", tradeSkuOrderBean.getSkuId(), JSONObject.class);

                   if (jsonObj.size() != 0) {
                       jedis.setex(redisKey, 2 * 60 * 60, jsonObj.toJSONString());
                   }
               } else {
                   System.out.println("有对应的redis数据" + tradeSkuOrderBean);
                   dimSkuInfo = jsonObj.parseObject(dim);
               }

               if (dimSkuInfo.size() != 0) {
                   tradeSkuOrderBean.setCategory3Id(dimSkuInfo.getString("category3_id"));
                   tradeSkuOrderBean.setTrademarkId(dimSkuInfo.getString("tm_id"));
                   tradeSkuOrderBean.setSkuId(dimSkuInfo.getString("sku_id"));
                   tradeSkuOrderBean.setSkuName("sku_name");
               } else {
                   System.out.println("没有对应的纬度信息" + tradeSkuOrderBean);
               }


               return tradeSkuOrderBean;
           }
       });
    }

    private static SingleOutputStreamOperator<TradeSkuOrderBean> getMapedStream(SingleOutputStreamOperator<TradeSkuOrderBean> keyedAndWindowReduceStream) {
        return keyedAndWindowReduceStream.map(new RichMapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {
           Connection connection;

           @Override
           public void open(Configuration parameters) throws Exception {
               connection = HbaseUtil.getConnection();
           }

           @Override
           public void close() throws Exception {
               connection.close();
           }

           @Override
           public TradeSkuOrderBean map(TradeSkuOrderBean tradeSkuOrderBean) throws Exception {
               // 关联HBASE_TABLE_DIM_SKU_INFO纬度表
               JSONObject dimSkuInfo = HbaseUtil.putRow(connection, Constant.HBASE_NAMESPACE, Constant.HBASE_TABLE_DIM_SKU_INFO, tradeSkuOrderBean.getSkuId());
               tradeSkuOrderBean.setCategory3Id(dimSkuInfo.getString("category3_id"));
               tradeSkuOrderBean.setTrademarkId(dimSkuInfo.getString("tm_id"));
               tradeSkuOrderBean.setSpuId(dimSkuInfo.getString("spu_id"));
               tradeSkuOrderBean.setSkuName(dimSkuInfo.getString("sku_name"));

               // 关联HBASE_TABLE_DIM_SPU_INFO
               JSONObject dimSpuInfo = HbaseUtil.putRow(connection, Constant.HBASE_NAMESPACE, Constant.HBASE_TABLE_DIM_SPU_INFO, tradeSkuOrderBean.getSpuId());
               tradeSkuOrderBean.setSpuName(dimSpuInfo.getString("spu_name"));

               // 关联HBASE_TABLE_DIM_BASE_CATEGORY3
               JSONObject dimC3 = HbaseUtil.putRow(connection, Constant.HBASE_NAMESPACE, Constant.HBASE_TABLE_DIM_BASE_CATEGORY3, tradeSkuOrderBean.getCategory3Id());
               tradeSkuOrderBean.setCategory3Name(dimC3.getString("name"));
               tradeSkuOrderBean.setCategory2Id(dimC3.getString("category2_id"));

               // 关联HBASE_TABLE_DIM_BASE_CATEGORY2
               JSONObject dimC2 = HbaseUtil.putRow(connection, Constant.HBASE_NAMESPACE, Constant.HBASE_TABLE_DIM_BASE_CATEGORY2, tradeSkuOrderBean.getCategory2Id());
               tradeSkuOrderBean.setCategory2Name(dimC2.getString("name"));
               tradeSkuOrderBean.setCategory1Id(dimC2.getString("category1_id"));

               // 关联HBASE_TABLE_DIM_BASE_CATEGORY1
               JSONObject dimC1 = HbaseUtil.putRow(connection, Constant.HBASE_NAMESPACE, Constant.HBASE_TABLE_DIM_BASE_CATEGORY1, tradeSkuOrderBean.getCategory1Id());
               tradeSkuOrderBean.setCategory1Name(dimC1.getString("name"));

               //关联HBASE_TABLE_DIM_BASE_TRADEMARK
               JSONObject dimTradeMark = HbaseUtil.putRow(connection, Constant.HBASE_NAMESPACE, Constant.HBASE_TABLE_DIM_BASE_TRADEMARK, tradeSkuOrderBean.getTrademarkId());
               tradeSkuOrderBean.setTrademarkName(dimTradeMark.getString("tm_name"));

               return tradeSkuOrderBean;
           }

       });
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