package org.example.realtime.dws.app;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.example.realtime.base.BaseAPP;
import org.example.realtime.bean.CartAddUuBean;
import org.example.realtime.constant.Constant;
import org.example.realtime.function.DorisMapFunction;
import org.example.realtime.util.DateFormatUtil;
import org.example.realtime.util.FlinkSinkUtil;
import org.jline.utils.Log;

import java.time.Duration;

/**
 * 从 Kafka 读取用户加购明细数据，统计各窗口加购独立用户数，写入 Doris。
 * 1）从 Kafka 加购明细主题读取数据
 * 2）转换数据结构
 * 将流中数据由 String 转换为 JSONObject。
 * 3）设置水位线
 * 4）按照用户 id 分组
 * 5）过滤独立用户加购记录
 * 运用 Flink 状态编程，将用户末次加购日期维护到状态中。
 * 如果末次加购日期为 null 或者不等于当天日期，则保留数据并更新状态，否则丢弃，不做操作。
 * 6）开窗、聚合
 * 统计窗口中数据条数即为加购独立用户数，补充窗口起始时间、关闭时间，统计日期，发送到下游。
 * 7）将数据写入 Doris。
 *
 */
public class DwsTradeCartAddUuWindow extends BaseAPP {
    public static void main(String[] args) {
        new DwsTradeCartAddUuWindow().Start(10026,4, Constant.DORIS_DWS_TRADE_CART_ADD_UU_WINDOW,Constant.TOPIC_DWD_TRADE_CART_ADD);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaSource) {
        //1.转换数据结构
        SingleOutputStreamOperator<JSONObject> flatMapStream = getFlatMapStream(kafkaSource);

        //2.设置水位线
        SingleOutputStreamOperator<JSONObject> watermarkStream = getTs(flatMapStream);

        //3.按照用户 id 分组
        KeyedStream<JSONObject, String> keyedStream = getUserId(watermarkStream);

        //4.过滤独立用户加购记录
        SingleOutputStreamOperator<CartAddUuBean> processStream = getPorcessStream(keyedStream);

        //5.开窗、聚合
        SingleOutputStreamOperator<CartAddUuBean> windowAndReduceStream = getReduce(processStream);

        //6.转换数据结构
        SingleOutputStreamOperator<String> mappedStream = windowAndReduceStream.map(new DorisMapFunction<>());
        mappedStream.print();

        //7.将数据写入 Doris
        mappedStream.sinkTo(FlinkSinkUtil.getDorisSink(Constant.DORIS_DWS_TRADE_CART_ADD_UU_WINDOW));
    }

    private static SingleOutputStreamOperator<CartAddUuBean> getReduce(SingleOutputStreamOperator<CartAddUuBean> processStream) {
        return processStream.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))
                .reduce((v1, v2) -> {
                    v1.setCartAddUuCt(v1.getCartAddUuCt() + v2.getCartAddUuCt());
                    return v1;
                }, new ProcessAllWindowFunction<CartAddUuBean, CartAddUuBean, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<CartAddUuBean, CartAddUuBean, TimeWindow>.Context context, Iterable<CartAddUuBean> iterable, Collector<CartAddUuBean> collector) throws Exception {
                        // 统计窗口中数据条数即为加购独立用户数
                        String start = DateFormatUtil.tsToDateTime(context.window().getStart());
                        String end = DateFormatUtil.tsToDateTime(context.window().getEnd());
                        String ts = DateFormatUtil.tsToDateForPartition(System.currentTimeMillis());
                        for (CartAddUuBean element : iterable) {
                            element.setStt(start);
                            element.setEdt(end);
                            element.setCurDate(ts);
                            collector.collect(element);
                        }
                    }
                });
    }

    private static SingleOutputStreamOperator<CartAddUuBean> getPorcessStream(KeyedStream<JSONObject, String> keyedStream) {
        return keyedStream.process(new ProcessFunction<JSONObject, CartAddUuBean>() {
            ValueState<String> lasLoginState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> lastLoginDesc = new ValueStateDescriptor<>("last_login_date", String.class);
                lastLoginDesc.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                lasLoginState = getRuntimeContext().getState(lastLoginDesc);
            }

            @Override
            public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, CartAddUuBean>.Context context, Collector<CartAddUuBean> collector) throws Exception {
                // 判断独立用户
                // 比较当前日期是否与末次加购日期不同
                String lastLoginDt = lasLoginState.value();
                String curDt = DateFormatUtil.tsToDate(jsonObject.getLong("ts"));
                // 加购独立用户数
                if (lastLoginDt == null || !lastLoginDt.equals(curDt)) {
                    // 保留数据并更新状态
                    lasLoginState.update(curDt);
                    collector.collect(new CartAddUuBean("", "", "", 1L));
                } else {
                    // 丢弃数据，不做操作
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
                    String userId = jsonObject.getString("user_id");
                    Long ts = jsonObject.getLong("ts");

                    if (userId != null && ts != null) {
                        jsonObject.put("ts", ts * 1000);
                        collector.collect(jsonObject);
                    }
                } catch (Exception e) {
                    Log.info("转换数据结构失败，数据：" + s);
                }
            }
        });
    }
}
