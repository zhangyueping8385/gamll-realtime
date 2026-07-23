package org.example.realtime.dws.app;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.example.realtime.base.BaseAPP;
import org.example.realtime.bean.TrafficHomeDetailPageViewBean;
import org.example.realtime.constant.Constant;
import org.example.realtime.function.DorisMapFunction;
import org.example.realtime.util.DateFormatUtil;
import org.example.realtime.util.FlinkSinkUtil;
import org.jline.utils.Log;

import java.time.Duration;

/**
 * 1）读取 Kafka 页面主题数据
 * 2）转换数据结构
 * 将流中数据由 String 转换为 JSONObject。
 * 3）过滤数据
 * 仅保留 page_id 为 home 或 good_detail 的数据，因为本程序统计的度量仅与这两个页面有关，其它数据无用。
 * 4）设置水位线
 * 5）按照 mid 分组
 * 6）统计首页和商品详情页独立访客数，转换数据结构
 * 运用 Flink 状态编程，为每个 mid 维护首页和商品详情页末次访问日期。如果 page_id 为 home，
 * 当状态中存储的日期为 null 或不是当日时，将 homeUvCt（首页独立访客数） 置为 1，并将状态中的日期更新为当日。
 * 否则置为 0，不做操作。商品详情页独立访客的统计同理。当 homeUvCt 和 detailUvCt 至少有一个不为 0 时，
 * 将统计结果和相关维度信息封装到定义的实体类中，发送到下游，否则舍弃数据。
 * 7）开窗
 * 8）聚合
 * 9）将数据写出到 Doris
 */
public class DwsTrafficHomeDetailPageViewWindow extends BaseAPP {
    public static void main(String[] args) {
        new DwsTrafficHomeDetailPageViewWindow().Start(10023,4, Constant.DORIS_DWS_TRAFFIC_HOME_DETAIL_PAGE_VIEW_WINDOW,Constant.TOPIC_DWD_TRAFFIC_PAGE);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaSource) {
            kafkaSource.print();

        //1. 转换数据结构,保留 page_id 为 home 或 good_detail 的数据
        SingleOutputStreamOperator<JSONObject> flatmapStream = getFlatmapStream(kafkaSource);

        //2. 设置水位线
        SingleOutputStreamOperator<JSONObject> watermarkStream = getTs(flatmapStream);

        //3. 按照 mid 分组
        KeyedStream<JSONObject, String> keyedStream = getKeyedStream(watermarkStream);

        //4. 统计首页和商品详情页独立访客数，转换数据结构
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> porcessStream = getPorcessStream(keyedStream);

        //5. 开窗聚合
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> reduceWithWinodwStream = getReduceWithWinodwStream(porcessStream);

        //6. 转换数据结构
        SingleOutputStreamOperator<String> mappedStream = reduceWithWinodwStream.map(new DorisMapFunction<>());
        mappedStream.print();

        //7.写入 Dororis
        mappedStream.sinkTo(FlinkSinkUtil.getDorisSink(Constant.DORIS_DWS_TRAFFIC_HOME_DETAIL_PAGE_VIEW_WINDOW));

    }

    private static SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> getReduceWithWinodwStream(SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> porcessStream) {
        return porcessStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(10L)))
                .reduce(new ReduceFunction<TrafficHomeDetailPageViewBean>() {
                    @Override
                    public TrafficHomeDetailPageViewBean reduce(TrafficHomeDetailPageViewBean v1, TrafficHomeDetailPageViewBean v2) throws Exception {
                        // 合并两个窗口的统计数据
                        v1.setHomeUvCt(v1.getHomeUvCt() + v2.getHomeUvCt());
                        v1.setGoodDetailUvCt(v1.getGoodDetailUvCt() + v2.getGoodDetailUvCt());
                        return v1;
                    }
                }, new AllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<TrafficHomeDetailPageViewBean> iterable, Collector<TrafficHomeDetailPageViewBean> collector) throws Exception {
                        String start = DateFormatUtil.tsToDateTime(timeWindow.getStart());
                        String end = DateFormatUtil.tsToDateTime(timeWindow.getEnd());
                        String curDate = DateFormatUtil.tsToDateForPartition(System.currentTimeMillis());
                        // 合并所有窗口的统计数据
                        for (TrafficHomeDetailPageViewBean value : iterable) {
                            value.setStt(start);
                            value.setEdt(end);
                            value.setCurDate(curDate);
                            collector.collect(value);
                        }
                    }
                });
    }

    private static SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> getPorcessStream(KeyedStream<JSONObject, String> keyedStream) {
        return keyedStream.process(new ProcessFunction<JSONObject, TrafficHomeDetailPageViewBean>() {
            ValueState<String> homeLastLoginState;
            ValueState<String> detailLastLoginState;

            @Override
            public void open(Configuration parameters) throws Exception {
                // 初始化状态,用于存储每个 mid 的首页末次访问日期
                ValueStateDescriptor<String> homeLastLoginDesc = new ValueStateDescriptor<>("home_last_login", String.class);
                homeLastLoginDesc.enableTimeToLive(StateTtlConfig.newBuilder(org.apache.flink.api.common.time.Time.days(1L))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite) // 状态在创建和写入时更新 TTL
                        .build());
                homeLastLoginState = getRuntimeContext().getState(homeLastLoginDesc);

                // 初始化状态,用于存储每个 mid 的商品详情页末次访问日期
                ValueStateDescriptor<String> detailLastLoginDesc = new ValueStateDescriptor<>("detail_last_login", String.class);
                detailLastLoginDesc.enableTimeToLive(StateTtlConfig.newBuilder(org.apache.flink.api.common.time.Time.days(1L))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite) // 状态在创建和写入时更新 TTL
                        .build());
                detailLastLoginState = getRuntimeContext().getState(detailLastLoginDesc);

            }

            @Override
            public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, TrafficHomeDetailPageViewBean>.Context context, Collector<TrafficHomeDetailPageViewBean> collector) throws Exception {
                JSONObject page = jsonObject.getJSONObject("page");
                String pageId = page.getString("page_id");
                Long ts = jsonObject.getLong("ts");
                String cur_dt = DateFormatUtil.tsToDate(ts);
                // 首页独立访客数
                Long homeUvCt = 0L;
                // 商品详情页独立访客数
                Long goodDetailUvCt = 0L;
                if ("home".equals(pageId)) {  // 首页
                    String homeLastLogin = homeLastLoginState.value(); // 获取首页末次访问日期
                    if (homeLastLogin == null || !homeLastLogin.equals(cur_dt)) { // 如果状态中存储的日期为 null 或不是当日
                        homeUvCt = 1L;  // 首页独立访客数置为 1
                        homeLastLoginState.update(cur_dt);  // 更新状态中的日期为当日
                    }
                } else { // 商品详情页
                    String detailLastLogin = detailLastLoginState.value();
                    if (detailLastLogin == null || !detailLastLogin.equals(cur_dt)) {
                        goodDetailUvCt = 1L;
                        detailLastLoginState.update(cur_dt);
                    }
                }

                if (homeUvCt != 0 || goodDetailUvCt != 0) {
                    collector.collect(TrafficHomeDetailPageViewBean.builder()
                            .homeUvCt(homeUvCt)
                            .goodDetailUvCt(goodDetailUvCt)
                            .ts(ts)
                            .build());
                }

            }
        });
    }

    private static KeyedStream<JSONObject, String> getKeyedStream(SingleOutputStreamOperator<JSONObject> watermarkStream) {
        return watermarkStream.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject jsonObject) throws Exception {
                return jsonObject.getJSONObject("common").getString("mid");
            }
        });
    }


    private static SingleOutputStreamOperator<JSONObject> getTs(SingleOutputStreamOperator<JSONObject> flatmapStream) {
        return flatmapStream.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3L)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
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
                    JSONObject jsonObject = JSONObject.parseObject(s);
                    JSONObject page = jsonObject.getJSONObject("page");
                    String pageId = page.getString("page_id");
                    String mid = jsonObject.getJSONObject("common").getString("mid");
                    if (mid != null && ("home".equals(pageId) || "good_detail".equals(pageId))) {
                        collector.collect(jsonObject);
                    }
                } catch (Exception e) {
                    Log.info("转换数据结构失败，数据：" + s);
                }
            }
        });
    }
}
