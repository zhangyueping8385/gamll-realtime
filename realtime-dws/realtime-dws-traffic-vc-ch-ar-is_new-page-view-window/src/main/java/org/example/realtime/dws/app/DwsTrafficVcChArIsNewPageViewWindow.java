package org.example.realtime.dws.app;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.example.realtime.base.BaseAPP;
import org.example.realtime.bean.TrafficPageViewBean;
import org.example.realtime.constant.Constant;
import org.example.realtime.function.DorisMapFunction;
import org.example.realtime.util.DateFormatUtil;
import org.example.realtime.util.FlinkSinkUtil;
import org.jline.utils.Log;

import java.time.Duration;

public class DwsTrafficVcChArIsNewPageViewWindow extends BaseAPP {
    public static void main(String[] args) {
        new DwsTrafficVcChArIsNewPageViewWindow().Start(10022,4, Constant.DORIS_DWS_TRAFFIC_VC_CH_AR_IS_NEW_PAGE_VIEW_WINDOW,Constant.TOPIC_DWD_TRAFFIC_PAGE);
    }


    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaSource) {
//        kafkaSource.print();

    //1. 转换为JSON格式
        SingleOutputStreamOperator<JSONObject> jsonObjectStream = getJsonObjectStream(kafkaSource);

        // 2. 按照mid进行分组，记录独立访客
        SingleOutputStreamOperator<TrafficPageViewBean> processStream = getTrafficPageViewBeanSingleOutputStreamOperator(jsonObjectStream);


        // 3. 按照时间戳进行水印分配
        SingleOutputStreamOperator<TrafficPageViewBean> watermarkStream = getWatermarkStream(processStream);

        // 4. 按照vc:ch:ar:is_new进行分组,vc代表渠道,ar代表区域,ch代表国家,is_new代表是否新访客
        KeyedStream<TrafficPageViewBean, String> keyedStream = getKeyedStream(watermarkStream);

        //5. 设置10秒的滚动窗口
        WindowedStream<TrafficPageViewBean, String, TimeWindow> windowStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10L)));

        // 6. 对窗口内的数据进行聚合,累加独立访客数,会话数,页面浏览数,累计访问时长
        SingleOutputStreamOperator<TrafficPageViewBean> reducedStream = getTrafficPageViewBeanSingleOutputStreamOperator(windowStream);

        //7. 对聚合结果进行转换,将对象转换为字符串
        SingleOutputStreamOperator<String> mappedStream = reducedStream.map(new DorisMapFunction<>());
        mappedStream.print();

        //8.写出到doris
        mappedStream.sinkTo(FlinkSinkUtil.getDorisSink(Constant.DORIS_DWS_TRAFFIC_VC_CH_AR_IS_NEW_PAGE_VIEW_WINDOW));


    }

    private static SingleOutputStreamOperator<TrafficPageViewBean> getTrafficPageViewBeanSingleOutputStreamOperator(WindowedStream<TrafficPageViewBean, String, TimeWindow> windowStream) {
        SingleOutputStreamOperator<TrafficPageViewBean> reducedStream = windowStream.reduce(new ReduceFunction<TrafficPageViewBean>() {
            @Override
            public TrafficPageViewBean reduce(TrafficPageViewBean v1, TrafficPageViewBean v2) throws Exception {
                // 多个元素的度量值累加到一起,v1代表当前窗口的聚合结果,v2代表当前元素
                v1.setUvCt(v1.getUvCt() + v2.getUvCt());
                v1.setPvCt(v1.getPvCt() + v2.getPvCt());
                v1.setDurSum(v1.getDurSum() + v2.getDurSum());
                v1.setSvCt(v1.getSvCt() + v2.getSvCt());
                return v1;
            }
        }, new ProcessWindowFunction<TrafficPageViewBean, TrafficPageViewBean, String, TimeWindow>() {
            // 处理窗口聚合结果,添加窗口时间,当天日期
            @Override
            public void process(String s, ProcessWindowFunction<TrafficPageViewBean, TrafficPageViewBean, String, TimeWindow>.Context context, Iterable<TrafficPageViewBean> iterable, Collector<TrafficPageViewBean> collector) throws Exception {
                TimeWindow window = context.window();
                String start = DateFormatUtil.tsToDateTime(window.getStart());
                String end = DateFormatUtil.tsToDateTime(window.getEnd());
                String cur_dt = DateFormatUtil.tsToDateForPartition(System.currentTimeMillis());
                for (TrafficPageViewBean ele : iterable) {
                    ele.setStt(start);
                    ele.setEdt(end);
                    ele.setCur_date(cur_dt);
                    collector.collect(ele);
                }
            }
        });
        return reducedStream;
    }

    private static KeyedStream<TrafficPageViewBean, String> getKeyedStream(SingleOutputStreamOperator<TrafficPageViewBean> watermarkStream) {
        return watermarkStream.keyBy(new KeySelector<TrafficPageViewBean, String>() {
            @Override
            public String getKey(TrafficPageViewBean trafficPageViewBean) throws Exception {
                return trafficPageViewBean.getVc() + ":" + trafficPageViewBean.getCh() + ":" + trafficPageViewBean.getAr() + ":" + trafficPageViewBean.getIsNew();
            }
        });
    }

    private static SingleOutputStreamOperator<TrafficPageViewBean> getWatermarkStream(SingleOutputStreamOperator<TrafficPageViewBean> processStream) {
        return processStream.assignTimestampsAndWatermarks(WatermarkStrategy.<TrafficPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(3L)).withTimestampAssigner(new SerializableTimestampAssigner<TrafficPageViewBean>() {
            @Override
            public long extractTimestamp(TrafficPageViewBean trafficPageViewBean, long l) {
                return trafficPageViewBean.getTs();
            }
        }));
    }

    private static SingleOutputStreamOperator<TrafficPageViewBean> getTrafficPageViewBeanSingleOutputStreamOperator(SingleOutputStreamOperator<JSONObject> jsonObjectStream) {
        SingleOutputStreamOperator<TrafficPageViewBean> processStream = jsonObjectStream.keyBy(new KeySelector<JSONObject, String>() {
                    @Override
                    public String getKey(JSONObject jsonObject) throws Exception {
                        return jsonObject.getJSONObject("common").getString("mid");
                    }
                })
                .process(new ProcessFunction<JSONObject, TrafficPageViewBean>() {
                    ValueState<String> lastOutDtState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> lastOutDtSDesc = new ValueStateDescriptor<>("last_out_dt", String.class);
                        lastOutDtSDesc.enableTimeToLive(StateTtlConfig.newBuilder(org.apache.flink.api.common.time.Time.days(1L))
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite) // 仅在创建和写入时更新TTL,OnReadAndWrite 读取时也更新TTL
                                .build());
                        lastOutDtState = getRuntimeContext().getState(lastOutDtSDesc);
                    }

                    /**
                     * UV	同设备当天是否首次出现	首次=1，其余=0
                     * SV	last_page_id 是否为 null	会话起始页=1，其余=0
                     * PV	每条日志代表一次页面浏览	恒为 1
                     * durSum	page.during_time 字段	页面停留毫秒数
                     * @param value
                     * @param context
                     * @param collector
                     * @throws Exception
                     */
                    @Override
                    public void processElement(JSONObject value, ProcessFunction<JSONObject, TrafficPageViewBean>.Context context, Collector<TrafficPageViewBean> collector) throws Exception {
                        // 判断独立访客
                        Long ts = value.getLong("ts");
                        JSONObject page = value.getJSONObject("page");
                        JSONObject common = value.getJSONObject("common");
                        String curDt = DateFormatUtil.tsToDateTime(ts);
                        String lastLoginDt = lastOutDtState.value();
                        Long uvCt = 0L;
                        Long svCt = 0L;

                        if (lastLoginDt == null || !lastLoginDt.equals(curDt)) {
                            uvCt = 1L;
                            // 新访客
                            lastOutDtState.update(curDt);
                        }

                        //判断会话数
                        String last_page_id = page.getString("last_page_id");
                        if (last_page_id == null) {
                            svCt = 1L;
                        }

                        collector.collect(
                                TrafficPageViewBean.builder()
                                        .vc(common.getString("vc"))
                                        .ar(common.getInteger("ar"))
                                        .ch(common.getString("ch"))
                                        .isNew(common.getString("is_new"))
                                        .uvCt(uvCt)
                                        .svCt(svCt)
                                        .pvCt(1L)
                                        .durSum(page.getLong("during_time"))
                                        .sid(common.getString("sid"))
                                        .ts(ts)
                                        .build()
                        );


                    }
                });
        return processStream;
    }

    private static SingleOutputStreamOperator<JSONObject> getJsonObjectStream(DataStreamSource<String> kafkaSource) {
        return kafkaSource.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObj = JSONObject.parseObject(s);
                    Long ts = jsonObj.getLong("ts");
                    String mid = jsonObj.getJSONObject("common").getString("mid");
                    if (mid != null && ts != null) {
                        collector.collect(jsonObj);
                    }
                } catch (Exception e) {
                    Log.info("转换为JSON格式失败", s);
                }
            }
        });
    }
}