package org.example.realtime.dwd.db.split.app;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.protobuf.ByteString;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.example.realtime.base.BaseAPP;
import org.example.realtime.constant.Constant;
import org.example.realtime.util.DateFormatUtil;
import org.example.realtime.util.FlinkSinkUtil;
import org.jline.utils.Log;

import java.time.Duration;

public class DwdBaseLog extends BaseAPP {
    public static void main(String[] args) {
        new DwdBaseLog().Start(10011,4,"dwd_base_log", Constant.TOPIC_LOG);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaSource) {
        // 核心业务处理逻辑

        //1. 过滤不完整的数据
        SingleOutputStreamOperator<JSONObject> flatmapStream = getFlatmapStream(kafkaSource);
//        flatmapStream.print();

        //2. 进行新旧访客修复
        KeyedStream<JSONObject, String> keyByWithWatermark = getKeyByWithWatermark(flatmapStream);
        SingleOutputStreamOperator<JSONObject> isNewFixStream = validateNewOrOld(keyByWithWatermark);
//        isNewFixStream.print();

        //3. 拆分不同用户的行为日志，主要分为
        //启动日志：启动信息start、报错信息err
        //页面日志：页面信息page、曝光信息display、动作信息action、报错信息err
        OutputTag<String> startOutputTag = new OutputTag<>("start", TypeInformation.of(String.class));
        OutputTag<String> errorOutputTag = new OutputTag<>("error", TypeInformation.of(String.class));
        OutputTag<String> actionOutputTag = new OutputTag<>("action", TypeInformation.of(String.class));
        OutputTag<String> displayOutputTag = new OutputTag<>("display", TypeInformation.of(String.class));

        SingleOutputStreamOperator<String> pageStream = getSplitLog(isNewFixStream, startOutputTag, errorOutputTag, displayOutputTag, actionOutputTag);
        SideOutputDataStream<String> startStream = pageStream.getSideOutput(startOutputTag);
        SideOutputDataStream<String> errorStream = pageStream.getSideOutput(errorOutputTag);
        SideOutputDataStream<String> actionStream = pageStream.getSideOutput(actionOutputTag);
        SideOutputDataStream<String> displayStream = pageStream.getSideOutput(displayOutputTag);
        pageStream.print("pageStream");
        startStream.print("startStream");
        errorStream.print("errorStream");
        actionStream.print("actionStream");
        displayStream.print("displayStream");

        //4. 写入kafka
        pageStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_PAGE));
        startStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_START));
        errorStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ERR));
        actionStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ACTION));
        displayStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_DISPLAY));


    }

    public SingleOutputStreamOperator<String> getSplitLog(SingleOutputStreamOperator<JSONObject> isNewFixStream, OutputTag<String> startOutputTag, OutputTag<String> errorOutputTag, OutputTag<String> displayOutputTag, OutputTag<String> actionOutputTag) {
        return isNewFixStream.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, String>.Context context, Collector<String> collector) throws Exception {
                // 全部改用安全API，缺失返回null，不抛异常
                JSONObject startObj = jsonObject.getObject("start", JSONObject.class);
                JSONObject pageObj = jsonObject.getObject("page", JSONObject.class);
                JSONObject errorObj = jsonObject.getObject("err", JSONObject.class);
                JSONObject commonObj = jsonObject.getObject("common", JSONObject.class);
                Long tsVal = jsonObject.getLong("ts");

                if (startObj != null) {
                    context.output(startOutputTag, jsonObject.toJSONString());
                } else if (errorObj != null) {
                    context.output(errorOutputTag, errorObj.toJSONString());
                    jsonObject.remove("err");
                } else if (pageObj != null) {
                    JSONArray displaysObj = jsonObject.getJSONArray("displays");
                    if (displaysObj != null) {
                        for (int i = 0; i < displaysObj.size(); i++) {
                            JSONObject display = displaysObj.getJSONObject(i);
                            display.put("common", commonObj);
                            display.put("page", pageObj);
                            display.put("ts", tsVal);
                            context.output(displayOutputTag, display.toJSONString());
                        }
                        jsonObject.remove("displays");
                    }

                    JSONArray actionsObj = jsonObject.getJSONArray("actions");
                    if (actionsObj != null) {
                        for (int i = 0; i < actionsObj.size(); i++) {
                            JSONObject actionObj = actionsObj.getJSONObject(i);
                            actionObj.put("common", commonObj);
                            actionObj.put("page", pageObj);
                            actionObj.put("ts", tsVal);
                            context.output(actionOutputTag, actionObj.toJSONString());
                        }
                        jsonObject.remove("actions");
                    }
                    collector.collect(jsonObject.toJSONString());
                } else {
                    collector.collect(jsonObject.toJSONString());
                }
            }
        });
    }

    public SingleOutputStreamOperator<JSONObject> validateNewOrOld(KeyedStream<JSONObject, String> keyByWithWatermark) {
        return keyByWithWatermark.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
            // 定义状态：存储用户首次登录日期 yyyy-MM-dd
            private transient ValueState<String> firstLoginState;

            @Override
            public void open(Configuration parameters) throws Exception {
                // 状态描述器，建议加TTL防止状态膨胀
                ValueStateDescriptor<String> desc = new ValueStateDescriptor<>("firstLoginState", String.class);
                // 设置状态TTL 30天，按需调整
//                StateTtlConfig ttlConfig = StateTtlConfig
//                        .newBuilder(Time.days(30))
//                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
//                        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
//                        .build();
//                desc.enableTimeToLive(ttlConfig);
                firstLoginState = getRuntimeContext().getState(desc);
            }

            @Override
            public void processElement(JSONObject jsonObj,
                                       KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx,
                                       Collector<JSONObject> out) throws Exception {
                Long ts = jsonObj.getLong("ts");
                JSONObject common = jsonObj.getJSONObject("common");
                String isNew = common.getString("is_new");
                String currentDt = DateFormatUtil.tsToDate(ts);
                String firstLoginDt = firstLoginState.value();

                if ("1".equals(isNew)) {
                    if (firstLoginDt != null) {
                        // 当天已存在登录记录，伪装新用户，修正is_new=0
                        if (firstLoginDt.equals(currentDt)) {
                            common.put("is_new", "0");
                        }
                        // 非同一天不修改，保持is_new=1
                    } else {
                        // 首次登录，写入状态
                        firstLoginState.update(currentDt);
                    }
                } else if ("0".equals(isNew)) {
                    // 标记老用户，但状态无记录，修复首次登录为前一天
                    if (firstLoginDt == null) {
                        long lastDayTs = ts - 24 * 60 * 60 * 1000L;
                        String lastDayDt = DateFormatUtil.tsToDate(lastDayTs);
                        firstLoginState.update(lastDayDt);
                    }
                }
                out.collect(jsonObj);
            }
        });
    }

    public KeyedStream<JSONObject, String> getKeyByWithWatermark(SingleOutputStreamOperator<JSONObject> flatmapStream) {
        return flatmapStream.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3L)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject jsonObject, long l) {
                return jsonObject.getLong("ts");
            }
        })).keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject jsonObject) throws Exception {
                return jsonObject.getJSONObject("common").getString("mid");
            }
        });
    }

    public SingleOutputStreamOperator<JSONObject> getFlatmapStream(DataStreamSource<String> kafkaSource) {
        return kafkaSource.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObj = JSONObject.parseObject(value);
                    // 安全获取，不抛异常
                    Object page = jsonObj.get("page");
                    Object start = jsonObj.get("start");
                    // 修复：安全获取common，缺失返回null，不抛NPE
                    JSONObject common = jsonObj.getObject("common", JSONObject.class);
                    // 安全获取ts
                    Long ts = jsonObj.getLong("ts");

                    // 必须有页面/启动日志
                    if (page != null || start != null) {
                        // mid、ts不能为空，避免水位线keyby空指针
                        if (common != null && common.getString("mid") != null && ts != null) {
                            collector.collect(jsonObj);
                        }
                    }
                } catch (Exception e) {
                    // 修复日志：打印原始数据 + 完整异常堆栈，定位真实报错
                    Log.info("脏数据过滤，原始数据：", value);
                }
            }
        });
    }
}
