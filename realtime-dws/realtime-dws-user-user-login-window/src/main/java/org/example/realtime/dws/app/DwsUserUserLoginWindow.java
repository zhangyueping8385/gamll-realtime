package org.example.realtime.dws.app;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateDescriptor;
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
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.example.realtime.base.BaseAPP;
import org.example.realtime.bean.UserLoginBean;
import org.example.realtime.constant.Constant;
import org.example.realtime.function.DorisMapFunction;
import org.example.realtime.util.DateFormatUtil;
import org.example.realtime.util.FlinkSinkUtil;
import org.jline.utils.Log;

import java.time.Duration;

/**
 * 1）读取 Kafka 页面主题数据
 * 2）转换数据结构
 * 流中数据由 String 转换为 JSONObject。
 * 3）过滤数据
 * 统计的指标与用户有关，uid 不为 null 的数据才是有用的。
 * 此外，登录分为两种情况：
 * 用户打开应用后自动登录
 * 用户打开应用后没有登录，浏览部分页面后跳转到登录页面，中途登录
 * 对于情况1，登录操作发生在会话首页，所以保留首页即可；
 * 对于情况2，登录操作发生在 login 页面，login页面之后必然会跳转到其它页面，保留login之后的页面即可记录情况2的登录操作。
 * 综上，我们应保留 uid 不为 null 且 last_page_id 为 null 或 last_page_id 为 login 的浏览记录。
 * 4）设置水位线
 * 5）按照 uid 分组
 * 不同用户的登录记录互不相干，各自处理。
 * 6）统计回流用户数和独立用户数
 * 运用 Flink 状态编程，记录用户末次登录日期。
 * 若状态中的末次登录日期不为 null，进一步判断。
 * 如果末次登录日期不等于当天日期则独立用户数 uuCt 记为 1，并将状态中的末次登录日期更新为当日，进一步判断。
 * 如果当天日期与末次登录日期之差大于等于8天则回流用户数backCt置为1。
 * 否则 backCt 置为 0。
 * 若末次登录日期为当天，则 uuCt 和 backCt 均为 0，此时本条数据不会影响统计结果，舍弃，不再发往下游。
 * 如果状态中的末次登录日期为 null，将 uuCt 置为 1，backCt 置为 0，并将状态中的末次登录日期更新为当日。
 * 7）开窗，聚合
 * 度量字段求和，补充窗口起始和结束时间，统计日期字段，用于Doris分区。
 * 8）写入 Doris
 */
public class DwsUserUserLoginWindow extends BaseAPP {
    public static void main(String[] args) {
        new DwsUserUserLoginWindow().Start(10024,4, Constant.DORIS_DWS_USER_USER_LOGIN_WINDOW,Constant.TOPIC_DWD_TRAFFIC_PAGE);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaSource) {
//        kafkaSource.print();

        //1. 转换数据结构
        SingleOutputStreamOperator<JSONObject> flatMapStream = getFlatMapStream(kafkaSource);

        //2. 设置水位线
        SingleOutputStreamOperator<JSONObject> watermarkStream = getTs(flatMapStream);

        //3. 按照 uid 分组
        KeyedStream<JSONObject, String> keyedStream = getKeyedStream(watermarkStream);

        //4. 统计回流用户数和独立用户数
        SingleOutputStreamOperator<UserLoginBean> processStream = getprocessStream(keyedStream);

        //5. 开窗，聚合, 补充窗口起始和结束时间，统计日期字段，用于Doris分区。
        SingleOutputStreamOperator<UserLoginBean> reduceAndWindowStream = getReduceAndWindowStream(processStream);

        //6. 转换数据类型
        SingleOutputStreamOperator<String> mappedStream = reduceAndWindowStream.map(new DorisMapFunction<>());
        mappedStream.print();

        //7. 写入 Doris
        mappedStream.sinkTo(FlinkSinkUtil.getDorisSink(Constant.DORIS_DWS_USER_USER_LOGIN_WINDOW));
    }

    private static SingleOutputStreamOperator<UserLoginBean> getReduceAndWindowStream(SingleOutputStreamOperator<UserLoginBean> processStream) {
        return processStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(10L)))
                .reduce(new ReduceFunction<UserLoginBean>() {
                    @Override
                    public UserLoginBean reduce(UserLoginBean v1, UserLoginBean v2) throws Exception {
                        v1.setUuCt(v1.getUuCt() + v2.getUuCt());
                        v1.setBackCt(v1.getBackCt() + v2.getBackCt());
                        return v1;
                    }
                }, new ProcessAllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>.Context context, Iterable<UserLoginBean> iterable, Collector<UserLoginBean> collector) throws Exception {
                        long start = context.window().getStart();
                        long end = context.window().getEnd();
                        String stt = DateFormatUtil.tsToDateTime(start);
                        String ett = DateFormatUtil.tsToDateTime(end);
                        String curDate = DateFormatUtil.tsToDateForPartition(System.currentTimeMillis());

                        for (UserLoginBean element : iterable) {
                            element.setStt(stt);
                            element.setEdt(ett);
                            element.setCurDate(curDate);
                            collector.collect(element);
                        }
                    }
                });
    }

    private static SingleOutputStreamOperator<UserLoginBean> getprocessStream(KeyedStream<JSONObject, String> keyedStream) {
        return keyedStream.process(new ProcessFunction<JSONObject, UserLoginBean>() {
            ValueState<String> lastLoginDtState;

            @Override
            public void open(Configuration parameters) throws Exception {
                // 初始化状态, 末次登录日期
                ValueStateDescriptor<String> lastLoginDesc = new ValueStateDescriptor("lastLoginDate", String.class);
                lastLoginDtState = getRuntimeContext().getState(lastLoginDesc);
            }

            @Override
            public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, UserLoginBean>.Context context, Collector<UserLoginBean> collector) throws Exception {
                // 比较当前日期与末次登录日期是否相等
                // 回流用户数
                Long backCt = 0L;
                // 独立用户数
                Long uuCt = 0L;
                String lastLoginDt = lastLoginDtState.value();
                Long ts = jsonObject.getLong("ts");
                String curDate = DateFormatUtil.tsToDate(ts);

                if (lastLoginDt == null || !lastLoginDt.equals(curDate)) { // 判断独用户
                    uuCt = 1L;
                }

                if (lastLoginDt != null && ts - DateFormatUtil.dateToTs(lastLoginDt) > 7 * 24 * 60 * 60 * 1000L) {   // 判断回流用户
                    backCt = 1L;
                }
                lastLoginDtState.update(curDate);

                if (uuCt != 0){
                    collector.collect(new UserLoginBean().builder()
                            .uuCt(uuCt)
                            .backCt(backCt)
                            .ts(ts)
                            .curDate(curDate)
                            .build());
                }

            }
        });
    }

    private static KeyedStream<JSONObject, String> getKeyedStream(SingleOutputStreamOperator<JSONObject> watermarkStream) {
        return watermarkStream.keyBy(key -> key.getJSONObject("common").getString("uid"));
    }

    private static SingleOutputStreamOperator<JSONObject> getTs(SingleOutputStreamOperator<JSONObject> flatMapStream) {
        return flatMapStream.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3L)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
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
                    JSONObject common = jsonObject.getJSONObject("common");
                    JSONObject page = jsonObject.getJSONObject("page");
                    String uid = common.getString("uid");
                    String last_page_id = page.getString("last_page_id");
                    if (uid != null && (last_page_id == null || "login".equals(last_page_id))) {
                        collector.collect(jsonObject);
                    }
                } catch (Exception e) {
                    Log.info("转换数据结构失败，数据：" + s);
                }
            }
        });
    }
}
