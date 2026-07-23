package org.example.realtime.dws.app;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.security.User;
import org.example.realtime.base.BaseAPP;
import org.example.realtime.bean.UserRegisterBean;
import org.example.realtime.constant.Constant;
import org.example.realtime.function.DorisMapFunction;
import org.example.realtime.util.DateFormatUtil;
import org.example.realtime.util.FlinkSinkUtil;
import org.jline.utils.Log;

import java.time.Duration;

/**
 * 从 DWD层用户注册表中读取数据，统计各窗口注册用户数，写入 Doris。
 * 1）读取Kafka用户注册主题数据
 * 2）转换数据结构
 * String 转换为 JSONObject。
 * 3）设置水位线
 * 4）开窗、聚合
 * 5）写入 Doris
 * @author xiaozhang
 * @date 2023/12/12 10:00
 * @description 用户注册窗口
 */
public class DwsUserUserRegisterWindow extends BaseAPP {
    public static void main(String[] args) {
        new DwsUserUserRegisterWindow().Start(10025,4, Constant.DORIS_DWS_USER_USER_REGISTER_WINDOW,Constant.TOPIC_DWD_USER_REGISTER);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaSource) {
        //1.转换数据结构
        SingleOutputStreamOperator<UserRegisterBean> flatMapStream = getFlatMapStream(kafkaSource);

        //2. 设置水位线
        SingleOutputStreamOperator<UserRegisterBean> watermarkStream = getCreateTime(flatMapStream);

        //3. 开窗、聚合
        SingleOutputStreamOperator<UserRegisterBean> windowAndReduceStream = getWindowAndReduceStream(watermarkStream);
//        windowAndReduceStream.print();

        //5.转换格式
        SingleOutputStreamOperator<String> mappedStream = windowAndReduceStream.map(new DorisMapFunction<>());
        mappedStream.print();

        //6.写入 Doris
        mappedStream.sinkTo(FlinkSinkUtil.getDorisSink(Constant.DORIS_DWS_USER_USER_REGISTER_WINDOW));
    }

    private static SingleOutputStreamOperator<UserRegisterBean> getWindowAndReduceStream(SingleOutputStreamOperator<UserRegisterBean> watermarkStream) {
        return watermarkStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(10L)))
                .reduce(new ReduceFunction<UserRegisterBean>() {
                    @Override
                    public UserRegisterBean reduce(UserRegisterBean v1, UserRegisterBean v2) throws Exception {
                        v1.setRegisterCt(v1.getRegisterCt() + v2.getRegisterCt());
                        return v1;
                    }
                }, new ProcessAllWindowFunction<UserRegisterBean, UserRegisterBean, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<UserRegisterBean, UserRegisterBean, TimeWindow>.Context context, Iterable<UserRegisterBean> iterable, Collector<UserRegisterBean> collector) throws Exception {
                        String start = DateFormatUtil.tsToDateTime(context.window().getStart());
                        String end = DateFormatUtil.tsToDateTime(context.window().getEnd());
                        String curDt = DateFormatUtil.tsToDateForPartition(System.currentTimeMillis());
                        for (UserRegisterBean element : iterable) {
                            element.setStt(start);
                            element.setEdt(end);
                            element.setCurDate(curDt);
                            collector.collect(element);
                        }
                    }
                });
    }

    private static SingleOutputStreamOperator<UserRegisterBean> getCreateTime(SingleOutputStreamOperator<UserRegisterBean> flatMapStream) {
        return flatMapStream.assignTimestampsAndWatermarks(WatermarkStrategy.<UserRegisterBean>forBoundedOutOfOrderness(Duration.ofSeconds(10L)).withTimestampAssigner(new SerializableTimestampAssigner<UserRegisterBean>() {
            @Override
            public long extractTimestamp(UserRegisterBean jsonObject, long l) {
                return DateFormatUtil.dateTimeToTs(jsonObject.getCreate_time());
            }
        }));
    }

    private static SingleOutputStreamOperator<UserRegisterBean> getFlatMapStream(DataStreamSource<String> kafkaSource) {
        return kafkaSource.flatMap(new FlatMapFunction<String, UserRegisterBean>() {
            @Override
            public void flatMap(String s, Collector<UserRegisterBean> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(s);
                    String create_time = jsonObject.getString("create_time");
                    String id = jsonObject.getString("id");
                    if (create_time != null && id != null) {
                        collector.collect(new UserRegisterBean().builder()
                                        .registerCt(1L)
                                        .create_time(create_time)
                                .build());
                    }
                } catch (Exception e) {
                    Log.info("转换数据结构失败，数据：" + s);
                }
            }
        });
    }
}
