package org.example.realtime.dwd.db.split.app;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchemaBuilder;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.example.realtime.base.BaseAPP;
import org.example.realtime.bean.TableProcessDwd;
import org.example.realtime.constant.Constant;
import org.example.realtime.util.FlinkSinkUtil;
import org.example.realtime.util.FlinkSourceUtil;
import org.example.realtime.util.JdbcUtil;
import org.jline.utils.Log;

import javax.annotation.Nullable;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class DwdBaseDb extends BaseAPP {
    public static void main(String[] args) {
        new DwdBaseDb().Start(10019,4,"dwd_base_db", Constant.TOPIC_DB);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaSource) {
        //1. 过滤脏数据，保留json数据
        SingleOutputStreamOperator<JSONObject> filterStream = getFilterStream(kafkaSource).setParallelism(1);;
//        filterStream.print();

        //2. 读取mysql配置表数据，使用flink cdc读取, 读取table_process_dwd表
        DataStreamSource<String> tableProcessDwd = env.fromSource(FlinkSourceUtil.getMysqlSource(Constant.MYSQL_DIM_PROCESS_DATABASE, Constant.MYSQL_DWD_PROCESS_TABLE), WatermarkStrategy.noWatermarks(), "mysql_dim_process_dwd").setParallelism(1);
//        tableProcessDwd.print();

        //3. 转换数据格式
        SingleOutputStreamOperator<TableProcessDwd> processDwdStream = getTableProcessDwdSingleOutputStreamOperator(tableProcessDwd).setParallelism(1);;

        //4. 广播数据
        MapStateDescriptor<String, TableProcessDwd> mapStateDescriptor = new MapStateDescriptor<>("processDwd", String.class, TableProcessDwd.class);
        BroadcastStream<TableProcessDwd> broadcastStream = processDwdStream.broadcast(mapStateDescriptor);

        //5.链接主流和广播流, 合并数据, 并根据sourceTable和sourceType进行筛选, 只保留需要处理的表, 并添加sink_table字段
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> resultStream = processStream(filterStream, broadcastStream, mapStateDescriptor).setParallelism(1);;
//        resultStream.print();

        //6. 筛选最后需要写出的字段
        SingleOutputStreamOperator<JSONObject> dataStream = getDataStream(resultStream);
        dataStream.print();

        //7. 写出数据
        dataStream.sinkTo(FlinkSinkUtil.getKafkaSinkWithTopicList());

    }

    private static SingleOutputStreamOperator<JSONObject> getDataStream(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> resultStream) {
        return resultStream.map(new MapFunction<Tuple2<JSONObject, TableProcessDwd>, JSONObject>() {
            @Override
            public JSONObject map(Tuple2<JSONObject, TableProcessDwd> value) throws Exception {
                JSONObject jsonObj = value.f0;
                TableProcessDwd processDwd = value.f1;
                JSONObject data = jsonObj.getJSONObject("data");
                List<String> stringList = Arrays.asList(processDwd.getSinkColumns().split(","));
                data.keySet().removeIf(key -> !stringList.contains(key));
                data.put("sink_table", processDwd.getSinkTable());
                return data;
            }
        }).setParallelism(1);
    }

    public SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> processStream(SingleOutputStreamOperator<JSONObject> filterStream, BroadcastStream<TableProcessDwd> broadcastStream, MapStateDescriptor<String, TableProcessDwd> mapStateDescriptor) {
        return  filterStream.connect(broadcastStream)
                .process(new BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>() {
                    HashMap<String, TableProcessDwd> map = new HashMap<>();

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        Connection connection = JdbcUtil.getMysqlConnection();
                        List<TableProcessDwd> tableProcessDwds = JdbcUtil.queryList(connection, "select * from gmall_config.table_process_dwd", TableProcessDwd.class);
                        for (TableProcessDwd tableProcessDwd : tableProcessDwds) {
                            map.put(tableProcessDwd.getSourceTable() + ":" + tableProcessDwd.getSourceType(), tableProcessDwd);
                        }
                    }

                    //  调用广播状态判断当前数据时候需要保留
                    @Override
                    public void processElement(JSONObject value, BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>.ReadOnlyContext readOnlyContext, Collector<Tuple2<JSONObject, TableProcessDwd>> collector) throws Exception {
                        String key = value.getString("table") + ":" + value.getString("type");
                        ReadOnlyBroadcastState<String, TableProcessDwd> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);
                        TableProcessDwd processDwd = broadcastState.get(key);

                        // 二次判断是否为先到的数据
                        if (processDwd == null) {
                            processDwd = map.get(key);
                        }

                        if (processDwd != null) {
                            collector.collect(Tuple2.of(value, processDwd));
                        }
                    }

                    // 将广播数据写入广播状态中
                    @Override
                    public void processBroadcastElement(TableProcessDwd value, BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>.Context context, Collector<Tuple2<JSONObject, TableProcessDwd>> collector) throws Exception {
                        BroadcastState<String, TableProcessDwd> broadcastState = context.getBroadcastState(mapStateDescriptor);
                        String op = value.getOp();
                        String key = value.getSourceTable() + ":" + value.getSourceType();
                        if ("d".equals(op)) {
                            broadcastState.remove(key);
                            map.remove(key);
                        } else {
                            broadcastState.put(key, value);
                        }
                    }

                });
    }

    private static SingleOutputStreamOperator<TableProcessDwd> getTableProcessDwdSingleOutputStreamOperator(DataStreamSource<String> tableProcessDwd) {
        SingleOutputStreamOperator<TableProcessDwd> processDwdStream = tableProcessDwd.flatMap(new FlatMapFunction<String, TableProcessDwd>() {
            @Override
            public void flatMap(String s, Collector<TableProcessDwd> collector) throws Exception {
                try {
                    JSONObject jsonObje = JSONObject.parseObject(s);
                    String op = jsonObje.getString("op");
                    TableProcessDwd ProcessDwd;
                    if ("d".equals(op)) {
                        ProcessDwd = jsonObje.getObject("before", TableProcessDwd.class);
                    } else {
                        ProcessDwd = jsonObje.getObject("after", TableProcessDwd.class);
                    }
                    ProcessDwd.setOp(op);
                    collector.collect(ProcessDwd);
                } catch (Exception e) {
                    Log.info("捕获脏数据：" + s);
                }
            }
        });
        return processDwdStream;
    }

    private static SingleOutputStreamOperator<JSONObject> getFilterStream(DataStreamSource<String> kafkaSource) {
        return kafkaSource.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObj = JSONObject.parseObject(s);
                    collector.collect(jsonObj);
                } catch (Exception e) {
                    Log.info("过滤脏数据：" + s);
                }
            }
        });
    }
}
