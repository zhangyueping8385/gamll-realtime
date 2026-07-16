package org.example.realtime;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.example.realtime.base.BaseAPP;
import org.example.realtime.bean.TableProcessDim;
import org.example.realtime.constant.Constant;
import org.example.realtime.function.DimBaseSinkFunction;
import org.example.realtime.function.DimBroadCastFunction;
import org.example.realtime.util.FlinkSourceUtil;
import org.example.realtime.util.HbaseUtil;
import org.example.realtime.util.JdbcUtil;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.example.realtime.constant.Constant.MYSQL_DIM_PROCESS_DATABASE;
import static org.example.realtime.constant.Constant.MYSQL_DIM_PROCESS_TABLE;

public class DimAPP extends BaseAPP {
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaSource) {
        //校心业务逻糊
        // 1.对ods读取的原始数据进行数無清法
        SingleOutputStreamOperator jsonObjStream = etl(kafkaSource).setParallelism(1);

        // 2. 使川fLinKCDC读取监控配費表数据
        MySqlSource<String> mySqlSource = FlinkSourceUtil.getMysqlSource(MYSQL_DIM_PROCESS_DATABASE, MYSQL_DIM_PROCESS_TABLE);
        DataStreamSource<String> mysqlSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql-source").setParallelism(1);

        // 3. 在HBase 中创建維度表
        SingleOutputStreamOperator createHbaseTableStream = CreateHbaseTableStream(mysqlSource);
//        createHbaseTableStream.print();

        // 4.做成广播流
        MapStateDescriptor<String, TableProcessDim> broadcaseState = new MapStateDescriptor<>("broadcase_state", String.class, TableProcessDim.class);
        BroadcastStream broadcaseStateStream = createHbaseTableStream.broadcast(broadcaseState);

        // 5.链接主流和广播流
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> connectionStream = dimStream(jsonObjStream, broadcaseStateStream, broadcaseState).setParallelism(1);
//        connectionStream.print();

        // 6. 筛选出需要写出的字段
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> mappedDataStream = mapDataStream(connectionStream).setParallelism(1);

        // 7. 写出到HBase
        mappedDataStream.print();
        mappedDataStream.addSink(new DimBaseSinkFunction()).setParallelism(1);

    }

    public SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> mapDataStream(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> connectionStream) {
        return connectionStream.map(new MapFunction<Tuple2<JSONObject, TableProcessDim>, Tuple2<JSONObject, TableProcessDim>>() {
            @Override
            public Tuple2<JSONObject, TableProcessDim> map(Tuple2<JSONObject, TableProcessDim> value) throws Exception {
                JSONObject jsonObj = value.f0;
                TableProcessDim dim = value.f1;
                String sinkColumns = dim.getSinkColumns();
                List<String> list = Arrays.asList(sinkColumns.split(","));
                JSONObject data = jsonObj.getJSONObject("data");
                data.keySet().removeIf(key -> !list.contains(key));
                return value;
            }
        });
    }

    public SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimStream(SingleOutputStreamOperator jsonObjStream, BroadcastStream broadcaseStateStream, MapStateDescriptor<String, TableProcessDim> broadcaseState) {
        return jsonObjStream.connect(broadcaseStateStream).process(new DimBroadCastFunction(broadcaseState));
    };


    public SingleOutputStreamOperator CreateHbaseTableStream(DataStreamSource<String> mysqlSource) {
        return mysqlSource.flatMap(new RichFlatMapFunction<String, TableProcessDim>() {
            public Connection connection;

            @Override
            public void open(Configuration parameters) throws Exception {
                connection = HbaseUtil.getConnection();
                if (connection == null || connection.isClosed()) {
                    throw new RuntimeException("HBase连接失败，请检查Zookeeper地址和HBase服务状态");
                }
                System.out.println("HBase连接成功");
            }


            @Override
            public void flatMap(String s, Collector<TableProcessDim> collector) throws Exception {
                try {
                    JSONObject jsonObj = JSONObject.parseObject(s);
                    String op = jsonObj.getString("op");
                    TableProcessDim dim;
                    if ("d".equals(op)) {
                        dim = jsonObj.getObject("before", TableProcessDim.class);
                        deleteTable(dim);
                    } else if ("c".equals(op) || "r".equals(op)) {
                        dim = jsonObj.getObject("after", TableProcessDim.class);
                        createTable(dim);
                    } else {
                        dim = jsonObj.getObject("after", TableProcessDim.class);
                        deleteTable(dim);
                        createTable(dim);
                    }
                    dim.setOp(op);
                    collector.collect(dim);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            private void createTable(TableProcessDim after) {
                try {
                    HbaseUtil.CreateTable(connection, Constant.HBASE_NAMESPACE, after.getSinkTable(), after.getSinkFamily());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            private void deleteTable(TableProcessDim before) {
                try {
                    HbaseUtil.dropTable(connection, Constant.HBASE_NAMESPACE, before.getSinkTable());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void close() throws Exception {
                HbaseUtil.closeConnection(connection);
            }
        }).setParallelism(1);
    }


    public SingleOutputStreamOperator etl(DataStreamSource<String> kafkaSource) {
        SingleOutputStreamOperator<JSONObject> jsonObjStream = kafkaSource.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObj = JSONObject.parseObject(value);
                    String database = jsonObj.getString("database");
                    String type = jsonObj.getString("type");
                    JSONObject data = jsonObj.getJSONObject("data");
                    System.out.println("ETL收到: database=" + database + ", table=" + jsonObj.getString("table") + ", type=" + type);
                    
                    if ("gmall".equals(database) && data != null) {
                        if ("insert".equals(type) || "update".equals(type) || "delete".equals(type) || "bootstrap-insert".equals(type)) {
                            System.out.println("ETL通过: " + jsonObj.getString("table"));
                            collector.collect(jsonObj);
                        }
                    }
                } catch (Exception e) {
                    System.err.println("ETL解析失败: " + e.getMessage());
                    e.printStackTrace();
                }
            }
        });
        return jsonObjStream;
    }

    public static void main(String[] args) {
        new DimAPP().Start(10001,4,"dim_app", Constant.TOPIC_DB);
    }
}