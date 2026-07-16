package org.example.realtime.function;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.example.realtime.bean.TableProcessDim;
import org.example.realtime.util.JdbcUtil;

import java.util.HashMap;
import java.util.List;

import static org.example.realtime.constant.Constant.MYSQL_DIM_PROCESS_DATABASE;
import static org.example.realtime.constant.Constant.MYSQL_DIM_PROCESS_TABLE;

public class DimBroadCastFunction extends BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>> {

    public HashMap<String, TableProcessDim> hashMap = new HashMap<>();
    public MapStateDescriptor<String,TableProcessDim> broadcaseState;

    public DimBroadCastFunction(MapStateDescriptor<String, TableProcessDim> broadcaseState) {
        this.broadcaseState = broadcaseState;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        java.sql.Connection mysqlConnection = JdbcUtil.getMysqlConnection();
        List<TableProcessDim> tableProcessDims = JdbcUtil.queryList(mysqlConnection, "select * from " + MYSQL_DIM_PROCESS_DATABASE + "." + MYSQL_DIM_PROCESS_TABLE, TableProcessDim.class, true);

        for (TableProcessDim tableProcessDim : tableProcessDims) {
            tableProcessDim.setOp("r");
            hashMap.put(tableProcessDim.getSourceTable(), tableProcessDim);
        }
        JdbcUtil.closeConnection(mysqlConnection);
    }

    /**
     * 处理广播流数据
     * @param value
     * @param context
     * @param collector
     * @throws Exception
     */
    @Override
    public void processBroadcastElement(TableProcessDim value, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.Context context, Collector<Tuple2<JSONObject, TableProcessDim>> collector) throws Exception {
        // 从广播流中获取配置表数据
        BroadcastState<String, TableProcessDim> broadcastState = context.getBroadcastState(broadcaseState);
        String op = value.getOp();
        if ("d".equals(op)) {
            broadcastState.remove(value.getSourceTable());
            // 同步删除hashMap中初始化加载的配置表数据
            hashMap.remove(value.getSourceTable());
        } else {
            broadcastState.put(value.getSourceTable(), value);
        }
    }


    /**
     * 处理主流数据
     * @param value
     * @param ctx
     * @param collector
     * @throws Exception
     */
    @Override
    public void processElement(JSONObject value, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.ReadOnlyContext ctx, Collector<Tuple2<JSONObject, TableProcessDim>> collector) throws Exception {
        // 从广播流中获取配置表数据
        ReadOnlyBroadcastState<String, TableProcessDim> broadcastState = ctx.getBroadcastState(broadcaseState);
        String tableName = value.getString("table");
        System.out.println("Broadcast收到主流数据，tableName=" + tableName);
        TableProcessDim tableProcessDim = broadcastState.get(tableName);

        // 如果数据到的太早，导致状态为空，从hashMap中获取
        if (tableProcessDim == null){
            tableProcessDim = hashMap.get(tableName);
            System.out.println("从hashMap获取配置: " + tableProcessDim);
        }

        if (tableProcessDim != null) {
            System.out.println("匹配成功，输出到下游: " + tableProcessDim.getSinkTable());
            collector.collect(Tuple2.of(value, tableProcessDim));
        } else {
            System.out.println("未找到匹配的配置，丢弃数据: tableName=" + tableName);
        }

    }
}