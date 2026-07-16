package org.example.realtime.function;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.hadoop.hbase.client.Connection;
import org.example.realtime.bean.TableProcessDim;
import org.example.realtime.constant.Constant;
import org.example.realtime.util.HbaseUtil;

import java.io.IOException;

public class DimBaseSinkFunction extends RichSinkFunction<Tuple2<JSONObject, TableProcessDim>> {
    public Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = HbaseUtil.getConnection();
    }

    @Override
    public void close() throws Exception {
        HbaseUtil.closeConnection(connection);
    }

    @Override
    public void invoke(Tuple2<JSONObject, TableProcessDim> value, SinkFunction.Context context) throws Exception {
        JSONObject jsonObj = value.f0;
        TableProcessDim dim = value.f1;
        String type = jsonObj.getString("type");
        JSONObject data = jsonObj.getJSONObject("data");
        System.out.println("Sink收到数据: type=" + type + ", table=" + dim.getSinkTable() + ", data=" + data);
        if ("delete".equals(type)) {
            delete(data, dim);
        } else {
            put(data, dim);
        }
    }

    private void put(JSONObject data, TableProcessDim dim) {
        String sinkTable = dim.getSinkTable();
        String sinkRowKeyName = dim.getSinkRowKey();
        String sinkRowKeyValue = data.getString(sinkRowKeyName);
        String sinkFamily = dim.getSinkFamily();
        System.out.println("准备写入HBase: namespace=gmall, table=" + sinkTable + ", rowKey=" + sinkRowKeyValue);
        try {
            HbaseUtil.putCells(connection, Constant.HBASE_NAMESPACE, sinkTable, sinkRowKeyValue, sinkFamily, data);
            System.out.println("成功写入HBase: table=" + sinkTable + ", rowKey=" + sinkRowKeyValue);
        } catch (IOException e) {
            System.err.println("写入HBase失败: table=" + sinkTable + ", error=" + e.getMessage());
            e.printStackTrace();
        }
    }

    private void delete(JSONObject data, TableProcessDim dim) {
        String sinkTable = dim.getSinkTable();
        String sinkRowKeyName = dim.getSinkRowKey();
        String sinkRowKeyValue = data.getString(sinkRowKeyName);
        try {
            HbaseUtil.deleteCells(connection, Constant.HBASE_NAMESPACE, sinkTable, sinkRowKeyValue);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}