package org.exaple.realtime.dwd.db.app;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.example.realtime.base.BaseSQLAPP;
import org.example.realtime.constant.Constant;

public class DwdTradeOrderDetail extends BaseSQLAPP {
    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv, String groupID) {
        // 1. 创建topic
        createTopicDB(groupID, tableEnv);

        tableEnv.executeSql()
    }

    public static void main(String[] args) {
        new DwdTradeOrderDetail().Start(10014,4, Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);
    }
}
