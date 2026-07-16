package org.example.realtime.dwd.db.app;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.example.realtime.base.BaseSQLAPP;
import org.example.realtime.constant.Constant;

public class DwdTradeOrderRefund extends BaseSQLAPP {
    public static void main(String[] args) {
        new DwdTradeOrderRefund().Start(10017,4, Constant.TOPIC_DWD_TRADE_ORDER_REFUND);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv, String ckAndGroupID) {
        //1. 读取topic_db表
        createTopicDB(ckAndGroupID, tableEnv);

        //2. 读取base_dic表
        createBaseDic(tableEnv);

        //3. 过滤退单表数据 order_refund_info   insert
        Table orderRefundInfo = tableEnv.sqlQuery(
                "select " +
                        "data['id'] id," +
                        "data['user_id'] user_id," +
                        "data['order_id'] order_id," +
                        "data['sku_id'] sku_id," +
                        "data['refund_type'] refund_type," +
                        "data['refund_num'] refund_num," +
                        "data['refund_amount'] refund_amount," +
                        "data['refund_reason_type'] refund_reason_type," +
                        "data['refund_reason_txt'] refund_reason_txt," +
                        "data['create_time'] create_time," +
                        "ts " +
                        "from topic_db " +
                        "where `database`='gmall' " +
                        "and `table`='order_refund_info' " +
                        "and `type`='insert' ");
        tableEnv.createTemporaryView("order_refund_info", orderRefundInfo);


        // 3. 过滤订单表中的退单数据: order_info  update
        Table orderInfo = tableEnv.sqlQuery(
                "select " +
                        "data['id'] id," +
                        "data['province_id'] province_id," +
                        "`old`['order_status'] " +
                        "from topic_db " +
                        "where `database`='gmall' " +
                        "and `table`='order_info' " +
                        "and `type`='update'" +
                        "and `old`['order_status'] is not null " +
                        "and `data`['order_status']='1005' ");
        tableEnv.createTemporaryView("order_info", orderInfo);
        tableEnv.executeSql("select * from order_info").print();

        // 4. join: 普通的和 lookup join
        Table result = tableEnv.sqlQuery(
                "select " +
                        "ri.id," +
                        "ri.user_id," +
                        "ri.order_id," +
                        "ri.sku_id," +
                        "oi.province_id," +
                        "date_format(ri.create_time,'yyyy-MM-dd') date_id," +
                        "ri.create_time," +
                        "ri.refund_type," +
                        "dic1.info.dic_name," +
                        "ri.refund_reason_type," +
                        "dic2.info.dic_name," +
                        "ri.refund_reason_txt," +
                        "ri.refund_num," +
                        "ri.refund_amount," +
                        "ri.ts " +
                        "from order_refund_info ri " +
                        "join order_info oi " +
                        "on ri.order_id=oi.id " +
                        "join base_dic for system_time as of ri.pt as dic1 " +
                        "on ri.refund_type=dic1.dic_code " +
                        "join base_dic for system_time as of ri.pt as dic2 " +
                        "on ri.refund_reason_type=dic2.dic_code ");
    }
}
