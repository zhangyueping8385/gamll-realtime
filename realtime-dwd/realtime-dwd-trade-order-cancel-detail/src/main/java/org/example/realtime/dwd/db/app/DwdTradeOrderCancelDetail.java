package org.example.realtime.dwd.db.app;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.example.realtime.base.BaseSQLAPP;
import org.example.realtime.constant.Constant;
import org.example.realtime.util.SQLUtil;

import java.time.Duration;

public class DwdTradeOrderCancelDetail extends BaseSQLAPP {
    public static void main(String[] args) {
        new DwdTradeOrderCancelDetail().Start(10015,4,"dwd_trade_order_cancel_detail");
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv, String groupID) {
        tableEnv.getConfig().setIdleStateRetention(Duration.ofMinutes(15 * 60 + 5));
        //1. 创建topic数据库
        createTopicDB(groupID, tableEnv);

        //2.读取 dwd 层下单事务事实表数据
        tableEnv.executeSql("create table "+Constant.TOPIC_DWD_TRADE_ORDER_DETAIL+"(" +
                        "id string," +
                        "order_id string," +
                        "user_id string," +
                        "sku_id string," +
                        "sku_name string," +
                        "province_id string," +
                        "activity_id string," +
                        "activity_rule_id string," +
                        "coupon_id string," +
                        "date_id string," +
                        "create_time string," +
                        "sku_num string," +
                        "split_original_amount string," +
                        "split_activity_amount string," +
                        "split_coupon_amount string," +
                        "split_total_amount string," +
                        "ts bigint " +
                        ")" + SQLUtil.getKafkaSourceSQL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL, Constant.TOPIC_DWD_TRADE_ORDER_CANCEL_DETAIL));

        // 3. 从 topic_db 过滤出订单取消数据
        Table orderCancel = tableEnv.sqlQuery("select " +
                " `data`['id'] id, " +
                " `data`['operate_time'] operate_time, " +
                " `ts` " +
                "from topic_db " +
                "where `database`='gmall' " +
                "and `table`='order_info' " +
                "and `type`='update' " +
                "and `old`['order_status']='1001' " +
                "and `data`['order_status']='1003' ");
//        orderCancel.execute().print();
        tableEnv.createTemporaryView("order_cancel", orderCancel);

//        // 4. 订单取消表和下单表进行 join
        Table result = tableEnv.sqlQuery(
                "select " +
                        "od.id," +
                        "od.order_id," +
                        "od.user_id," +
                        "od.sku_id," +
                        "od.sku_name," +
                        "od.province_id," +
                        "od.activity_id," +
                        "od.activity_rule_id," +
                        "od.coupon_id," +
                        "date_format(oc.operate_time, 'yyyy-MM-dd') order_cancel_date_id," +
                        "oc.operate_time," +
                        "od.sku_num," +
                        "od.split_original_amount," +
                        "od.split_activity_amount," +
                        "od.split_coupon_amount," +
                        "od.split_total_amount," +
                        "oc.ts " +
                        "from dwd_trade_order_detail od " +
                        "join order_cancel oc " +
                        "on od.order_id=oc.id ");
//        result.execute().print();

        // 5. 写出
        tableEnv.executeSql(
                "create table "+Constant.TOPIC_DWD_TRADE_ORDER_CANCEL_DETAIL+"(" +
                        "id string," +
                        "order_id string," +
                        "user_id string," +
                        "sku_id string," +
                        "sku_name string," +
                        "province_id string," +
                        "activity_id string," +
                        "activity_rule_id string," +
                        "coupon_id string," +
                        "date_id string," +
                        "cancel_time string," +
                        "sku_num string," +
                        "split_original_amount string," +
                        "split_activity_amount string," +
                        "split_coupon_amount string," +
                        "split_total_amount string," +
                        "ts bigint " +
                        ")" + SQLUtil.getKafkaSinkSQL(Constant.TOPIC_DWD_TRADE_ORDER_CANCEL_DETAIL));

        result.insertInto(Constant.TOPIC_DWD_TRADE_ORDER_CANCEL_DETAIL).execute();
    }



}
