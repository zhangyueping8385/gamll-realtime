package org.exaple.realtime.dwd.db.app;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.example.realtime.base.BaseSQLAPP;
import org.example.realtime.constant.Constant;

import java.time.Duration;

import static org.example.realtime.util.SQLUtil.getUpsertKafkaSinkSQl;

public class DwdTradeOrderDetail extends BaseSQLAPP {
    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv, String groupID) {
        // 1. 创建topic
        createTopicDB(groupID, tableEnv);
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5L));

        // 2. 筛选订单明细表
        Table odTable = getOdTable(tableEnv);

        tableEnv.createTemporaryView("order_detail", odTable);

        //3. 筛选订单信息表
        Table oiTable = getOiTable(tableEnv);

        tableEnv.createTemporaryView("order_info", oiTable);

        //4. 筛选订单详情活动关联表
        Table odaTable = getOdaTable(tableEnv);
        tableEnv.createTemporaryView("order_detail_activity", odaTable);

        //5. 筛选订单详情优惠券关联表
        Table odcTable = getOdcTable(tableEnv);
        tableEnv.createTemporaryView("order_detail_coupon", odcTable);

        //6. 合并四张表，订单明细表、订单信息表使用join
        Table joinTable = getJoinTable(tableEnv);

        //7. 写出到Kafka
        // 一旦使用到了left join，kafkaSink就得使用upsert kafka模式
        getTableResult(tableEnv);
        joinTable.insertInto(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL).execute().print();

    }

    public void getTableResult(StreamTableEnvironment tableEnv) {
        tableEnv.executeSql("create table " + Constant.TOPIC_DWD_TRADE_ORDER_DETAIL + " (\n" +
                "id STRING,\n" +
                "order_id STRING,\n" +
                "sku_id STRING,\n" +
                "activity_id STRING,\n" +
                "activity_rule_id STRING,\n" +
                "coupon_id STRING,\n" +
                "sku_name STRING,\n" +
                "order_price STRING,\n" +
                "sku_num STRING,\n" +
                "create_time STRING,\n" +
                "split_total_amount STRING,\n" +
                "split_activity_amount STRING,\n" +
                "split_coupon_amount STRING,\n" +
                "user_id STRING,\n" +
                "province_id STRING,\n" +
                "ts bigint,\n" +
                "PRIMARY KEY (id) NOT ENFORCED " +
                ") " + getUpsertKafkaSinkSQl(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL));
    }

    public Table getJoinTable(StreamTableEnvironment tableEnv) {
        return tableEnv.sqlQuery("select " +
                "od.id," +
                "od.order_id," +
                "oi.user_id," +
                "od.sku_id," +
                "od.sku_name," +
                "oi.province_id," +
                "act.activity_id," +
                "act.activity_rule_id," +
                "cou.coupon_id," +
                "date_format(od.create_time, 'yyyy-MM-dd') date_id," +  // 年月日
                "od.create_time," +
                "od.sku_num," +
                "od.split_activity_amount," +
                "od.split_coupon_amount," +
                "od.split_total_amount," +
                "od.ts " +
                "from order_detail od " +
                "join order_info oi on od.order_id=oi.id " +
                "left join order_detail_activity act " +
                "on od.id=act.order_detail_id " +
                "left join order_detail_coupon cou " +
                "on od.id=cou.order_detail_id ");
    }

    public Table getOdcTable(StreamTableEnvironment tableEnv) {
        return tableEnv.sqlQuery("select " +
                "data['order_detail_id'] order_detail_id, " +
                "data['coupon_id'] coupon_id " +
                "from topic_db " +
                "where `database`='gmall' " +
                "and `table`='order_detail_coupon' " +
                "and `type`='insert' ");
    }

    public Table getOdaTable(StreamTableEnvironment tableEnv) {
        return tableEnv.sqlQuery("select " +
                "data['order_detail_id'] order_detail_id, " +
                "data['activity_id'] activity_id, " +
                "data['activity_rule_id'] activity_rule_id " +
                "from topic_db " +
                "where `database`='gmall' " +
                "and `table`='order_detail_activity' " +
                "and `type`='insert' ");
    }

    public Table getOiTable(StreamTableEnvironment tableEnv) {
        return tableEnv.sqlQuery("select " +
                "data['id'] id," +
                "data['user_id'] user_id," +
                "data['province_id'] province_id," +
                "ts " +
                "from topic_db " +
                "where `database`='gmall' " +
                "and `table`='order_info' " +
                "and `type`='insert' ");
    }

    public Table getOdTable(StreamTableEnvironment tableEnv) {
        return tableEnv.sqlQuery("select \n" +
                "\t`data`['id'] id,\n" +
                "\t`data`['order_id'] order_id,\n" +
                "\t`data`['sku_id'] sku_id,\n" +
                "\t`data`['sku_name'] sku_name,\n" +
                "\t`data`['order_price'] order_price,\n" +
                "\t`data`['sku_num'] sku_num,\n" +
                "\t`data`['create_time'] create_time,\n" +
                "\t`data`['split_total_amount'] split_total_amount,\n" +
                "\t`data`['split_activity_amount'] split_activity_amount,\n" +
                "\t`data`['split_coupon_amount'] split_coupon_amount,\n" +
                "\t ts\n" +
                "from topic_db\n" +
                "where `database` = 'gmall'\n" +
                "\tand `table` = 'order_detail'\n" +
                "\tand `type` = 'insert'");
    }

    public static void main(String[] args) {
        new DwdTradeOrderDetail().Start(10014,4, Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);
    }
}
