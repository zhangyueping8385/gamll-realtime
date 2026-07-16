package org.example.realtime.dwd.db.app;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.example.realtime.base.BaseSQLAPP;
import org.example.realtime.constant.Constant;
import org.example.realtime.util.SQLUtil;

import static org.example.realtime.util.SQLUtil.getKafkaSourceSQL;
import static org.example.realtime.util.SQLUtil.getUpsertKafkaSinkSQl;

public class DwdTradeOrderPaySucDetail extends BaseSQLAPP {
    public static void main(String[] args) {
        new DwdTradeOrderPaySucDetail().Start(10016,4, Constant.TOPIC_DWD_TRADE_ORDER_PAY_SUC_DETAIL);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv, String ckAndGroupID) {
        // 1. 从kafka读取数据topic_db
        createTopicDB(ckAndGroupID, tableEnv);

        // 2. 从topic_db中读取数据，筛选出支付成功订单
        Table paymentInfoTable = getPaymentInfoTable(tableEnv);
        tableEnv.createTemporaryView("payment_info", paymentInfoTable);

        // 3. 从topic_db中读取数据，筛选出订单详情
        orderDetailTable(tableEnv, ckAndGroupID);

        // 4. 从hbase中读取base_dic纬度数据
        createBaseDic(tableEnv);
//        tableEnv.executeSql("select * from base_dic").print();

        // 5. 合并支付成功订单和订单详情
        Table payOrderTable = getPayOrderTable(tableEnv);
        tableEnv.createTemporaryView("pay_order", payOrderTable);

        // 6. 合并支付成功订单和订单详情
        Table resultTable = getLeftJoinTable(tableEnv);
        tableEnv.createTemporaryView("left_join", resultTable);
//        tableEnv.executeSql("select * from left_join").print();


        // 7. 写出到 kafka 中
        dwdTradeOrderPaySucDetailTable(tableEnv);
        resultTable.insertInto(Constant.TOPIC_DWD_TRADE_ORDER_PAY_SUC_DETAIL).execute();

    }

    public void dwdTradeOrderPaySucDetailTable(StreamTableEnvironment tableEnv) {
// 7. 创建upsert kafka写出
        tableEnv.executeSql("create table " + Constant.TOPIC_DWD_TRADE_ORDER_PAY_SUC_DETAIL + "(\n" +
                " id STRING,\n" +
                " order_id STRING,\n" +
                " user_id STRING,\n" +
                " payment_type_code STRING,\n" +
                " payment_type_name STRING,\n" +
                " payment_time STRING,\n" +
                " sku_id STRING,\n" +
                " province_id STRING,\n" +
                " activity_id STRING,\n" +
                " activity_rule_id STRING,\n" +
                " coupon_id STRING,\n" +
                " sku_name STRING,\n" +
                " order_price STRING,\n" +
                " sku_num STRING,\n" +
                " split_total_amount STRING,\n" +
                " split_activity_amount STRING,\n" +
                " split_coupon_amount STRING,\n" +
                " ts bigint ,\n" +
                " PRIMARY KEY (id) NOT ENFORCED \n" +
                ")" + SQLUtil.getUpsertKafkaSinkSQl(Constant.TOPIC_DWD_TRADE_ORDER_PAY_SUC_DETAIL));
    }

    public Table getLeftJoinTable(StreamTableEnvironment tableEnv) {
        return tableEnv.sqlQuery("SELECT\n" +
                "\tid,\n" +
                "\torder_id,\n" +
                "\tuser_id,\n" +
                "\tpayment_type payment_type_code,\n" +
                "\tinfo.dic_name payment_type_name,\n" +
                "\tpayment_time,\n" +
                "\tsku_id,\n" +
                "\tprovince_id,\n" +
                "\tactivity_id,\n" +
                "\tactivity_rule_id,\n" +
                "\tcoupon_id,\n" +
                "\tsku_name,\n" +
                "\torder_price,\n" +
                "\tsku_num,\n" +
                "\tsplit_total_amount,\n" +
                "\tsplit_activity_amount,\n" +
                "\tsplit_coupon_amount,\n" +
                "\tts\n" +
                "FROM \n" +
                "\tpay_order p\n" +
                "left join\n" +
                "\tbase_dic FOR SYSTEM_TIME AS OF p.proc_time AS b\n" +
                "ON\n" +
                " \tp.payment_type = b.rowkey");
    }

    public Table getPayOrderTable(StreamTableEnvironment tableEnv) {
        return tableEnv.sqlQuery("SELECT \n" +
                "\tod.id,\n" +
                "\tp.order_id,\n" +
                "\tp.user_id,\n" +
                "\tpayment_type,\n" +
                "\tcallback_time payment_time,\n" +
                "\tsku_id,\n" +
                "\tactivity_id,\n" +
                "\tactivity_rule_id,\n" +
                "\tcoupon_id,\n" +
                "\tsku_name,\n" +
                "\torder_price,\n" +
                "\tsku_num,\n" +
                "\tsplit_total_amount,\n" +
                "\tsplit_activity_amount,\n" +
                "\tsplit_coupon_amount,\n" +
                "\tprovince_id,\n" +
                "\tp.ts,\n" +
                "\tp.proc_time\n" +
                "FROM\n" +
                "\tpayment_info p, order_detail od\n" +
                "WHERE\n" +
                "    p.order_id = od.order_id\n" +
                "    AND p.row_time >= od.row_time\n" +
                "    AND p.row_time <= od.row_time + INTERVAL '24' HOUR");
    }

    public void orderDetailTable(StreamTableEnvironment tableEnv, String ckAndGroupID) {
        tableEnv.executeSql("create table order_detail (\n" +
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
                    "row_time as to_timestamp_ltz(ts,0)," +
                    "WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND" +      // 事件时间字段，水印延迟5秒
                    ") " + getKafkaSourceSQL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL, ckAndGroupID));
    }

    public Table getPaymentInfoTable(StreamTableEnvironment tableEnv) {
        return tableEnv.sqlQuery("select\n" +
                "\t`data`['id'] id,\n" +
                "\t`data`['order_id'] order_id,\n" +
                "\t`data`['user_id'] user_id,\n" +
                "\t`data`['payment_type'] payment_type,\n" +
                "\t`data`['total_amount'] total_amount,\n" +
                "\t`data`['callback_time'] callback_time,\n" +
                "\tts,\n" +
                "\tproc_time,\n" +
                "\trow_time\n" +
                "from\n" +
                "\ttopic_db\n" +
                "where\n" +
                " `database` = 'gmall'\n" +
                " and `table` = 'payment_info'\n" +
                " and `old`['payment_status'] is not null\n" +
                " and `type` = 'update'\n" +
                " and `data`['payment_status']='1602'");
    }
}