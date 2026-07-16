package org.example.realtime.dwd.db.app;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.example.realtime.base.BaseSQLAPP;
import org.example.realtime.constant.Constant;
import org.example.realtime.util.SQLUtil;

public class DwdTradeCartAdd extends BaseSQLAPP {
    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv, String ckAndGroupID) {
        // 1. 从kafka读取数据，创建topic_db
        createTopicDB(ckAndGroupID, tableEnv);

        // 2. 从topic_db读取数据，创建dwd_trade_cart_add表
        // 提取加购操作生成加购表, 包含用户id, 商品id, 商品数量, 商品名称, 商品价格, 商品是否选中, 商品创建时间, 商品操作时间, 商品是否已下单, 商品下单时间, 商品来源类型, 商品来源id, 商品时间戳
        // 判断条件：类型为insert或update，且商品数量大于0
        Table filterTable = getFilterTable(tableEnv);

        dwd_trade_cart_add(tableEnv);

        filterTable.insertInto(Constant.TOPIC_DWD_TRADE_CART_ADD).execute();
    }

    public void dwd_trade_cart_add(StreamTableEnvironment tableEnv) {
        tableEnv.executeSql("create table "+Constant.TOPIC_DWD_TRADE_CART_ADD+"(\n" +
            "    id string,\n" +
            "    user_id string,\n" +
            "    sku_id string,\n" +
            "    cart_price string,\n" +
            "    sku_num string,\n" +
            "    sku_name string,\n" +
            "    is_checked string,\n" +
            "    create_time string,\n" +
            "    operate_time string,\n" +
            "    is_ordered string,\n" +
            "    order_time string,\n" +
            "    source_type string,\n" +
            "    source_id string,\n" +
            "    ts bigint\n" +
            ")"+ SQLUtil.getKafkaSinkSQL(Constant.TOPIC_DWD_TRADE_CART_ADD));
    }

    public Table getFilterTable(StreamTableEnvironment tableEnv) {
        return tableEnv.sqlQuery("select \n" +
                " `data`['id'] id, \n" +
                " `data`['user_id'] user_id, \n" +
                " `data`['sku_id'] sku_id, \n" +
                " `data`['cart_price'] cart_price, \n" +
                " if(`type`='insert',`data`['sku_num'],cast(cast(`data`['sku_num'] as bigint) - cast(`old`['sku_num'] as bigint) as string))  sku_num, \n" +
                " `data`['sku_name'] sku_name, \n" +
                " `data`['is_checked'] is_checked, \n" +
                " `data`['create_time'] create_time, \n" +
                " `data`['operate_time'] operate_time, \n" +
                " `data`['is_ordered'] is_ordered, \n" +
                " `data`['order_time'] order_time, \n" +
                " `data`['source_type'] source_type, \n" +
                " `data`['source_id'] source_id,\n" +
                " ts\n" +
                "from topic_db\n" +
                "where `database`='gmall'\n" +
                "and `table`='cart_info'\n" +
                "and (`type`='insert' or (\n" +
                " `type`='update' and `old`['sku_num'] is not null \n" +
                " and cast(`data`['sku_num'] as bigint) > cast(`old`['sku_num'] as bigint)))");
    }

    public static void main(String[] args) {
        new DwdTradeCartAdd().Start(10013, 4, Constant.TOPIC_DWD_TRADE_CART_ADD);
    }
}
