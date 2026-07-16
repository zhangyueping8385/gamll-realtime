package com.example.realtime.dwd.db.app;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.example.realtime.base.BaseSQLAPP;
import org.example.realtime.constant.Constant;
import org.example.realtime.util.SQLUtil;


public class DwdInterationCommonInfo extends BaseSQLAPP {
    public static void main(String[] args) {
        new DwdInterationCommonInfo().Start(10012,4,Constant.TOPIC_DWD_INTERACTION_COMMON_INFO);
    }


    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv, String ckAndGroupID) {
        // 核心逻辑
        // 1. 创建topic数据库：topic_db
        createTopicDB(ckAndGroupID, tableEnv);

        // 2. 读取hbase数据库：dim_base_dic表
        createBaseDic(tableEnv);

        // 3. 清洗topic_db表，筛选出评论信息表新增的数据
        Table commentInfo = getCommentInfo(tableEnv);

        tableEnv.createTemporaryView("comment_info", commentInfo);

//        tableEnv.executeSql("select * from base_dic").print();
        // 4. 使用lookup join完成纬度退化
        Table joinTable = getJoinTable(tableEnv);

        // 5. 创建dwd_interaction_comment_info表
        dwd_interation_comment_info(tableEnv);

        // 6. 插入数据到dwd_interaction_comment_info表
        joinTable.insertInto(Constant.TOPIC_DWD_INTERACTION_COMMON_INFO).execute();

    }

    private static void dwd_interation_comment_info(StreamTableEnvironment tableEnv) {
        tableEnv.executeSql("create table " + Constant.TOPIC_DWD_INTERACTION_COMMON_INFO + "(" +
                "id STRING,\n" +
                "user_id STRING,\n" +
                "nick_name STRING,\n" +
                "sku_id STRING,\n" +
                "spu_id STRING,\n" +
                "order_id STRING,\n" +
                "appraise_code STRING,\n" +
                "appraise STRING,\n" +
                "comment_txt STRING,\n" +
                "create_time STRING,\n" +
                "operate_time STRING" +
                ") " + SQLUtil.getKafkaSinkSQL(Constant.TOPIC_DWD_INTERACTION_COMMON_INFO));
    }

    public Table getJoinTable(StreamTableEnvironment tableEnv) {
        return tableEnv.sqlQuery("select " +
                "id,\n" +
                "user_id,\n" +
                "nick_name,\n" +
                "sku_id,\n" +
                "spu_id,\n" +
                "order_id,\n" +
                "appraise appraise_code,\n" +
                "info.dic_name appraise_name,\n" +
                "comment_txt,\n" +
                "create_time,\n" +
                "operate_time\n" +
                "from\n" +
                "comment_info c \n" +
                "join base_dic FOR SYSTEM_TIME AS OF c.proc_time b\n" +
                "on c.appraise = b.rowkey");
    }

    public Table getCommentInfo(StreamTableEnvironment tableEnv) {
        return tableEnv.sqlQuery("select\n" +
                "data['id'] id, \n" +
                "data['user_id'] user_id,\n" +
                "data['nick_name'] nick_name,\n" +
                "data['head_img'] head_img, \n" +
                "data['sku_id'] sku_id,\n" +
                "data['spu_id'] spu_id,\n" +
                "data['order_id'] order_id,\n" +
                "data['appraise'] appraise,\n" +
                "data['comment_txt'] comment_txt,\n" +
                "data['create_time'] create_time,\n" +
                "data['operate_time'] operate_time,\n" +
                "proc_time\n" +
                "from topic_db\n" +
                "where `database` ='gmall'\n" +
                "and `table` = 'comment_info'\n" +
                "and `type` = 'insert'");
    }
}