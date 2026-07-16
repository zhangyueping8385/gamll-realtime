package org.example.realtime.util;

import org.example.realtime.constant.Constant;

/**
 * SQL工具类，用于生成Flink SQL语句
 * 提供Kafka源表相关的DDL生成方法
 */
public class SQLUtil {

    /**
     * 生成Kafka源表的WITH配置子句
     * 
     * @param topicName Kafka主题名称
     * @param groupId   消费者组ID
     * @return WITH配置字符串，包含Kafka连接器配置
     */
    public static String getKafkaSourceSQL(String topicName, String groupId) {
        return "WITH (\n" +
                // 指定使用Kafka连接器
                "  'connector' = 'kafka',\n" +
                // 设置要消费的Kafka主题
                "  'topic' = '" + topicName + "',\n" +
                // Kafka集群地址，从常量类获取
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "',\n" +
                // 消费者组ID
                "  'properties.group.id' = '" + groupId + "',\n" +
                // 从最早的offset开始消费
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                // 数据格式为JSON
                "  'format' = 'json'\n" +
                ")";
    }

    /**
     * 生成topic_db表的CREATE TABLE语句
     * 该表用于消费数据库变更事件（binlog）
     * 
     * @param groupID 消费者组ID
     * @return CREATE TABLE SQL语句
     */
    public static String getKafkaTopicDB(String groupID) {
        return "CREATE TABLE topic_db (\n" +
                "  `database` STRING,\n" +     // 数据库名称
                "  `table` STRING,\n" +        // 表名称
                "  `ts` bigint,\n" +
                "  `type` STRING,\n" +             // 变更时间戳
                "  `data` map<STRING,STRING>,\n" +  // 变更后的数据
                "  `old` map<STRING,STRING>,\n" +   // 变更前的数据（UPDATE时才有值）
                "   proc_time as PROCTIME()" +      // 处理时间字段
                ")" + getKafkaSourceSQL(Constant.TOPIC_DB, groupID);
    }

    public static String getKafkaSinkSQL(String topicName){
        return "WITH (\n" +
                // 指定使用Kafka连接器
                "  'connector' = 'kafka',\n" +
                // 设置要消费的Kafka主题
                "  'topic' = '" + topicName + "',\n" +
                // Kafka集群地址，从常量类获取
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "',\n" +
                // 数据格式为JSON
                "  'format' = 'json'\n" +
                ")";
    }


}