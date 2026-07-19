package org.example.realtime.util;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.example.realtime.constant.Constant;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class FlinkSourceUtil {

    public static KafkaSource<String> getKafkaSource(String topicName,String groupID){
        /**
         * 创建 Kafka 源表
         * 该表用于消费Kafka主题中的数据
         */
        return KafkaSource.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setTopics(topicName)
                .setGroupId(groupID)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new DeserializationSchema<String>() {
                    @Override
                    public String deserialize(byte[] message) throws IOException {
                        if (message != null){
                            return new String(message, StandardCharsets.UTF_8);
                        }
                        return null;
                    }

                    @Override
                    public boolean isEndOfStream(String s) {
                        return false;
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return Types.STRING;
                    }
                })
                .build();
    }

    /**
     * 创建 MySQL 源表
     * 该表用于消费MySQL数据库变更事件（binlog）
     * @param databaseName 数据库名称
     * @param tableName 表名称
     * @return
     */
    public static MySqlSource<String> getMysqlSource(String databaseName,String tableName){
        return MySqlSource.<String>builder()
                .hostname(Constant.MYSQL_HOST)
                .port(Constant.MYSQL_PORT)
                .username(Constant.MYSQL_USERNAME)
                .password(Constant.MYSQL_PASSWORD)
                .databaseList(databaseName)
                .tableList(databaseName+"."+tableName)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();
    }
}
