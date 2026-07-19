package org.example.realtime.util;

import com.alibaba.fastjson.JSONObject;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.SimpleStringSerializer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchemaBuilder;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.example.realtime.constant.Constant;

import javax.annotation.Nullable;
import java.util.Properties;

public class FlinkSinkUtil {
    /**
     * 创建 Kafka 消息接收表
     * 该表用于接收Kafka主题中的数据
     * @param topicName 主题名称
     * @return
     */
    public static KafkaSink<String> getKafkaSink(String topicName) {
        return KafkaSink.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setRecordSerializer(new KafkaRecordSerializationSchemaBuilder<String>()
                        .setTopic(topicName)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE) // 确保消息Exactly Once
                .setTransactionalIdPrefix("realtime"+topicName+System.currentTimeMillis()) // 事务ID前缀
                .setProperty("transaction.commit.interval.ms", 15*60*1000+"") // 事务提交间隔，单位毫秒
                .build();
    }

    /**
     * 创建 Kafka 消息接收表
     * 该表用于接收Kafka主题中的数据
     * @return
     */
    public static KafkaSink<JSONObject> getKafkaSinkWithTopicList(){
        return KafkaSink.<JSONObject>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setRecordSerializer(new KafkaRecordSerializationSchema<JSONObject>() {
                    @Nullable
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObj, KafkaSinkContext kafkaSinkContext, Long aLong) {
                        String sinkTable = jsonObj.getString("sink_table");
                        return new ProducerRecord<>(sinkTable, Bytes.toBytes(jsonObj.toString()));
                    }
                })
//                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE) // 确保消息Exactly Once
//                .setTransactionalIdPrefix("realtime"+"base_db"+System.currentTimeMillis()) // 事务ID前缀
//                .setProperty("transaction.commit.interval.ms", 15*60*1000+"") // 事务提交间隔，单位毫秒
                .build();
    }

    /**
     * 创建 Doris 消息接收表
     * 该表用于接收Doris表中的数据
     * @param tableName 表名
     * @return
     */
    public static DorisSink<String> getDorisSink(String tableName){
        Properties properties = new Properties();
        // 上游是json数据的时候，需要开启以下配置
        properties.setProperty("read_json_by_line", "true");
        properties.setProperty("format", "json");

         return DorisSink.<String>builder()
                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(DorisExecutionOptions.builder()
                        .setLabelPrefix("label-doris"+System.currentTimeMillis()) // 标签前缀
                        .setDeletable(false)    // 不可删除
                        //.setBatchMode(true)  开启攒批写入
                        .setStreamLoadProp(properties) // 设置流加载属性
                        .build())
                .setSerializer(new SimpleStringSerializer()) // 设置序列化器为SimpleStringSerializer
                .setDorisOptions(DorisOptions.builder()
                        .setFenodes(Constant.DORIS_FENODES) //  Doris节点地址
                        .setTableIdentifier(Constant.DORIS_DATABASE + "." + tableName) // Doris表标识符
                        .setUsername(Constant.DORIS_USERNAME) // Doris用户名
                        .setPassword(Constant.DORIS_PASSWORD) // Doris密码
                        .build())
            .build();





    }

}
