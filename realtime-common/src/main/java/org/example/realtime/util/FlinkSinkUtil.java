package org.example.realtime.util;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchemaBuilder;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.example.realtime.constant.Constant;

import javax.annotation.Nullable;

public class FlinkSinkUtil {
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
}
