import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;

public class test1 {
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "root");

        // 1. 构建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 添加水位线和状态后端等参数
        env.setParallelism(4);
        // 状态后端及检查点相关配置
        // 设置状态后端
        env.setStateBackend(new HashMapStateBackend());

        // 开启 checkpoint
        env.enableCheckpointing(5000);
        // 设置 checkpoint 模式: 精准一次
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // checkpoint 存储
        env.getCheckpointConfig().setCheckpointStorage("hdfs://bigdata1:9000/gmall2026/stream/");
        // checkpoint 并发数
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // checkpoint 之间的最小间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
        // checkpoint  的超时时间
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        // job 取消时 checkpoint 保留策略
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(RETAIN_ON_CANCELLATION);

        // 3. 添加数据源
        DataStreamSource<String> kafkaSource = env.fromSource(
                KafkaSource.<String>builder()
                        .setBootstrapServers("bigdata1:9092")
                        .setTopics("topic_db")
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setGroupId("test01")
                        .setValueOnlyDeserializer(new SimpleStringSchema())
                        .build(), WatermarkStrategy.noWatermarks(), "kafka-source"
        );

        // 4. 数据处理
        kafkaSource.print();

        // 5. 执行环境
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
