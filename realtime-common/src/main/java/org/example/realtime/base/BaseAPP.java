package org.example.realtime.base;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.realtime.util.FlinkSourceUtil;

import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;

public abstract class BaseAPP {

    public void Start(int port,int parallelism,String ckAndGroupID,String topicName){
        // 设置hdfs的用户名权限
        System.setProperty("HADOOP_USER_NAME", "root");

        /**
         * 测试环境下启动本地WebUI的端口，为了避免本地端口冲突，做出以下规定：
         * （1）DIM层维度分流应用使用10001端口
         * （2）DWD层应用程序按照在本文档中出现的先后顺序，端口从10011开始，自增1
         * （3）DWS层应用程序按照在本文档中出现的先后顺序，端口从10021开始，自增1
         */
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",port);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        // 设置并行度
        env.setParallelism(parallelism);
        // 设置状态后端
        env.setStateBackend(new HashMapStateBackend());
        // 开启checkpoint
        env.enableCheckpointing(5000);
        // 设置checkpoint精准一次
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 设置checkpoint存储位置
        env.getCheckpointConfig().setCheckpointStorage("hdfs://bigdata1:9000/checkpoint/" + ckAndGroupID);
        // checkpoint 并发数量
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // checkpoint 之间最小间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
        // checkpoint 超时时间
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        // job取消时checkpoint的保留策略
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(RETAIN_ON_CANCELLATION);

        DataStreamSource<String> kafkaSource = env.fromSource(
                FlinkSourceUtil.getKafkaSource(topicName,ckAndGroupID),
                WatermarkStrategy.noWatermarks(),
                "kafka-source"
        );

        handle(env,kafkaSource);


        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }


    }


    public abstract void handle(StreamExecutionEnvironment env,DataStreamSource<String> kafkaSource);

}
