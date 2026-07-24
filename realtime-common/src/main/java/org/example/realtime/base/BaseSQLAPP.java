package org.example.realtime.base;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.example.realtime.constant.Constant;
import org.example.realtime.util.SQLUtil;

import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;

public abstract class BaseSQLAPP {

    public void Start(int port,int parallelism,String ckAndGroupID){
        // 设置hdfs的用户名权限
        System.setProperty("HADOOP_USER_NAME", "root");
        
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",port);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        // 设置并行度
//        env.setParallelism(parallelism);
//        // 设置状态后端
//        env.setStateBackend(new HashMapStateBackend());
//        // 开启checkpoint
//        env.enableCheckpointing(5000);
//        // 设置checkpoint精准一次
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        // 设置checkpoint存储位置
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://bigdata1:9000/checkpoint/" + ckAndGroupID);
//        // checkpoint 并发数量
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
//        // checkpoint 之间最小间隔
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
//        // checkpoint 超时时间
//        env.getCheckpointConfig().setCheckpointTimeout(10000);
//        // job取消时checkpoint的保留策略
//        env.getCheckpointConfig().setExternalizedCheckpointCleanup(RETAIN_ON_CANCELLATION);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        handle(env,tableEnv,ckAndGroupID);

//        try {
//            env.execute();
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }


    }

    public TableResult createTopicDB(String ckAndGroupID, StreamTableEnvironment tableEnv) {
        return tableEnv.executeSql(SQLUtil.getKafkaTopicDB(ckAndGroupID));
    }

    // 读取hbase的base_dic表
    public void createBaseDic(StreamTableEnvironment tableEnv){
        tableEnv.executeSql("CREATE TABLE base_dic (\n" +
                " rowkey STRING,\n" +
                " info ROW<dic_name String>,\n" +
                " PRIMARY KEY (rowkey) NOT ENFORCED\n" +
                ") WITH (\n" +
                " 'connector' = 'hbase-2.2',\n" +
                " 'table-name' = 'gmall:dim_base_dic',\n" +
                " 'zookeeper.quorum' = '" + Constant.HBASE_ZOOKEEPER_AND_PORT_QUORUM + "',\n" +
                " 'zookeeper.znode.parent' = '" + Constant.ZOOKEEPER_ZNODE_PARENT + "'\n" +
                ");");
    }


    public abstract void handle(StreamExecutionEnvironment env,StreamTableEnvironment tableEnv,String ckAndGroupID);

}