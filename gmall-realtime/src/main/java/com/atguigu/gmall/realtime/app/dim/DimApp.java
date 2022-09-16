package com.atguigu.gmall.realtime.app.dim;

import com.atguigu.gmall.realtime.common.Constant;
import com.atguigu.gmall.realtime.util.FlinkSourceUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author lzc
 * @Date 2022/9/16 11:28
 */
public class DimApp {
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "atguigu");
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
        
        // 1. 开启checkpoint
        env.enableCheckpointing(3000);
        // 2. 设置状态后端
        env.setStateBackend(new HashMapStateBackend());
        // 3. 设置checkpoint的存储
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop162:8020/gmall/DimApp");
        // 4. 设置checkpoint的模式
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 5. checkpoint 并发数
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 6. checkpoint 之间最小的时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        // 7. 当job取消的时候, 是否删除checkpoint的存储
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 8. 设置checkpoint的超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000);
    
        DataStreamSource<String> stream = env.addSource(FlinkSourceUtil.getKafkaSource("DimApp", Constant.TOPIC_ODS_DB));
        stream.print();
    
    
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        
        // 1. 消费ods_db数据
        
        // 2. 分流
        
        // 3. 不同的表写入到phoenix中不同表中
    }
}
/*
完成维度从 ods_db 到维度底层


维度数据:
 ods层: ods_db

 目标: flink消费ods_db, 分流, 一个维度表写入到phoenix中的一个表中

 分流使用动态分流: 根据配置动态的把维度分开
 */