package com.atguigu.gmall.realtime.app;

import com.atguigu.gmall.realtime.util.FlinkSourceUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author lzc
 * @Date 2022/9/16 13:52
 */
public abstract class BaseAppV1 {
    protected abstract void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream);
    
    public void init(int port, int p, String ckAndGroupIdAndJobName, String topic) {
        System.setProperty("HADOOP_USER_NAME", "atguigu");
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(p);
        
        // 1. 开启checkpoint
        env.enableCheckpointing(3000);
        // 2. 设置状态后端
        env.setStateBackend(new HashMapStateBackend());
        // 3. 设置checkpoint的存储
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop162:8020/gmall/" + ckAndGroupIdAndJobName);
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
        
        DataStreamSource<String> stream = env.addSource(FlinkSourceUtil.getKafkaSource(ckAndGroupIdAndJobName, topic));
        
        // 是对流进行业务处理
        handle(env, stream);
        
        try {
            env.execute(ckAndGroupIdAndJobName);  // 传入一个job的名字
        } catch (Exception e) {
            e.printStackTrace();
        }
        
    }
}
