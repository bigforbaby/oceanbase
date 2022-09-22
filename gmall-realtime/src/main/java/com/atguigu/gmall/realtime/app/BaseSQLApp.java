package com.atguigu.gmall.realtime.app;

import com.atguigu.gmall.realtime.common.Constant;
import com.atguigu.gmall.realtime.util.SQLUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lzc
 * @Date 2022/9/16 13:52
 */
public abstract class BaseSQLApp {
    
    // 封装一个方法, 专门用来读取ods_db数据
    public void readOdsDb(StreamTableEnvironment tEnv, String groupId){
        tEnv.executeSql("create table ods_db(" +
                            " `database` string, " +
                            " `table` string, " +
                            " `type` string, " +
                            " `ts` bigint, " +
                            " `data` map<string, string>, " +
                            " `old` map<string, string>," +
                            "  pt as proctime() " + // 是为了 lookup join 的时候使用
                            ")" + SQLUtil.getKafkaSourceSQL(Constant.TOPIC_ODS_DB, groupId));
        
    }
    public void readBaseDic(StreamTableEnvironment tEnv){
        tEnv.executeSql("CREATE TEMPORARY TABLE base_dic ( " +
                            "  dic_code string, " +
                            "  dic_name string  " +
                            ") WITH ( " +
                            "  'connector' = 'jdbc', " +
                            "  'url' = 'jdbc:mysql://hadoop162:3306/gmall2022?useSSL=false', " +
                            "  'table-name' = 'base_dic',  " +
                            " 'lookup.cache.max-rows' = '10'," + // 最多换成多少条数据
                            " 'lookup.cache.ttl' = '1 hour'," + // 每条数据最多换成多长时间
                            "  'username' = 'root',  " +
                            "  'password' = 'aaaaaa'  " +
                            ")");
        
    }
    
   
    
    public void init(int port, int p, String ckAndJobName) {
        System.setProperty("HADOOP_USER_NAME", "atguigu");
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(p);
        
        // 1. 开启checkpoint
//       env.enableCheckpointing(3000);
        // 2. 设置状态后端
//        env.setStateBackend(new HashMapStateBackend());
        // 3. 设置checkpoint的存储
       // env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop162:8020/gmall/" + ckAndJobName);
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
        
        // 表的执行环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        
        handle(env, tEnv);
    
    
    }
    
    protected abstract void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv);
}
