package com.atguigu;

import com.atguigu.gmall.realtime.app.BaseSQLApp;
import com.atguigu.gmall.realtime.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lzc
 * @Date 2022/9/21 09:27
 */
public class Join2 extends BaseSQLApp {
    public static void main(String[] args) {
        new Join2().init(10002, 1, "Join1");
    }
    
    @Override
    protected void handle(StreamExecutionEnvironment env,
                          StreamTableEnvironment tEnv) {
        
        /*tEnv.executeSql("create table abc(" +
                            " id int, " +
                            " name string, " +
                            " age int, " +
                            " primary key(id)NOT ENFORCED" +
                            ")with(" +
                            "'connector' = 'upsert-kafka', " +
                            "'properties.bootstrap.servers' = 'hadoop162:9092', " +
                            "'topic' = 'r1', " +
                            "'value.format' = 'json', " +
                            "'key.format' = 'json' " +
                            ")");*/
    
        tEnv.executeSql("create table abc(" +
                            " id int, " +
                            " name string, " +
                            " age int " +
                            ")" + SQLUtil.getKafkaSourceSQL("r1", "atguigu"));
        
        tEnv.sqlQuery("select * from abc").execute().print();
        
        
    }
    
    
  
}
