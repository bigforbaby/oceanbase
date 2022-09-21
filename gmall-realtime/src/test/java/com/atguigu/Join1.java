package com.atguigu;

import com.atguigu.gmall.realtime.app.BaseSQLApp;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @Author lzc
 * @Date 2022/9/21 09:27
 */
public class Join1 extends BaseSQLApp {
    public static void main(String[] args) {
        new Join1().init(10001,1,"Join1");
    }
    
    @Override
    protected void handle(StreamExecutionEnvironment env,
                          StreamTableEnvironment tEnv) {
        SingleOutputStreamOperator<User1> s1 = env
            .socketTextStream("hadoop162", 8888)
            .map(new MapFunction<String, User1>() {
                @Override
                public User1 map(String s) throws Exception {
                    String[] data = s.split(",");
                    return new User1(Integer.parseInt(data[0]), data[1]);
                }
            });
    
        SingleOutputStreamOperator<User2> s2 = env
            .socketTextStream("hadoop162", 9999)
            .map(new MapFunction<String, User2>() {
                @Override
                public User2 map(String s) throws Exception {
                    String[] data = s.split(",");
                    return new User2(Integer.parseInt(data[0]), Integer.parseInt(data[1]));
                }
            });
        
        // 把流转成表
    
        Table user1 = tEnv.fromDataStream(s1);
        Table user2 = tEnv.fromDataStream(s2);
        
        tEnv.createTemporaryView("user1", user1);
        tEnv.createTemporaryView("user2", user2);
        // 对内连接: 每个表的数据, 只在内存中保存 10s
        tEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5));
//        tEnv.getConfig().getConfiguration().setString("table.exec.state.ttl", "10 second");
        tEnv.sqlQuery("select " +
                          "user1.id, " +
                          "name, " +
                          "age " +
                          "from user1 " +
                          "join user2 on user1.id=user2.id")
            .execute()
            .print();
        
    
    
    }
    
    
    
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class User1{
        private int id;
        private String name;
    }
    
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class User2{
        private int id;
        private int age;
    }
}
