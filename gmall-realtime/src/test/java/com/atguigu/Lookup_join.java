package com.atguigu;

import com.atguigu.gmall.realtime.app.BaseSQLApp;
import com.atguigu.gmall.realtime.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lzc
 * @Date 2022/9/21 11:16
 */
public class Lookup_join extends BaseSQLApp {
    public static void main(String[] args) {
        new Lookup_join().init(10001,1,"Lookup_join");
    }
    
    @Override
    protected void handle(StreamExecutionEnvironment env,
                          StreamTableEnvironment tEnv) {
        /*
        lookup 的应用场景:
            消费了事实表, 事实表有些信息不完整, 一般需要查询维度表来补充
            根据 id 去查找对应的数据
         */
    
        tEnv.executeSql("create table abc(" +
                           " id string, " +
                            "pt as proctime() " +  // 如果是 lookup join 事实表必须提供一个处理时间字段
                           ")" + SQLUtil.getKafkaSourceSQL("s5", "atguigu"));
        
        
        tEnv.executeSql("CREATE TEMPORARY TABLE base_dic ( " +
                            "  dic_code string, " +
                            "  dic_name string  " +
                            ") WITH ( " +
                            "  'connector' = 'jdbc', " +
                            "  'url' = 'jdbc:mysql://hadoop162:3306/gmall2022?useSSL=false', " +
                            "  'table-name' = 'base_dic',  " +
                            " 'lookup.cache.max-rows' = '10'," + // 最多换成多少条数据
                            " 'lookup.cache.ttl' = '60 second'," + // 每条数据最多换成多长时间
                            "  'username' = 'root',  " +
                            "  'password' = 'aaaaaa'  " +
                            ")");
    
        Table result = tEnv.sqlQuery("select " +
                                        " id, " +
                                        " dic_name " +
                                        "from abc " +
                                        "join base_dic for system_time as of abc.pt as dic " +
                                        "on abc.id=dic.dic_code ");
        result.execute().print();
    
    }
}
