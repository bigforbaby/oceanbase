package com.atguigu.gmall.realtime.app.dim;

import com.atguigu.gmall.realtime.app.BaseAppV1;
import com.atguigu.gmall.realtime.common.Constant;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author lzc
 * @Date 2022/9/16 11:28
 */
public class DimApp extends BaseAppV1 {
    public static void main(String[] args) {
        new DimApp().init(
            2001,
            2,
            "DimApp",
            Constant.TOPIC_ODS_DB
        );
        
      
    }
    
    @Override
    protected void handle(StreamExecutionEnvironment env,
                          DataStreamSource<String> stream) {
        // 对流进行业务处理
        stream.print();
    }
}
/*
完成维度从 ods_db 到维度底层


维度数据:
 ods层: ods_db

 目标: flink消费ods_db, 分流, 一个维度表写入到phoenix中的一个表中

 分流使用动态分流: 根据配置动态的把维度分开
 */