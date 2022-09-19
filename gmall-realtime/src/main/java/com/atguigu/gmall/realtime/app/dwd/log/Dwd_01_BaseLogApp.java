package com.atguigu.gmall.realtime.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.BaseAppV1;
import com.atguigu.gmall.realtime.common.Constant;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author lzc
 * @Date 2022/9/19 8:53
 */
public class Dwd_01_BaseLogApp extends BaseAppV1 {
    public static void main(String[] args) {
        new Dwd_01_BaseLogApp().init(
            3001,
            2,
            "Dwd_01_BaseLogApp",
            Constant.TOPIC_ODS_LOG
        );
    }
    
    @Override
    protected void handle(StreamExecutionEnvironment env,
                          DataStreamSource<String> stream) {
        // 1. 对数据做清洗
        SingleOutputStreamOperator<JSONObject> etledStream = etl(stream);
    
        // 2. 纠正新老客户
        
        // 3. 分流
        
        // 4. 不同的流的数据写入到不同的topic中
    }
    
    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
       return stream
            .filter(new FilterFunction<String>() {
                @Override
                public boolean filter(String s) throws Exception {
                    try {
                        JSON.parseObject(s);
                    } catch (Exception e) {
                        System.out.println("你的数据格式不是一个有效的json格式: " + s);
                        // 如果有异常,表示json格式不对, 方法false, 数据不要了
                        return false;
                    }
                    // 去掉空白字符之后, 判断长度应该大于0
                    return s.trim().length() > 0;
                }
            })
            .map(JSON::parseObject);
    }
}
