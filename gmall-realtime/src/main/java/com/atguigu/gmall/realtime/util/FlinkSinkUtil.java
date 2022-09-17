package com.atguigu.gmall.realtime.util;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.sink.PhoenixSink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * @Author lzc
 * @Date 2022/9/17 14:16
 */
public class FlinkSinkUtil {
    public static void main(String[] args) {
        
    }
    
    public static SinkFunction<Tuple2<JSONObject, TableProcess>> getPhoenixSink() {
        
        return new PhoenixSink();
    }
}
