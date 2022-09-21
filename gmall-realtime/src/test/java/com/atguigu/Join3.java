package com.atguigu;

import com.atguigu.gmall.realtime.app.BaseAppV1;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author lzc
 * @Date 2022/9/21 09:27
 */
public class Join3 extends BaseAppV1 {
    public static void main(String[] args) {
        new Join3().init(
            10003,
            2,
            "Join3",
            "r1"
        );
    }
    
    
    @Override
    protected void handle(StreamExecutionEnvironment env,
                          DataStreamSource<String> stream) {
        stream.print();
    }
}
