package com.atguigu;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author lzc
 * @Date 2022/9/22 09:03
 */
public class StateTest {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
    
        env
            .fromElements("a", "b", "c")
            .keyBy(x -> 1)
            .process(new KeyedProcessFunction<Integer, String, String>() {
    
                private ValueState<String> a;
    
                @Override
                public void open(Configuration parameters) throws Exception {
                    a = getRuntimeContext().getState(new ValueStateDescriptor<String>("a", String.class));
                    
                }
    
                @Override
                public void processElement(String s,
                                           Context context,
                                           Collector<String> collector) throws Exception {
                
                }
            })
            .print();
        
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
