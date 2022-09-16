package com.atguigu.gmall.realtime.util;

import com.atguigu.gmall.realtime.common.Constant;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @Author lzc
 * @Date 2022/9/16 11:37
 */
public class FlinkSourceUtil {
  
    
    public static SourceFunction<String> getKafkaSource(String groupId, String topic) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", Constant.KAFKA_BROKERS);
        props.setProperty("group.id", groupId);
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), props);
        consumer.setStartFromLatest(); // 如果不是从状态恢复, 从topic的最新的位置开始消费. 如果是从状态恢复, 则这个参数就无效
        return consumer;
    }
}
