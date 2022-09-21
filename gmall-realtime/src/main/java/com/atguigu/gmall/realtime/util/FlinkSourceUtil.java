package com.atguigu.gmall.realtime.util;

import com.atguigu.gmall.realtime.common.Constant;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
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
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
            topic,
            new DeserializationSchema<String>() {
                
                // 把从 kafka 读取的数据进行返回序列化之后的类型
                @Override
                public TypeInformation<String> getProducedType() {
                    //                    return Types.STRING; // 针对常见的类型, flink 内置好了
                    //                    return  TypeInformation.of(String.class); // 针对没有泛型的 pojo
                    return TypeInformation.of(new TypeHint<String>() {}); // 针对带泛型的类型
                }
                
                // 把字节数组变成自己需要的类型
                @Override
                public String deserialize(byte[] message) throws IOException {
                    if (message == null) {
                        return null;
                    }
                    return new String(message, StandardCharsets.UTF_8); // 如果message是 null, 会空指针
                }
                
                // 要不要结束列. 如果是无界流, 永远返回 false
                @Override
                public boolean isEndOfStream(String nextElement) {
                    return false;
                }
            },
            props
        );
        consumer.setStartFromLatest(); // 如果不是从状态恢复, 从topic的最新的位置开始消费. 如果是从状态恢复, 则这个参数就无效
        return consumer;
    }
}
