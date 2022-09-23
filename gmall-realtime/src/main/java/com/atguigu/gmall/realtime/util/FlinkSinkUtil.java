package com.atguigu.gmall.realtime.util;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.common.Constant;
import com.atguigu.gmall.realtime.sink.PhoenixSink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

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
    
    public static SinkFunction<String> getKafkaSink(String topic) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", Constant.KAFKA_BROKERS);
        // 严格一次!
        props.setProperty("transaction.timeout.ms", 15 * 60 * 1000 + "");
        return new FlinkKafkaProducer<String>(
            "default",
            new KafkaSerializationSchema<String>() {
                @Override
                public ProducerRecord<byte[], byte[]> serialize(String element,
                                                                @Nullable Long timestamp) {
                    //
                    return new ProducerRecord<>(topic, element.getBytes(StandardCharsets.UTF_8));
                }
            },
            props,
            FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );
    }
    
    public static SinkFunction<Tuple2<JSONObject, TableProcess>> getKafkaSink() {
        
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", Constant.KAFKA_BROKERS);
        // 严格一次!
        props.setProperty("transaction.timeout.ms", 15 * 60 * 1000 + "");
        
        return new FlinkKafkaProducer<Tuple2<JSONObject, TableProcess>>(
            "default",
            new KafkaSerializationSchema<Tuple2<JSONObject, TableProcess>>() {
                @Override
                public ProducerRecord<byte[], byte[]> serialize(Tuple2<JSONObject, TableProcess> t,
                                                                @Nullable Long timestamp) {
                    String topic = t.f1.getSinkTable();
                    byte[] value = t.f0.toJSONString().getBytes(StandardCharsets.UTF_8);
                    return new ProducerRecord<>(topic, value);
                }
            },
            props,
            FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );
        
    }
}
