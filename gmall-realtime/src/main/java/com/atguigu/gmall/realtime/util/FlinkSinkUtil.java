package com.atguigu.gmall.realtime.util;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.common.Constant;
import com.atguigu.gmall.realtime.sink.PhoenixSink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.SQLException;
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
    
    // 如果有泛型, 内部代码绝大部分情况会用到反射, 得需要 class
    public static <T> SinkFunction<T> getClickHouseSink(String table,
                                                        Class<T> tClass) {
        String driver = "com.clickhouse.jdbc.ClickHouseDriver";
        String url = "jdbc:clickhouse://hadoop162:8123/gmall2022";
        
        // TODO
        String sql = "";
        
        return getJdbcSink(driver, url, "default", "aaaaaa", sql);
    }
    
    private static <T> SinkFunction<T> getJdbcSink(String driver,
                                                   String url,
                                                   String user,
                                                   String password,
                                                   String sql) {
        return JdbcSink.sink(
            sql,
            new JdbcStatementBuilder<T>() {
                @Override
                public void accept(PreparedStatement ps,
                                   T t) throws SQLException {
                    //TODO 给占位符进行赋值. 需要参考 sql 语句的拼写
                }
            },
            new JdbcExecutionOptions.Builder()
                .withBatchIntervalMs(2000)
                .withBatchSize(1024 * 1024)
                .withMaxRetries(3)
                .build(),
            new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withDriverName(driver)
                .withUrl(url)
                .withUsername(user)
                .withPassword(password)
                .build()
        );
    }
    
    
}
