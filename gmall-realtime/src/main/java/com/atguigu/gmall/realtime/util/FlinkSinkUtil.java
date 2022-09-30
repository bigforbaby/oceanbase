package com.atguigu.gmall.realtime.util;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.annotation.NoNeedSink;
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
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
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
        
        List<String> names = AtguiguUtil.getClassFieldsName(tClass);
        // TODO
        // insert into person (id, name, age) values(?,?,?)
        StringBuilder sql = new StringBuilder();
        sql
            .append("insert into ")
            .append(table)
            .append("(")
            // 拼接字段名. tClass 中的有哪些属性, 把属性名找到, 拼成一个字符串
            .append(String.join(",", names))
            .append(") values(")
            // 拼接占位符
            .append(String.join(",", names).replaceAll("[^,]+", "?"))
            .append(")");
        
        System.out.println("clickhouse 的插入语句: " + sql);
        
        return getJdbcSink(driver, url, "default", "aaaaaa", sql.toString());
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
                    //insert into abc(stt,edt,source,keyword,keyword_count,ts) values(?,?,?,?,?,?)
                    Class<?> tClass = t.getClass();
                    //
                    Field[] fields = tClass.getDeclaredFields();
                    try {
                        for (int i = 0, position = 1; i < fields.length; i++) {
                            Field field = fields[i];
                            NoNeedSink noNeedSink = field.getAnnotation(NoNeedSink.class);
                            if (noNeedSink == null) {
                                field.setAccessible(true);// 给这个属性设置访问权限
                                ps.setObject(position++, field.get(t));
                            }
                        }
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }
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
