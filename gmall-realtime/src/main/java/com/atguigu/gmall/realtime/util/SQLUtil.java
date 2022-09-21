package com.atguigu.gmall.realtime.util;

import com.atguigu.gmall.realtime.common.Constant;

/**
 * @Author lzc
 * @Date 2022/9/19 15:39
 */
public class SQLUtil {
    public static String getKafkaSourceSQL(String topic, String groupId) {
        return "with(" +
            "'connector' = 'kafka', " +
            "'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "', " +
            "'properties.group.id' = '" + groupId + "', " +
            "'topic' = '" + topic + "', " +
            "'format' = 'json', " +
            "'scan.startup.mode' = 'latest-offset' " +
            ")";
    }
    
    public static String getKafkaSinkSQL(String topic) {
        return "with(" +
            "'connector' = 'kafka', " +
            "'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "', " +
            "'topic' = '" + topic + "', " +
            "'format' = 'json' " +
            ")";
    }
    
    public static String getUpsertKafkaSinkSQL(String topic) {
        return "with(" +
            "'connector' = 'upsert-kafka', " +
            "'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "', " +
            "'topic' = '" + topic + "', " +
            "'key.format' = 'json', " +
            "'value.format' = 'json' " +
            ")";
    }
}
