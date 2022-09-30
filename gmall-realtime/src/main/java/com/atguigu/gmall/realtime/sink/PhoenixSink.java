package com.atguigu.gmall.realtime.sink;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.util.DruidDSUtil;
import com.atguigu.gmall.realtime.util.RedisUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @Author lzc
 * @Date 2022/9/17 14:17
 */
public class PhoenixSink extends RichSinkFunction<Tuple2<JSONObject, TableProcess>> {
    
    private DruidDataSource dataSource;
    private Jedis redisClient;
    
    @Override
    public void open(Configuration parameters) throws Exception {
        dataSource = DruidDSUtil.getDataSource(); // 获取连接器
        
        redisClient = RedisUtil.getRedisClient();
    }
    
    @Override
    public void close() throws Exception {
        if (dataSource != null) {
            dataSource.close();  // 关闭连接池
        }
        if (redisClient != null) {
            redisClient.close();
        }
    }
    
    // 流中每进来一条元素执行一次
    @Override
    public void invoke(Tuple2<JSONObject, TableProcess> t,  // 流中的元素
                       Context ctx) throws Exception {
        JSONObject data = t.f0;
        TableProcess tp = t.f1;
        // 1. 写出维度数据到 phoenix
        writeToPhoenix(data, tp);
        
        // 2. 新增一个功能, 当维度发生更新的时候, 删除缓存
        delCache(data, tp);
    }
    
    private void delCache(JSONObject data, TableProcess tp) {
        //当维度发生更新的时候, 删除缓存
        String op = data.getString("op");
        if ("update".equals(op)) {
            // 当维度更新的时候, 删除换成
            String key = tp.getSinkTable()+":"+data.getString("id");
            redisClient.del(key);
        }
    }
    
    private void writeToPhoenix(JSONObject data, TableProcess tp) throws SQLException {
        //TODO
        // upsert into t(a,b,c)values(?,?,?)
        StringBuilder sql = new StringBuilder();
        sql
            .append("upsert into ")
            .append(tp.getSinkTable())
            .append("(")
            // 拼接字段
            .append(tp.getSinkColumns())
            .append(")values(")
            // 拼接占位符  id,tm_name
            .append(tp.getSinkColumns().replaceAll("[^,]+", "?"))
            .append(")");
        // 1. 从连接池获取连接对象
        DruidPooledConnection conn = dataSource.getConnection();
        
        // 2. 通过连接对象得到一个预处理语句
        PreparedStatement ps = conn.prepareStatement(sql.toString());
        // 3. 对sql中的占位符赋值 TODO
        //upsert into t(a,b,c)values(?,?,?)
        String[] columns = tp.getSinkColumns().split(",");
        for (int i = 0; i < columns.length; i++) {
            String columnName = columns[i];
            // 需要做非空判断: 如果是null, 就写入null, 否则就写入自己
            ps.setString(i + 1, data.get(columnName) == null ? null : data.get(columnName).toString());
        }
        
        // 4. 执行预处理语句
        ps.execute();
        // 5. 提交
        conn.commit();
        // 6. 关闭预处理语句
        ps.close();
        // 7. 归还连接
        conn.close();  // 如果这个连接是自己手动获取的,则是关闭连接;  如果是从连接池, 则是归还连接给连接池
    }
}
/*
长连接的问题!
连接mysql的时候, 如果一个连接超过8个小时没有数据传输,则会自动关闭这个连接

解决:
 1. 每次使用之前, 判断连接是否关闭, 如果关闭就重新创建一个
 2. 使用连接池, 可以避免长连接问题. 我每使用完, 可以归还连接池
 */