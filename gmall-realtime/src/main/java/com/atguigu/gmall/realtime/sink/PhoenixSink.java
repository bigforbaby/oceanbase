package com.atguigu.gmall.realtime.sink;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.util.DruidDSUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.PreparedStatement;

/**
 * @Author lzc
 * @Date 2022/9/17 14:17
 */
public class PhoenixSink extends RichSinkFunction<Tuple2<JSONObject, TableProcess>> {
    
    private DruidDataSource dataSource;
    
    @Override
    public void open(Configuration parameters) throws Exception {
        dataSource = DruidDSUtil.getDataSource(); // 获取连接器
    }
    
    @Override
    public void close() throws Exception {
        if (dataSource != null) {
            dataSource.close();  // 关闭连接池
        }
    }
    
    // 流中每进来一条元素执行一次
    @Override
    public void invoke(Tuple2<JSONObject, TableProcess> t,  // 流中的元素
                       Context ctx) throws Exception {
        JSONObject data = t.f0;
        TableProcess tp = t.f1;
        
        //TODO
        StringBuilder sql = new StringBuilder();
        
        // 1. 从连接池获取连接对象
        DruidPooledConnection conn = dataSource.getConnection();
    
        // 2. 通过连接对象得到一个预处理语句
        PreparedStatement ps = conn.prepareStatement(sql.toString());
        // 3. 对sql中的占位符赋值 TODO
        
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