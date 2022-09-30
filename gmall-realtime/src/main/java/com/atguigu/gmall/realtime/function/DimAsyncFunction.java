package com.atguigu.gmall.realtime.function;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.util.DimUtil;
import com.atguigu.gmall.realtime.util.DruidDSUtil;
import com.atguigu.gmall.realtime.util.RedisUtil;
import com.atguigu.gmall.realtime.util.ThreadPoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @Author lzc
 * @Date 2022/9/30 11:27
 */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> {
    
    private ThreadPoolExecutor threadPool;
    private DruidDataSource dataSource;
    
    @Override
    public void open(Configuration parameters) throws Exception {
        threadPool = ThreadPoolUtil.getThreadPool(); // 获取线程池
        dataSource = DruidDSUtil.getDataSource();  // 获取 phoenix 连接池
        
    }
    
    protected abstract String getTable();
    protected abstract String getId(T input);
    protected abstract void addDim(T input, JSONObject dim);
    
    @Override
    public void asyncInvoke(T input,
                            ResultFuture<T> resultFuture) throws Exception {
        // 多线程(线程池)+ 多客户端(连接池): 一个线程一个客户端
        
        // 把线程交给线程执行
        threadPool.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    Connection conn = dataSource.getConnection();  // 获取 phoenix 连接
                    Jedis redisClient = RedisUtil.getRedisClient();
                    // 读取维度的时候需要表名, 搞个抽象方法, 返回表名
                    JSONObject dim = DimUtil.readDim(redisClient, conn, getTable(), getId(input));
                    // 给 input 中的确实维度补充数据
                    addDim(input, dim);
                    // input的数据已经补充完整了一个张维度表
                    // 来一条走一条, 传入一个只有一个对象集合
                    // 把数据输出, 放入到后续的流中
                    resultFuture.complete(Collections.singletonList(input));
                    
                    if (conn != null) {  // 线程内获取的连接,应该在使用的时候关闭
                        conn.close();
                    }
                    if (redisClient != null) {
                        redisClient.close();
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }
}
