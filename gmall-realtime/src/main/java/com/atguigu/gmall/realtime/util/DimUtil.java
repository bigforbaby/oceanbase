package com.atguigu.gmall.realtime.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.Constant;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.util.List;

/**
 * @Author lzc
 * @Date 2022/9/29 15:23
 */
public class DimUtil {
    // "10 or 1=1"
    public static JSONObject readDimFromPhoenix(Connection conn, String table, String id) {
        // select * from t where id=10 or 1=1
        String sql = "select * from " + table + " where id=?";  // sql注入
        
        // 通用的查询方法: 返回一个 list 集合, 每个元素算是查询到一行数据
        List<JSONObject> list = JdbcUtil.queryList(conn, sql, new String[]{id}, JSONObject.class);
        return list.get(0);
    }
    
    public static JSONObject readDim(Jedis redisClient, Connection conn, String table, String id) {
        // 1. 先从 redis 读取维度
        JSONObject dim = readDimFromRedis(redisClient, table, id);
        // 2. 如果没有读到, 从 Phoenix 读取, 还要把维度写入到 redis
        if (dim == null) {
    
            System.out.println("走的数据库: " + table  + "  " + id);
            dim = readDimFromPhoenix(conn, table,id);
            
            writeToRedis(redisClient, table, id, dim);
        }else{
            System.out.println("走的缓存: " + table  + "  " + id);
        }
        // 3. 如果读取到了, 直接返回
        return dim;
    }
    
    // 把维度数据写入到 redis
    private static void writeToRedis(Jedis redisClient, String table, String id, JSONObject dim) {
        // 写: key= table ":" id
        // value: dim
        String key = getRedisKey(table, id);
        /*redisClient.set(key, dim.toJSONString());
        redisClient.expire(key, Constant.TTL_TWO_DAYS); // two days 的 ttl*/
    
        redisClient.setex(key, Constant.TTL_TWO_DAYS, dim.toJSONString());
        
    }
 
    
    // 从 redis 读取数据
    private static JSONObject readDimFromRedis(Jedis redisClient, String table, String id) {
        String key = getRedisKey(table, id);
        String str = redisClient.get(key);
        JSONObject dim = null;
        if (str != null) {
            dim = JSON.parseObject(str);
        }
        return dim;
    }
    
    
    private static String getRedisKey(String table, String id) {
        return table + ":" + id;
    }
}
