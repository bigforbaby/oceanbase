package com.atguigu.gmall.realtime.util;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @Author lzc
 * @Date 2022/9/30 09:49
 */
public class RedisUtil {
    
    private static JedisPool pool;
    
    static {
        GenericObjectPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(100); // 最多提供多少个客户端
        config.setMaxIdle(2); // 最多的空闲客户端数量
        config.setMinIdle(1);
        config.setMaxWaitMillis(10 * 1000);
        config.setTestOnCreate(true);
        config.setTestOnCreate(true);
        config.setTestOnReturn(true);
        
        pool = new JedisPool(config, "hadoop162", 6379);
        
    
    }
    public static Jedis getRedisClient(){
        
        return pool.getResource();
    }
}
