package com.atguigu.gmall.realtime.util;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @Author lzc
 * @Date 2022/9/30 11:10
 */
public class ThreadPoolUtil {
    
    public static ThreadPoolExecutor getThreadPool(){
        return new ThreadPoolExecutor(
            300,  // 这个线程能提供的核心线程的上限
            500, // 最多可以把线程池中的线程数量上限拓展到 500
            60,
            TimeUnit.SECONDS, // 当扩展线程空闲超过 60s, 则销毁这个线程
            new LinkedBlockingQueue<>(100) // 超过 500 的线程会先存入到队列中
        );
    }
}
