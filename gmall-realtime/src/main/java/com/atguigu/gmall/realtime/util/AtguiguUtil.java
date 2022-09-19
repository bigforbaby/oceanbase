package com.atguigu.gmall.realtime.util;

import java.text.SimpleDateFormat;

/**
 * @Author lzc
 * @Date 2022/9/19 10:16
 */
public class AtguiguUtil {
    
    public static String toDate(Long ts) {
        return new SimpleDateFormat("yyyy-MM-dd").format(ts);
    }
}
