package com.atguigu.gmall.realtime.util;

import com.alibaba.fastjson.JSONObject;

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
        List<JSONObject> list =  JdbcUtil.queryList(conn, sql, new String[]{id}, JSONObject.class);
        return list.get(0);
    }
    
}
