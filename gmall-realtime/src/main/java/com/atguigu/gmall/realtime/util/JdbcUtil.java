package com.atguigu.gmall.realtime.util;

import com.atguigu.gmall.realtime.common.Constant;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * @Author lzc
 * @Date 2022/9/17 9:16
 */
public class JdbcUtil {
    public static void main(String[] args) {
        
    }
    
    public static Connection getPhoenixConnection() {
        String driver = Constant.PHOENIX_DRIVER;
        String url = Constant.PHOENIX_URL;
        
        return getJdbcConnection(driver, url, null, null);
        
    }
    
    public static Connection getJdbcConnection(String driver,
                                               String url,
                                               String user,
                                               String password) {
        try {
            Class.forName(driver);
            return DriverManager.getConnection(url, user, password);
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
        return null;
    }
    
    public static void closeConnection(Connection conn) {
        try {
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
