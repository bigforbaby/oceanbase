package com.atguigu.gmall.realtime.util;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.Constant;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.flink.shaded.guava18.com.google.common.base.CaseFormat;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author lzc
 * @Date 2022/9/17 9:16
 */
public class JdbcUtil {
    
    
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
    
    public static <T> List<T> queryList(Connection conn,
                                        String sql,
                                        Object[] args,
                                        Class<T> tClass,
                                        boolean... isCame) {
        // 从数据读取的字段名, 是否要转成驼峰
        boolean underlineToCame = false;
        if (isCame.length > 0) {
            underlineToCame = isCame[0];
        }
        
        
        List<T> list = new ArrayList<>();
        try {
            // 1. 先根据连接对象获取一个预处理语句
            PreparedStatement ps = conn.prepareStatement(sql);
            // 2. sql中有可能会有占位符, 给占位符复制
            for (int i = 0; args != null && i < args.length; i++) {
                ps.setObject(i + 1, args[i]);
            }
            
            // 3. 执行查询
            ResultSet resultSet = ps.executeQuery();
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();
            // 4. 解析查询结果, 封装到 T 类型的对象中, 存入到List 集合中
            while (resultSet.next()) {
                
                
                /*
                    id   name   age
                    1    lisi    10
                    2    ww      20
                    3    zl      30
                 */
                // 循环进来一次, 表示读取到了结果中的一行数据. 把这个一样的每一列数据, 封装到一个 T 类型的对象中
                T t = tClass.newInstance();  // 创建一个 T 类型的对象
                for (int i = 1; i <= columnCount; i++) {
                    String name = metaData.getColumnLabel(i);
                    if (underlineToCame) { // 如果是 true,表示把下划线转成驼峰
                        name = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, name);
                    }
                    
                    Object v = resultSet.getObject(i);
                    // 给t 对象的 name 属性赋值为 v      t.name=value
                    BeanUtils.setProperty(t, name, v);
                }
                list.add(t);
            }
            
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return list;
    }
    
    
    public static void main(String[] args) {
        //        Connection conn = JdbcUtil.getPhoenixConnection();
        //        List<JSONObject> list = queryList(conn, "select * from dim_sku_info where id=?", new String[]{"10"}, JSONObject.class);
        //        List<JSONObject> list = queryList(conn, "select * from dim_sku_info", new String[]{}, JSONObject.class);
        
        Connection conn = JdbcUtil.getJdbcConnection("com.mysql.cj.jdbc.Driver", "jdbc:mysql://hadoop162:3306/gmall_config", "root", "aaaaaa");
                List<JSONObject> list = queryList(conn, "select * from table_process", null, JSONObject.class, true);
//        List<TableProcess> list = queryList(conn, "select * from table_process", null, TableProcess.class, true);
        for (JSONObject obj : list) {
            System.out.println(obj);
        }
    }
}
