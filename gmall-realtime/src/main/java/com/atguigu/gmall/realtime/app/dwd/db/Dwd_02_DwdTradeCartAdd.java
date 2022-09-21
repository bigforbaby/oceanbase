package com.atguigu.gmall.realtime.app.dwd.db;

import com.atguigu.gmall.realtime.app.BaseSQLApp;
import com.atguigu.gmall.realtime.common.Constant;
import com.atguigu.gmall.realtime.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lzc
 * @Date 2022/9/19 15:09
 */
public class Dwd_02_DwdTradeCartAdd extends BaseSQLApp {
    public static void main(String[] args) {
        new Dwd_02_DwdTradeCartAdd().init(
            3002,
            2,
            "Dwd_02_DwdTradeCartAdd"
        );
    }
    
    @Override
    protected void handle(StreamExecutionEnvironment env,
                          StreamTableEnvironment tEnv) {
        // 1. 读取ods_db数据
        readOdsDb(tEnv, "Dwd_02_DwdTradeCartAdd");
        // 2. 过滤其中的 加购数据
        // empty set
        // 对更新来说,我们只关注 sku_num 变大的
        Table cartInfo = tEnv.sqlQuery("select " +
                                           " `data`['id'] id, " +
                                           " `data`['user_id'] user_id, " +
                                           " `data`['sku_id'] sku_id, " +
                                           " `data`['source_id'] source_id, " +
                                           " `data`['source_type'] source_type, " +
                                           // 如果是insert直接取, 如果update 取与old差值
                                           " if(`type`='insert', " +
                                           "  `data`['sku_num'], " +
                                           "   cast(cast(`data`['sku_num'] as int) - cast(`old`['sku_num'] as int) as string)" +
                                           " ) sku_num, " +
                                           " ts, " +
                                           " pt " +
                                           "from ods_db " +
                                           "where `database`='gmall2022' " +
                                           "and `table`='cart_info' " +
                                           "and (`type`='insert'  " +
                                           "  or " +
                                           " (`type`='update' " +
                                           "     and `old`['sku_num'] is not null " +
                                           "     and cast(`data`['sku_num'] as int) > cast(`old`['sku_num'] as int)" +
                                           "  )" +
                                           ") ");
        tEnv.createTemporaryView("cart_info", cartInfo);
        // 3. 地区维度表: 字典表
        readBaseDic(tEnv);
        // 4. 把字典表中的数据退化到加购表
        // 使用join ? join, left join, lookup join
        Table result = tEnv.sqlQuery("select " +
                                        "cadd.id, " +
                                        "user_id, " +
                                        "sku_id, " +
                                        "source_id, " +
                                        "source_type, " +
                                        "dic_name source_type_name, " +
                                        "sku_num, " +
                                        "ts " +
                                        "from cart_info as cadd " +
                                        "join base_dic for system_time as of cadd.pt as dic " +
                                        "on cadd.source_type=dic.dic_code");
        
        // 5. 把join后的明细数据写入到kafka的topic中
        // kafka or upsert-kafka
        tEnv.executeSql(
            "create table dwd_trade_cart_add( " +
                "id string, " +
                "user_id string, " +
                "sku_id string, " +
                "source_id string, " +
                "source_type_code string, " +
                "source_type_name string, " +
                "sku_num string, " +
                "ts bigint " +
                ")" + SQLUtil.getKafkaSinkSQL(Constant.TOPIC_DWD_TRADE_CART_ADD)
        );
        result.executeInsert("dwd_trade_cart_add");
        
    }
}
/*
加购事实表

1. 数据源
 来源于 ods_db

2. 过滤出需要的表
    cart_info

3. 找到加购行为
   
   insert 数据肯定需要
   update ?

   张三   苹果手机     1    // insert
   张三   苹果手机     3    // update 新增2个手机


   {张三   苹果手机     1}  // 这个直接存储

   {张三   苹果手机     3 - 1}  // 这个能否直接存3

 4 ....

 5. 在做明细的时候, 需要把字典表的值做一个退化
    维度退化
    sql join
------------
交易域的数据, 都是来源业务表, 是一种关系型数据库

流的方式处理
sql的方式
    1. 比较适合结构化数据处理
    2. sql处理起来比较简单
    3. 企业用sql的比较多
 */