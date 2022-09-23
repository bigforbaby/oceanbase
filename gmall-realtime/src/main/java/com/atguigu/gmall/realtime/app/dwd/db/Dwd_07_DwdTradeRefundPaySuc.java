package com.atguigu.gmall.realtime.app.dwd.db;

import com.atguigu.gmall.realtime.app.BaseSQLApp;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @Author lzc
 * @Date 2022/9/23 09:11
 */
public class Dwd_07_DwdTradeRefundPaySuc extends BaseSQLApp {
    public static void main(String[] args) {
        new Dwd_07_DwdTradeRefundPaySuc().init(
            3007,
            2,
            "Dwd_07_DwdTradeRefundPaySuc"
        );
    }
    
    @Override
    protected void handle(StreamExecutionEnvironment env,
                          StreamTableEnvironment tEnv) {
        // 1. 读取 ods_db
        readOdsDb(tEnv, "Dwd_07_DwdTradeRefundPaySuc");
        
        // 2. 读取字典表
        readBaseDic(tEnv);
        
        // 3. 从 ods_db 中过滤出退款表
        Table refundPayment = tEnv.sqlQuery("select " +
                                                "data['id'] id," +
                                                "data['order_id'] order_id," +
                                                "data['sku_id'] sku_id," +
                                                "data['payment_type'] payment_type," +
                                                "data['callback_time'] callback_time," +
                                                "data['total_amount'] total_amount," +
                                                "pt," +
                                                "ts " +
                                                "from ods_db " +
                                                "where `database`='gmall2022' " +
                                                "and `table`='refund_payment' "
                                                + "and `type`<>'delete' "
                                            //                       +    "and `type`='update' " +
                                            //                          "and `old`['refund_status'] is not null " +
                                            //                          "and `data`['refund']='0702'"
        );
        tEnv.createTemporaryView("refund_payment", refundPayment);
        // 4. 从 ods_db 中过滤出 order_info中的退款信息
        Table orderInfo = tEnv.sqlQuery("select " +
                                            "data['id'] id," +
                                            "data['user_id'] user_id," +
                                            "data['province_id'] province_id," +
                                            "`old` " +
                                            "from ods_db " +
                                            "where `database`='gmall2022' " +
                                            "and `table`='order_info'  " +
                                            "and `type`='update'  " +
                                            "and `old`['order_status'] is not null " +
                                            "and `data`['order_status']='1006' ");
        tEnv.createTemporaryView("order_info", orderInfo);
        
        // 5. 从 ods_db 过滤出退单详情
        Table orderRefundInfo = tEnv.sqlQuery("select " +
                                                  "data['order_id'] order_id," +
                                                  "data['sku_id'] sku_id," +
                                                  "data['refund_num'] refund_num," +
                                                  "`old`" +
                                                  "from ods_db " +
                                                  "where `database`='gmall2022' " +
                                                  "and `table`='order_refund_info'  "
                                                  + "and `type`<>'delete' "  // 删除旧数据, 所以把旧数据过滤掉
                                              //                          + "and `type`='update' " +
                                              //                          "and `old`['refund_status'] is not null " +
                                              //                          "and `data`['refund_status']='0705' " +
        );
        // 当退款成功之后,三张事实表是同时更新的
        tEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5));
        // 6. 4 张表做 join
        
        // 7. 写出到 kafka 中
        
    }
}
