package com.atguigu.gmall.realtime.app.dwd.db;

import com.atguigu.gmall.realtime.app.BaseSQLApp;
import com.atguigu.gmall.realtime.common.Constant;
import com.atguigu.gmall.realtime.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lzc
 * @Date 2022/9/22 11:11
 */
public class Dwd_05_DwdTradePayDetailSuc extends BaseSQLApp {
    public static void main(String[] args) {
        new Dwd_05_DwdTradePayDetailSuc().init(
            3005,
            2,
            "Dwd_05_DwdTradePayDetailSuc"
        );
    }
    
    @Override
    protected void handle(StreamExecutionEnvironment env,
                          StreamTableEnvironment tEnv) {
        // 1. 读取 ods_db
        readOdsDb(tEnv, "Dwd_05_DwdTradePayDetailSuc");
        
        // 2. 取字典表
        readBaseDic(tEnv);
        
        // 3. 从 ods_db 过滤出支付信息: update  payment_status = 1602
        Table paymentInfo = tEnv.sqlQuery("select " +
                                              "data['user_id'] user_id, " +
                                              "data['order_id'] order_id, " +
                                              "data['payment_type'] payment_type, " +
                                              "data['callback_time'] callback_time, " +
                                              "ts, " +
                                              "`pt` " +
                                              "from ods_db " +
                                              "where `database`='gmall2022' " +
                                              "and `table`='payment_info' " +
                                              "and `type`='update' " +
                                              "and `old`['payment_status'] is not null " +
                                              "and `data`['payment_status']='1602'");
        tEnv.createTemporaryView("payment_info", paymentInfo);
        // 4. 过滤出详情: insert
        // 5. 过滤订单表: update order_status=1002
        // 4 和 5 和并成从 下单详情表获取
        // dwd_trade_order_detail 这个张表有可能有重复数据
        tEnv.executeSql("create table dwd_trade_order_detail( " +
                            "id string, " +
                            "order_id string, " +
                            "user_id string, " +
                            "sku_id string, " +
                            "sku_name string, " +
                            "province_id string, " +
                            "activity_id string, " +
                            "activity_rule_id string, " +
                            "coupon_id string, " +
                            "date_id string, " +
                            "create_time string, " +
                            "source_id string, " +
                            "source_type_code string, " +
                            "source_type_name string, " +
                            "sku_num string, " +
                            "split_original_amount string, " +
                            "split_activity_amount string, " +
                            "split_coupon_amount string, " +
                            "split_total_amount string, " +
                            "ts bigint, " +
                            "row_op_ts timestamp_ltz(3) " + //
                            ")" + SQLUtil.getKafkaSourceSQL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL, "Dwd_05_DwdTradePayDetailSuc"));
        //        tEnv.sqlQuery("select * from dwd_trade_order_detail").execute().print();
        // 6. 3 张表的 join
        Table result = tEnv.sqlQuery("select " +
                                         "od.id order_detail_id, " +
                                         "od.order_id, " +
                                         "od.user_id, " +
                                         "od.sku_id, " +
                                         "od.sku_name, " +
                                         "od.province_id, " +
                                         "od.activity_id, " +
                                         "od.activity_rule_id, " +
                                         "od.coupon_id, " +
                                         "pi.payment_type payment_type_code, " +
                                         "dic.dic_name payment_type_name, " +
                                         "pi.callback_time, " +
                                         "od.source_id, " +
                                         "od.source_type_code, " +
                                         "od.source_type_name, " +
                                         "od.sku_num, " +
                                         "od.split_original_amount, " +
                                         "od.split_activity_amount, " +
                                         "od.split_coupon_amount, " +
                                         "od.split_total_amount split_payment_amount, " +
                                         "pi.ts, " +
                                         "od.row_op_ts row_op_ts " +
                                         "from payment_info pi " +
                                         "join dwd_trade_order_detail od on pi.order_id=od.order_id " +
                                         "join base_dic for system_time as of pi.pt as dic on pi.payment_type=dic.dic_code ");
        
      
        // 7. 创建表与 topic 关联
        tEnv.executeSql("create table dwd_trade_pay_detail_suc( " +
                                "order_detail_id string, " +
                                "order_id string, " +
                                "user_id string, " +
                                "sku_id string, " +
                                "sku_name string, " +
                                "province_id string, " +
                                "activity_id string, " +
                                "activity_rule_id string, " +
                                "coupon_id string, " +
                                "payment_type_code string, " +
                                "payment_type_name string, " +
                                "callback_time string, " +
                                "source_id string, " +
                                "source_type_code string, " +
                                "source_type_name string, " +
                                "sku_num string, " +
                                "split_original_amount string, " +
                                "split_activity_amount string, " +
                                "split_coupon_amount string, " +
                                "split_payment_amount string, " +
                                "ts bigint, " +
                                "row_op_ts timestamp_ltz(3) " +
                                ")" + SQLUtil.getKafkaSinkSQL(Constant.TOPIC_DWD_TRADE_PAY_DETAIL_SUC));
        // 8. 写出到输出表
        result.executeInsert("dwd_trade_pay_detail_suc");
    }
}
