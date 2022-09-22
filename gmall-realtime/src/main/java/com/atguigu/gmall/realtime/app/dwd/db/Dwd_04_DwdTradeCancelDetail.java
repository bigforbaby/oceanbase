package com.atguigu.gmall.realtime.app.dwd.db;

import com.atguigu.gmall.realtime.app.BaseSQLApp;
import com.atguigu.gmall.realtime.common.Constant;
import com.atguigu.gmall.realtime.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @Author lzc
 * @Date 2022/9/21 14:57
 */
public class Dwd_04_DwdTradeCancelDetail extends BaseSQLApp {
    public static void main(String[] args) {
        new Dwd_04_DwdTradeCancelDetail().init(
            3004,
            2,
            "Dwd_04_DwdTradeCancelDetail"
        );
    }
    
    @Override
    protected void handle(StreamExecutionEnvironment env,
                          StreamTableEnvironment tEnv) {
        // 由于取消发生的比较晚, 取 30min
        tEnv.getConfig().setIdleStateRetention(Duration.ofMinutes(30));
        // 1. 先读取 ods_db
        readOdsDb(tEnv, "Dwd_04_DwdTradeCancelDetail");
        // 2. 读取字典表
        readBaseDic(tEnv);
        // 3. 过滤详情: 只要 insert
        Table orderDetail = tEnv.sqlQuery("select " +
                                              "data['id'] id, " +
                                              "data['order_id'] order_id, " +
                                              "data['sku_id'] sku_id, " +
                                              "data['sku_name'] sku_name, " +
                                              "data['create_time'] create_time, " +
                                              "data['source_id'] source_id, " +
                                              "data['source_type'] source_type, " +
                                              "data['sku_num'] sku_num, " +
                                              "cast( cast(data['sku_num'] as decimal(16,2)) * " +
                                              "  cast(data['order_price'] as decimal(16,2)) as String) split_original_amount, " +
                                              "data['split_total_amount'] split_total_amount, " +
                                              "data['split_activity_amount'] split_activity_amount, " +
                                              "data['split_coupon_amount'] split_coupon_amount, " +
                                              "ts, " +
                                              "pt " +
                                              "from ods_db " +
                                              "where `database`='gmall2022' " +
                                              "and `table`='order_detail' " +
                                              "and `type`='insert'");
        tEnv.createTemporaryView("order_detail", orderDetail);
        // 4. 过滤订单表: 只要 update  并且是订单的状态发生变化, 变成了 1003
        Table orderInfo = tEnv.sqlQuery("select " +
                                            "data['id'] id," +
                                            "data['user_id'] user_id," +
                                            "data['province_id'] province_id " +
                                            "from ods_db " +
                                            "where `database`='gmall2022' " +
                                            "and `table`='order_info' " +
                                            "and `type`='update' " +
                                            "and `old`['order_status'] is not null " +
                                            "and `data`['order_status']='1003' "  // 订单状态发生了变化
        );
        tEnv.createTemporaryView("order_info", orderInfo);
        
        // 5. 详情活动: 只要 insert
        Table orderDetailActivity = tEnv.sqlQuery("select " +
                                                      "data['order_detail_id'] order_detail_id, " +
                                                      "data['activity_id'] activity_id, " +
                                                      "data['activity_rule_id'] activity_rule_id " +
                                                      "from ods_db " +
                                                      "where `database`='gmall2022' " +
                                                      "and `table`='order_detail_activity' " +
                                                      "and `type`='insert'");
        tEnv.createTemporaryView("order_detail_activity", orderDetailActivity);
        // 6. 详情优惠券: 只要 insert
        Table orderDetailCoupon = tEnv.sqlQuery("select " +
                                                    "data['order_detail_id'] order_detail_id, " +
                                                    "data['coupon_id'] coupon_id " +
                                                    "from ods_db " +
                                                    "where `database`='gmall2022' " +
                                                    "and `table`='order_detail_coupon' " +
                                                    "and `type`='insert'");
        tEnv.createTemporaryView("order_detail_coupon", orderDetailCoupon);
        // 7. 5 张表做 join
        Table result = tEnv.sqlQuery("select " +
                                         "od.id, " +
                                         "od.order_id, " +
                                         "oi.user_id, " +
                                         "od.sku_id, " +
                                         "od.sku_name, " +
                                         "oi.province_id, " +
                                         "act.activity_id, " +
                                         "act.activity_rule_id, " +
                                         "cou.coupon_id, " +
                                         "date_format(od.create_time, 'yyyy-MM-dd') date_id, " +  // 后期有用
                                         "od.create_time, " +
                                         "od.source_id, " +
                                         "od.source_type, " +
                                         "dic.dic_name source_type_name, " +
                                         "od.sku_num, " +
                                         "od.split_original_amount, " +
                                         "od.split_activity_amount, " +
                                         "od.split_coupon_amount, " +
                                         "od.split_total_amount, " +
                                         "od.ts, " +
                                         // 返回每行数据计算的实时的时间: 用于后期的去重处理. 找这个时间最大
                                         "current_row_timestamp() row_op_ts " +
                                         "from order_detail od " +
                                         "join order_info oi on od.order_id=oi.id " +
                                         "left join order_detail_activity act on od.id=act.order_detail_id " +
                                         "left join order_detail_coupon cou on od.id=cou.order_detail_id " +
                                         "join base_dic for system_time as of od.pt as dic on od.source_type=dic.dic_code");
        // 8. 建动态表与 kafka 的 topic 关联
        tEnv.executeSql("create table dwd_trade_cancel_detail( " +
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
                            "source_type string, " +
                            "source_type_name string, " +
                            "sku_num string, " +
                            "split_original_amount string, " +
                            "split_activity_amount string, " +
                            "split_coupon_amount string, " +
                            "split_total_amount string, " +
                            "ts bigint, " +
                            "row_op_ts timestamp_ltz(3), " +
                            "primary key(id) NOT ENFORCED" +
                            ")" + SQLUtil.getUpsertKafkaSinkSQL(Constant.DWD_TRADE_CANCEL_DETAIL));
        
        // 9. 写出创建的动态表中
        result.executeInsert("dwd_trade_cancel_detail");
    }
}
/*



 */