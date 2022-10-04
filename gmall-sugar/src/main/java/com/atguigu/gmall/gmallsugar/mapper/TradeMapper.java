package com.atguigu.gmall.gmallsugar.mapper;

import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;

public interface TradeMapper {

    @Select("SELECT sum(order_amount) AS order_amount " +
        "FROM dws_trade_sku_order_window " +
        "WHERE toYYYYMMDD(stt) = #{date}")
    BigDecimal gmv(int date);
}
