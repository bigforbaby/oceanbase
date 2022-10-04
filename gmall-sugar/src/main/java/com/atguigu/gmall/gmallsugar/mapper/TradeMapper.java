package com.atguigu.gmall.gmallsugar.mapper;

import com.atguigu.gmall.gmallsugar.bean.Spu;
import com.atguigu.gmall.gmallsugar.bean.Tm;
import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;
import java.util.List;

public interface TradeMapper {

    @Select("SELECT sum(order_amount) AS order_amount " +
        "FROM dws_trade_sku_order_window " +
        "WHERE toYYYYMMDD(stt) = #{date}")
    BigDecimal gmv(int date);
    
    
    @Select("SELECT\n" +
        "    spu_name,\n" +
        "    sum(order_amount) AS order_amount\n" +
        "FROM dws_trade_sku_order_window\n" +
        "WHERE toYYYYMMDD(stt) = #{date}\n" +
        "GROUP BY spu_name order by order_amount desc limit 5")
    List<Spu> gmvBySpu(int date);
    
    @Select("SELECT\n" +
        "    trademark_name tm_name,\n" +
        "    sum(order_amount) AS order_amount\n" +
        "FROM dws_trade_sku_order_window\n" +
        "WHERE toYYYYMMDD(stt) = #{date}\n" +
        "GROUP BY trademark_name")
    List<Tm> gmvByTm(int date);
}
