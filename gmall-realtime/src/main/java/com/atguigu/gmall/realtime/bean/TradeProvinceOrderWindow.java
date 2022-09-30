package com.atguigu.gmall.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
@Builder
public class TradeProvinceOrderWindow {
    // 窗口起始时间
    String stt;
    // 窗口结束时间
    String edt;
    // 省份 ID
    String provinceId;
    // 省份名称
    String provinceName;
    
    // 累计下单次数
    Long orderCount;
    // 累计下单金额
    BigDecimal orderAmount;
    // 时间戳
    Long ts;
}