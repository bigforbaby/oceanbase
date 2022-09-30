package com.atguigu.gmall.realtime.bean;

import com.atguigu.gmall.realtime.annotation.NoNeedSink;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;
import java.util.Set;

@Data
@AllArgsConstructor
@Builder
public class TradeSkuOrderBean {
    // 窗口起始时间
//    @Builder.Default
    String stt;
    // 窗口结束时间
    String edt;
    // 品牌 ID
    String trademarkId;
    // 品牌名称
    String trademarkName;
    // 一级品类 ID
    String category1Id;
    // 一级品类名称
    String category1Name;
    // 二级品类 ID
    String category2Id;
    // 二级品类名称
    String category2Name;
    // 三级品类 ID
    String category3Id;
    // 三级品类名称
    String category3Name;
    // sku_id
    String skuId;
    // sku 名称
    String skuName;
    // spu_id
    String spuId;
    // spu 名称
    String spuName;
   
    // 原始金额
    BigDecimal originalAmount;
    // 活动减免金额
    BigDecimal activityAmount;
    // 优惠券减免金额
    BigDecimal couponAmount;
    // 下单金额
    BigDecimal orderAmount;
    
   @NoNeedSink
    Set<String> orderIdSet;
    // 订单数: 这个商品被下单的个数
    // 怎么计算? 有一个订单就 +1 不是很准.
    // 应该把订单 id 存入到一个set集合, 等到聚合的最终结果的时候, 把数量设置为集合的长度
    Long orderCount;
    
    // 时间戳
    Long ts;
    
    public static void main(String[] args) {
    
    }
}