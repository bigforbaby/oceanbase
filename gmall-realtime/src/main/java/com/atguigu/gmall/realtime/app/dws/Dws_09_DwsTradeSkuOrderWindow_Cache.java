package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.BaseAppV1;
import com.atguigu.gmall.realtime.bean.TradeSkuOrderBean;
import com.atguigu.gmall.realtime.common.Constant;
import com.atguigu.gmall.realtime.util.AtguiguUtil;
import com.atguigu.gmall.realtime.util.DimUtil;
import com.atguigu.gmall.realtime.util.DruidDSUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.sql.Connection;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;

/**
 * @Author lzc
 * @Date 2022/9/29 11:18
 */
public class Dws_09_DwsTradeSkuOrderWindow_Cache extends BaseAppV1 {
    public static void main(String[] args) {
        new Dws_09_DwsTradeSkuOrderWindow_Cache().init(
            4009,
            2,
            "Dws_09_DwsTradeSkuOrderWindow_Cache",
            Constant.TOPIC_DWD_TRADE_ORDER_DETAIL
        );
    }
    
    @Override
    protected void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 1. 先按照详情id 进行去重
        SingleOutputStreamOperator<JSONObject> distinctedStream = distinctByOrderDetailId(stream);
        // 2. 把数据封装到 pojo 中
        SingleOutputStreamOperator<TradeSkuOrderBean> beanStream = parseToPojo(distinctedStream);
        // 3. 开窗聚合
        // keyBy: 按照 sku_id
        SingleOutputStreamOperator<TradeSkuOrderBean> resultStreamWithoutDims = windowAndAgg(beanStream);
        // 4. 补充维度信息
        SingleOutputStreamOperator<TradeSkuOrderBean> resultStreamWithDims =  addDim(resultStreamWithoutDims);
        resultStreamWithDims.print();
        // 2个优化
        
        
        
        // 5. 写出到 clickhouse 中
    }
    
    private SingleOutputStreamOperator<TradeSkuOrderBean> addDim(SingleOutputStreamOperator<TradeSkuOrderBean> stream) {
        return stream.map(new RichMapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {
    
            private Connection conn;
    
            @Override
            public void open(Configuration parameters) throws Exception {
                DruidDataSource dataSource = DruidDSUtil.getDataSource();
                conn = dataSource.getConnection();
            }
    
            @Override
            public TradeSkuOrderBean map(TradeSkuOrderBean bean) throws Exception {
                // select * from t where id=?
                // 1. sku_info
                JSONObject skuInfo = DimUtil.readDimFromPhoenix(conn, "dim_sku_info", bean.getSkuId());
                bean.setSkuName(skuInfo.getString("SKU_NAME")); // phoenix中的字段名默认都大写
                bean.setSpuId(skuInfo.getString("SPU_ID")); // phoenix中的字段名默认都大写
                bean.setTrademarkId(skuInfo.getString("TM_ID"));
                bean.setCategory3Id(skuInfo.getString("CATEGORY3_ID"));
                
                // 2. base_trademark
                JSONObject baseTrademark = DimUtil.readDimFromPhoenix(conn, "dim_base_trademark", bean.getTrademarkId());
                bean.setTrademarkName(baseTrademark.getString("TM_NAME"));
                
                // 3. spu_info
                JSONObject spuInfo = DimUtil.readDimFromPhoenix(conn, "dim_spu_info", bean.getSpuId());
                bean.setSpuName(spuInfo.getString("SPU_NAME"));
                
                // 4. c2
                JSONObject c3 = DimUtil.readDimFromPhoenix(conn, "dim_base_category3", bean.getCategory3Id());
                bean.setCategory3Name(c3.getString("NAME"));
                bean.setCategory2Id(c3.getString("CATEGORY2_ID"));
                
                // 5. c2
                JSONObject c2 = DimUtil.readDimFromPhoenix(conn, "dim_base_category2", bean.getCategory2Id());
                bean.setCategory2Name(c2.getString("NAME"));
                bean.setCategory1Id(c2.getString("CATEGORY1_ID"));
                
                // 5. c1
                JSONObject c1 = DimUtil.readDimFromPhoenix(conn, "dim_base_category1", bean.getCategory1Id());
                bean.setCategory1Name(c1.getString("NAME"));
                
                return bean;
            }
        });
    }
    
    private SingleOutputStreamOperator<TradeSkuOrderBean> windowAndAgg(
        SingleOutputStreamOperator<TradeSkuOrderBean> beanStream) {
        return beanStream
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<TradeSkuOrderBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner((bean, ts) -> bean.getTs())
            )
            .keyBy(TradeSkuOrderBean::getSkuId)
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .reduce(
                new ReduceFunction<TradeSkuOrderBean>() {
                    @Override
                    public TradeSkuOrderBean reduce(TradeSkuOrderBean b1,
                                                    TradeSkuOrderBean b2) throws Exception {
                        b1.setOriginalAmount(b1.getOriginalAmount().add(b2.getOrderAmount()));
                        b1.setActivityAmount(b1.getActivityAmount().add(b2.getActivityAmount()));
                        b1.setCouponAmount(b1.getCouponAmount().add(b2.getCouponAmount()));
                        b1.setOrderAmount(b1.getOrderAmount().add(b2.getOrderAmount()));
                        
                        b1.getOrderIdSet().addAll(b2.getOrderIdSet());
                        return b1;
                    }
                },
                new ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>() {
                    @Override
                    public void process(String skuId,
                                        Context ctx,
                                        Iterable<TradeSkuOrderBean> elements,
                                        Collector<TradeSkuOrderBean> out) throws Exception {
                        TradeSkuOrderBean bean = elements.iterator().next();
                        bean.setStt(AtguiguUtil.toDateTime(ctx.window().getStart()));
                        bean.setEdt(AtguiguUtil.toDateTime(ctx.window().getEnd()));
    
                        bean.setTs(System.currentTimeMillis());
                        
                        bean.setOrderCount((long) bean.getOrderIdSet().size());
                        
                        out.collect(bean);
    
                    }
                }
            );
    }
    
    private SingleOutputStreamOperator<TradeSkuOrderBean> parseToPojo(SingleOutputStreamOperator<JSONObject> steam) {
        return steam
            .map(obj -> TradeSkuOrderBean.builder()
                .skuId(obj.getString("sku_id"))
                .orderIdSet(new HashSet<>(Collections.singletonList(obj.getString("order_id"))))
                .originalAmount(obj.getBigDecimal("split_original_amount"))
                .activityAmount(obj.getBigDecimal("split_activity_amount") == null ? new BigDecimal(0) : obj.getBigDecimal("split_activity_amount"))
                .couponAmount(obj.getBigDecimal("split_coupon_amount") == null ? new BigDecimal(0) : obj.getBigDecimal("split_coupon_amount"))
                .orderAmount(obj.getBigDecimal("split_total_amount"))
                .ts(obj.getLong("ts") * 1000)
                .build());
    }
    
    private SingleOutputStreamOperator<JSONObject> distinctByOrderDetailId(DataStreamSource<String> stream) {
        
        /*
        去重的逻辑:
            row_op_ts 保存这个时间最大的那条数据
            需要把同一个详情 id 的数据放在一起进行比较
        
        1.开窗
            同一个详情 id 的数据放在同一个窗口中
            什么窗口? 基于个数的不行, 应该同一个详情将来有多少条数据不确定
                    用基于时间
                        用 session 窗口
                            可以.  gap 多少? 5s
                            
            
            最后一条数据来了之后, 5s 之后才会出最终结果
            
        2. 定时器
            第一条数据来了之后, 注册定时器: 5s 后触发的定时器
                定时器触发之前, 每来一条数据都进行比较,保留下时间打的那个. 定时器触发的时候, 状态只存储的就是最大的那个
                
            实效性要比开窗要高
            
         3. 如果我需要统计的字段都在左表, 右边根本就没有用到.
            直接保留第一条
         */
        return stream
            .map(JSON::parseObject)
            /* .assignTimestampsAndWatermarks(
                 WatermarkStrategy
                     .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                     .withTimestampAssigner((obj, ts) -> obj.getLong("ts") * 1000)
             )*/
            .keyBy(obj -> obj.getString("id"))
            .process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                
                private ValueState<JSONObject> rowOpTsMaxDataState;
                
                @Override
                public void open(Configuration parameters) throws Exception {
                    rowOpTsMaxDataState = getRuntimeContext().getState(new ValueStateDescriptor<JSONObject>("rowOpTsMaxDataState", JSONObject.class));
                }
                
                @Override
                public void processElement(JSONObject obj,
                                           Context ctx,
                                           Collector<JSONObject> out) throws Exception {
                    // 如果不是第一条, 新数据和状态中的数据进行比较, 保留时间大的那个
                    JSONObject rowOpTsMaxData = rowOpTsMaxDataState.value();
                    
                    if (rowOpTsMaxData == null) {
                        long ts = obj.getLong("ts") * 1000;
                        // 当第一条来的时候注册定时器
                        ctx.timerService().registerProcessingTimeTimer(ts + 5000);
                        
                        // 数据存入到状态中
                        rowOpTsMaxDataState.update(obj);
                    } else {
                        String preTime = rowOpTsMaxData.getString("row_op_ts");
                        String currentTime = obj.getString("row_op_ts");
                        // 2022-09-29 03:38:59.055Z
                        // 07z=70 ms
                        // 071z=71 ms
                        // 如果currentTime > preTime, 更新状态
                        boolean greater = AtguiguUtil.isGreater(currentTime, preTime);
                        if (greater) {
                            rowOpTsMaxDataState.update(obj);
                        }
                    }
                }
                
                @Override
                public void onTimer(long timestamp,
                                    OnTimerContext ctx,
                                    Collector<JSONObject> out) throws Exception {
                    // 当定时器触发的时候, 最全的已经到了
                    JSONObject obj = rowOpTsMaxDataState.value();
                    out.collect(obj);
                }
            });
        
        
    }
}
/*
补充维度信息:
    每来一条数据, 就需要查询一次6 张维度表
    sku_id = 10
        多次频繁的使用, 多次去查询数据.
        影响: 1. 影响实时的实效性 2. 对数据库也有一定的压力
        
优化 1:
    缓存(内存)
    先查缓存, 缓存没有,再去查数据库, 然后存储到缓存中
a: flink 的状态中(堆内存)
    好处:
        1. 本地内存. 所以不经过网络, 读写速度特别快
        2. 状态有多种数据结果供选择, 使用也比较方便
        
    坏处:
        1. 额外占据 flink 的内存, 影响 flink 的计算
        2. 当维度发生变化的时候, 没有办法及时更新

b: redis 旁路缓存
    好处:
        1. 当维度发生变化的时候,可以及时更新
     
     坏处:
        1. 访问 redis 经过, 相比堆内存, 要慢
        2. 堆 redis 的内存也有一定的压力
        
----------------------------
redis 中数据结构的选择
1. string  选择这种数据
key             value
table+id        json格式的字符串
sku:10        {"id": 10, "sku_name": "", ...}

优点:
    1. 读取特点方便
    2. 方便给每条数据单独设置 ttl
缺点:
    每条维度都需要占用一个 key, key 过多
    单独的放入一个库中 4 号
   
2. list
key                 value
table               {}, {}, {},....
sku_id              ...
缺点:
    1. 无法单独给每条数据设置 ttl
    2. 写比较方便, 查的时候需要全部独处, 遍历

2. set
也不能

4. hash
key                 field   value
table               id      {...}

优点:
    1. key 有限
    2. 读取方便
缺点:
    没有办法单独对每条维度数据设置 ttl

5. zset




------

sku 粒度
    多了一些维度信息

1. 去重
    为什么会有重复数据?
     在 dwd 层下单事务事实表中有 left join
    
    按照订单详情 id 来去重
     a: 使用 session 窗口
        实现性比较低: 最后一条数据到来之后 5s 才会出结果
     b: 定时器
        使用处理时间定时器
            第一条数据来了之后 5s 后出结果
     c: 如果用到右表的数据, 直接取第一条数据
        第一条来了之后直接出结果
2. 解析成 pojo
    封装到 pojo
        builder
        
3. 开窗聚合
    keyBy   开窗 聚合
    
4. 补充维度信息

*/


















