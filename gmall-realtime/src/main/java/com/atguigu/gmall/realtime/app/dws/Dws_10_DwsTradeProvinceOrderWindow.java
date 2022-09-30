package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.BaseAppV1;
import com.atguigu.gmall.realtime.bean.TradeProvinceOrderWindow;
import com.atguigu.gmall.realtime.common.Constant;
import com.atguigu.gmall.realtime.function.DimAsyncFunction;
import com.atguigu.gmall.realtime.util.AtguiguUtil;
import com.atguigu.gmall.realtime.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * @Author lzc
 * @Date 2022/9/30 15:09
 */
public class Dws_10_DwsTradeProvinceOrderWindow extends BaseAppV1 {
    public static void main(String[] args) {
        new Dws_10_DwsTradeProvinceOrderWindow().init(
            4010,
            2,
            "Dws_10_DwsTradeProvinceOrderWindow",
            Constant.TOPIC_ODS_DB
        );
    }
    
    @Override
    protected void handle(StreamExecutionEnvironment env,
                          DataStreamSource<String> stream) {
        // 1. 从 ods_db 读取数据, 过滤出 order_info中的 insert\
        
        // 2. 把数据封装到 pojo 中
        
        // 3. 开窗聚合
        
        // 4. 补充省份信息
        
        // 5. 写出到 clickhouse 中
        SingleOutputStreamOperator<TradeProvinceOrderWindow> beanStream = stream
            .map(JSON::parseObject)
            .filter(obj -> "gmall2022".equals(obj.getString("database"))
                && "order_info".equals(obj.getString("table"))
                && "insert".equals(obj.getString("type")))
            .map(obj -> {
                JSONObject data = obj.getJSONObject("data");
                System.out.println(data.getString("create_time") + "   " + AtguiguUtil.dateTimeToTs(data.getString("create_time")));
                return TradeProvinceOrderWindow.builder()
                    .provinceId(data.getString("province_id"))
                    .orderCount(1L)
                    .orderAmount(data.getBigDecimal("total_amount"))
                    .ts(AtguiguUtil.dateTimeToTs(data.getString("create_time")))
                    .build();
            })
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<TradeProvinceOrderWindow>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner((bean, ts) -> bean.getTs())
                    .withIdleness(Duration.ofSeconds(10))
            )
            .keyBy(TradeProvinceOrderWindow::getProvinceId)
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .reduce(
                new ReduceFunction<TradeProvinceOrderWindow>() {
                    @Override
                    public TradeProvinceOrderWindow reduce(TradeProvinceOrderWindow value1,
                                                           TradeProvinceOrderWindow value2) throws Exception {
                        value1.setOrderCount(value1.getOrderCount() + value2.getOrderCount());
                        value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                        
                        return value1;
                    }
                },
                new ProcessWindowFunction<TradeProvinceOrderWindow, TradeProvinceOrderWindow, String, TimeWindow>() {
                    @Override
                    public void process(String provinceId,
                                        Context context,
                                        Iterable<TradeProvinceOrderWindow> elements,
                                        Collector<TradeProvinceOrderWindow> out) throws Exception {
                        
                        TradeProvinceOrderWindow bean = elements.iterator().next();
                        
                        bean.setStt(AtguiguUtil.toDateTime(context.window().getStart()));
                        bean.setEdt(AtguiguUtil.toDateTime(context.window().getEnd()));
                        
                        bean.setTs(System.currentTimeMillis());
                        
                        out.collect(bean);
                        
                    }
                }
            );
        
        AsyncDataStream
            .unorderedWait(
                beanStream,
                new DimAsyncFunction<TradeProvinceOrderWindow>() {
                    @Override
                    protected String getTable() {
                        return "dim_base_province";
                    }
                    
                    @Override
                    protected String getId(TradeProvinceOrderWindow input) {
                        return input.getProvinceId();
                    }
                    
                    @Override
                    protected void addDim(TradeProvinceOrderWindow input, JSONObject dim) {
                        input.setProvinceName(dim.getString("NAME"));
                    }
                },
                60,
                TimeUnit.SECONDS
            )
            .addSink(FlinkSinkUtil.getClickHouseSink("dws_trade_province_order_window", TradeProvinceOrderWindow.class));
    }
}
