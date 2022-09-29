package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.BaseAppV1;
import com.atguigu.gmall.realtime.bean.CartAddUuBean;
import com.atguigu.gmall.realtime.common.Constant;
import com.atguigu.gmall.realtime.util.AtguiguUtil;
import com.atguigu.gmall.realtime.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Author lzc
 * @Date 2022/9/29 10:27
 */
public class Dws_06_DwsTradeCartAddUuWindow extends BaseAppV1 {
    public static void main(String[] args) {
        new Dws_06_DwsTradeCartAddUuWindow().init(
            4006,
            2,
            "Dws_06_DwsTradeCartAddUuWindow",
            Constant.TOPIC_DWD_TRADE_CART_ADD
        );
    }
    
    @Override
    protected void handle(StreamExecutionEnvironment env,
                          DataStreamSource<String> stream) {
        stream
            .map(JSON::parseObject)
            .keyBy(obj -> obj.getString("user_id"))
            .process(new KeyedProcessFunction<String, JSONObject, CartAddUuBean>() {
                
                private ValueState<String> lastCartAddDateState;
                
                @Override
                public void open(Configuration parameters) throws Exception {
                    lastCartAddDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("lastCartAddDateState", String.class));
                }
                
                @Override
                public void processElement(JSONObject value,
                                           Context ctx,
                                           Collector<CartAddUuBean> out) throws Exception {
                    String lastCartAddDate = lastCartAddDateState.value();
                    
                    // 把时间戳转成年月日 注意的 ts 是一个秒
                    long ts = value.getLong("ts") * 1000;
                    String today = AtguiguUtil.toDate(ts);
                    
                    if (!today.equals(lastCartAddDate)) {
                        lastCartAddDateState.update(today);
                        
                        out.collect(new CartAddUuBean("", "", 1L, ts));
                    }
                    
                }
            })
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<CartAddUuBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner((bean, ts) -> bean.getTs())
            )
            .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
            .reduce(
                new ReduceFunction<CartAddUuBean>() {
                    @Override
                    public CartAddUuBean reduce(CartAddUuBean value1, CartAddUuBean value2) throws Exception {
                        value1.setCartAddUuCt(value1.getCartAddUuCt() + value2.getCartAddUuCt());
                        return value1;
                    }
                },
                new ProcessAllWindowFunction<CartAddUuBean, CartAddUuBean, TimeWindow>() {
                    @Override
                    public void process(Context ctx,
                                        Iterable<CartAddUuBean> elements,
                                        Collector<CartAddUuBean> out) throws Exception {
                        CartAddUuBean bean = elements.iterator().next();
                        
                        bean.setStt(AtguiguUtil.toDateTime(ctx.window().getStart()));
                        bean.setEdt(AtguiguUtil.toDateTime(ctx.window().getEnd()));
                        
                        bean.setTs(System.currentTimeMillis());
                        
                        out.collect(bean);
                    }
                }
            )
            .addSink(FlinkSinkUtil.getClickHouseSink("dws_trade_cart_add_uu_window", CartAddUuBean.class));
    }
}
