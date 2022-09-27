package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.BaseAppV1;
import com.atguigu.gmall.realtime.bean.TrafficHomeDetailPageViewBean;
import com.atguigu.gmall.realtime.common.Constant;
import com.atguigu.gmall.realtime.util.AtguiguUtil;
import com.atguigu.gmall.realtime.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
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
 * @Date 2022/9/27 14:39
 */
public class Dws_03_DwsTrafficPageViewWindow extends BaseAppV1 {
    public static void main(String[] args) {
        new Dws_03_DwsTrafficPageViewWindow().init(
            4003,
            2,
            "Dws_03_DwsTrafficPageViewWindow",
            Constant.TOPIC_DWD_TRAFFIC_PAGE
        );
    }
    
    @Override
    protected void handle(StreamExecutionEnvironment env,
                          DataStreamSource<String> stream) {
        // 1. 过滤出首页和详情页的记录
        SingleOutputStreamOperator<JSONObject> homeAndDetailStream = filterHomeAndDetail(stream);
        //        homeAndDetailStream.print("abc");
        // 2. 封装到 bean 中
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> beanStream = parseToBean(homeAndDetailStream);
        // 3.开窗聚和
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> resultStream = windowAndAgg(beanStream);
        
        // 4. 写出到 clickhouse 中
        writeToClickHouse(resultStream);
    }
    
    private void writeToClickHouse(SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> resultStream) {
        resultStream.addSink(FlinkSinkUtil.getClickHouseSink("dws_traffic_page_view_window", TrafficHomeDetailPageViewBean.class));
    }
    
    private SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> windowAndAgg(SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> beanStream) {
      return  beanStream
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<TrafficHomeDetailPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner((bean, ts) -> bean.getTs())
            )
            .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
            .reduce(new ReduceFunction<TrafficHomeDetailPageViewBean>() {
                        @Override
                        public TrafficHomeDetailPageViewBean reduce(TrafficHomeDetailPageViewBean bean1,
                                                                    TrafficHomeDetailPageViewBean bean2) throws Exception {
                            bean1.setHomeUvCt(bean1.getHomeUvCt() + bean2.getHomeUvCt());
                            bean1.setGoodDetailUvCt(bean1.getGoodDetailUvCt() + bean2.getGoodDetailUvCt());
                            return bean1;
                        }
                    },
                    new ProcessAllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>() {
                        @Override
                        public void process(Context ctx,
                                            Iterable<TrafficHomeDetailPageViewBean> elements,
                                            Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                            TrafficHomeDetailPageViewBean bean = elements.iterator().next();
                            bean.setStt(AtguiguUtil.toDateTime(ctx.window().getStart()));
                            bean.setEdt(AtguiguUtil.toDateTime(ctx.window().getEnd()));
                            
                            bean.setTs(System.currentTimeMillis());
                            out.collect(bean);
                        }
                    }
            );
    }
    
    private SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> parseToBean(
        SingleOutputStreamOperator<JSONObject> stream) {
        return stream
            .keyBy(obj -> obj.getJSONObject("common").getString("uid"))
            .process(new KeyedProcessFunction<String, JSONObject, TrafficHomeDetailPageViewBean>() {
                
                private ValueState<String> goodDetailState;
                private ValueState<String> homeState;
                
                @Override
                public void open(Configuration parameters) throws Exception {
                    homeState = getRuntimeContext().getState(new ValueStateDescriptor<String>("homeState", String.class));
                    goodDetailState = getRuntimeContext().getState(new ValueStateDescriptor<String>("goodDetailState", String.class));
                }
                
                @Override
                public void processElement(JSONObject obj,
                                           Context ctx,
                                           Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                    String pageId = obj.getJSONObject("page").getString("page_id");
                    
                    Long homeUvCt = 0L;
                    Long goodDetailUvCt = 0L;
                    
                    Long ts = obj.getLong("ts");
                    String today = AtguiguUtil.toDate(ts);
                    // 如何使 home 页面, 然后再判断是否当天的第一个 home 页面
                    if ("home".equals(pageId) && !today.equals(homeState.value())) {
                        System.out.println("home");
                        // 当天的第一个 home 来了, 更新状态
                        homeState.update(today);
                        homeUvCt = 1L;
                    }
                    
                    if ("good_detail".equals(pageId) && !today.equals(goodDetailState.value())) { //
                        System.out.println("detail");
                        // 当天的第一个 详情 来了, 更新状态
                        goodDetailState.update(today);
                        goodDetailUvCt = 1L;
                    }
                    if (homeUvCt + goodDetailUvCt == 1) {
                        out.collect(new TrafficHomeDetailPageViewBean(
                            "", "",
                            homeUvCt, goodDetailUvCt,
                            ts
                        ));
                    }
                    
                    
                }
            });
    }
    
    private SingleOutputStreamOperator<JSONObject> filterHomeAndDetail(DataStreamSource<String> stream) {
        return stream
            .map(JSON::parseObject)
            .filter(obj -> {
                String pageId = obj.getJSONObject("page").getString("page_id");
                return "home".equals(pageId) || "good_detail".equals(pageId);
            });
    }
}
/*
独立访客数

	首页的独立访客数
		统计有多个不同的用户访问过首页



	详情页的独立访客数
		统计有多个不同的用户访问过详情
  
 0. 数据源: 页面日志

1. 过滤出首页和详情页

2. 封装一个 bean

homeCt   detailCt
1           0
0           1

0           0

分别两个状态记录每个用户访问首页和详情

3. 开窗聚合

4. 写出到 clickhouse 中
 */