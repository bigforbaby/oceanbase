package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.BaseAppV1;
import com.atguigu.gmall.realtime.bean.UserLoginBean;
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
 * @Date 2022/9/29 09:04
 */
public class Dws_04_DwsUserUserLoginWindow extends BaseAppV1 {
    public static void main(String[] args) {
        new Dws_04_DwsUserUserLoginWindow().init(
            4004,
            2,
            "Dws_04_DwsUserUserLoginWindow",
            Constant.TOPIC_DWD_TRAFFIC_PAGE
        );
    }
    
    @Override
    protected void handle(StreamExecutionEnvironment env,
                          DataStreamSource<String> stream) {
        // 1. 找到登录记录
        SingleOutputStreamOperator<JSONObject> loginDataStream = findLoginPage(stream);
        // 2. 把 stream 中的数据解析成一个 pojo 类型 UserLoginBean
        SingleOutputStreamOperator<UserLoginBean> beanStream = parseToPojo(loginDataStream);
        // 2. 开窗聚合
        SingleOutputStreamOperator<UserLoginBean> resultStream = windowAndAgg(beanStream);
    
        // 3. 写出 clickhouse 中
        writeToClickHouse(resultStream);
        
    }
    
    private void writeToClickHouse(SingleOutputStreamOperator<UserLoginBean> resultStream) {
        resultStream.addSink(FlinkSinkUtil.getClickHouseSink("dws_user_user_login_window", UserLoginBean.class));
    }
    
    private SingleOutputStreamOperator<UserLoginBean> windowAndAgg(SingleOutputStreamOperator<UserLoginBean> beanStream) {
       return beanStream
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<UserLoginBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner((bean, ts) -> bean.getTs())
            )
            .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
            .reduce(
                new ReduceFunction<UserLoginBean>() {
                    @Override
                    public UserLoginBean reduce(UserLoginBean b1,
                                                UserLoginBean b2) throws Exception {
                        b1.setUuCt(b2.getUuCt() + b1.getUuCt());
                        b1.setBackCt(b2.getBackCt() + b1.getBackCt());
                        return b1;
                    }
                },
                new ProcessAllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>() {
                    @Override
                    public void process(Context ctx,
                                        Iterable<UserLoginBean> elements,
                                        Collector<UserLoginBean> out) throws Exception {
                        UserLoginBean bean = elements.iterator().next();
                        
                        bean.setStt(AtguiguUtil.toDateTime(ctx.window().getStart()));
                        bean.setEdt(AtguiguUtil.toDateTime(ctx.window().getEnd()));
    
                        bean.setTs(System.currentTimeMillis());
                        
                        out.collect(bean);
                    }
                }
            );
    }
    
    private SingleOutputStreamOperator<UserLoginBean> parseToPojo(
        SingleOutputStreamOperator<JSONObject> loginDataStream) {
        // 找到每个用户的第一第一次 登陆记录, 封装到 bean
        return loginDataStream
            .keyBy(obj -> obj.getJSONObject("common").getString("uid"))
            .process(new KeyedProcessFunction<String, JSONObject, UserLoginBean>() {
                
                private ValueState<String> lastLoginDateState;
                
                @Override
                public void open(Configuration parameters) throws Exception {
                    // 记录上一次登陆的日期
                    lastLoginDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("lastLoginDateState", String.class));
                }
                
                @Override
                public void processElement(JSONObject value,
                                           Context ctx,
                                           Collector<UserLoginBean> out) throws Exception {
                    /*
                    找出这个用户的当日首次登录记录
                        
                        在首次记录中再看是否是 7 日回流
                     */
                    String lastLoginDate = lastLoginDateState.value();
                    Long ts = value.getLong("ts");
                    String today = AtguiguUtil.toDate(ts);
                    
                    Long uuCt = 0L;
                    Long backCt = 0L;
                    // 如何和今天不一样, 就是今天第一次登录
                    if (!today.equals(lastLoginDate)) {
                        // 今天第一登陆
                        lastLoginDateState.update(today);
                        uuCt = 1L;
                        // 当天第一次登录, 需要判断下是否为 7 日回流用户
                        if (lastLoginDate != null) { // 今天第一次登录, lastLoginDate不会 null 证明前面登录过
                            long delta = (ts - AtguiguUtil.dateToTs(lastLoginDate)) / 1000 / 60 / 60 / 24;
                            if (delta > 7) {
                                backCt = 1L;
                            }
                        }
                        
                    }
                    
                    if (uuCt == 1) {
                        out.collect(new UserLoginBean("", "",
                                                      backCt, uuCt,
                                                      ts
                        ));
                    }
                    
                    
                }
            });
    }
    
    private SingleOutputStreamOperator<JSONObject> findLoginPage(DataStreamSource<String> stream) {
        return stream
            .map(JSON::parseObject)
            .filter(obj -> {
                String uid = obj.getJSONObject("common").getString("uid");
                String lastPageId = obj.getJSONObject("page").getString("last_page_id");
                
                return (lastPageId == null || "login".equals(lastPageId)) && uid != null;
            });
    }
}
/*
1. 启动 yarn session 起不来

	确认 yarn 是否起来
	date node 0 个可用

	hdfs 没起来

2. flink job 打包到 linux 的出错
	依赖冲突
	kakfa 的依赖冲突

	打包的时候把 kakfa 连接器打到 jar

	lib 目录也曾经下载过一个 kafka 的连接器


-------------------------
统计七日回流用户和当日独立用户数。

1.数据源
	页面日志
 2. 指标 1:
 	当日独立用户
 		和前面 uv 的计算完全一样

    指标 2:
    	七日回流用户

    	首先: 今天这个用户第一登陆的记录
    	然后: 再判断他上次登陆是在 7 天前

   登录用户:
   	1. 自动登陆
   		发生在会话的首页
   			last_page_id = null
   			uid != null
    2. 中途跳转登陆
    	 先跳转到登录页面:这个页面不要

    	 在登陆页面输入用户信息, 点击登录, 进入下一个页面: 登录成功后的
    	 	需要这个这个页面
    	 		last_page_id="login"
    	 		uid != null

 3. 封装到一个 bean











 */