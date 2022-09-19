package com.atguigu.gmall.realtime.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.BaseAppV1;
import com.atguigu.gmall.realtime.common.Constant;
import com.atguigu.gmall.realtime.util.AtguiguUtil;
import com.atguigu.gmall.realtime.util.FlinkSinkUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author lzc
 * @Date 2022/9/19 8:53
 */
public class Dwd_01_BaseLogApp extends BaseAppV1 {
    private final String PAGE = "page";
    private final String DISPLAY = "display";
    private final String ERR = "err";
    private final String ACTION = "action";
    private final String START = "start";
    public static void main(String[] args) {
        new Dwd_01_BaseLogApp().init(
            3001,
            2,
            "Dwd_01_BaseLogApp",
            Constant.TOPIC_ODS_LOG
        );
    }
    
    @Override
    protected void handle(StreamExecutionEnvironment env,
                          DataStreamSource<String> stream) {
        // 1. 对数据做清洗
        SingleOutputStreamOperator<JSONObject> etledStream = etl(stream);
        // 2. 纠正新老客户
        SingleOutputStreamOperator<JSONObject> validatedStream = validateNewOrOld(etledStream);
        // 3. 分流
        Map<String, DataStream<JSONObject>> streams = splitStream(validatedStream);
        // 4. 不同的流的数据写入到不同的topic中
        writeToKafka(streams);
        
    }
    
    private void writeToKafka(Map<String, DataStream<JSONObject>> streams) {
        streams.get(START)
            .map(JSONAware::toJSONString)
            .addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_START));
        
        streams.get(DISPLAY)
            .map(JSONAware::toJSONString)
            .addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_DISPLAY));
        
        streams.get(ACTION)
            .map(JSONAware::toJSONString)
            .addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ACTION));
        
        streams.get(ERR)
            .map(JSONAware::toJSONString)
            .addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ERR));
        
        streams.get(PAGE)
            .map(JSONAware::toJSONString)
            .addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_PAGE));
        
        
    }
    
    private Map<String, DataStream<JSONObject>> splitStream(SingleOutputStreamOperator<JSONObject> validatedStream) {
        /*
        一共5种日志, 分5个流
        侧输出流
        
        启动   主流
        曝光    侧输出流
        活动   侧输出流
        页面   侧输出流
        错误   侧输出流
         */
        OutputTag<JSONObject> displayTag = new OutputTag<JSONObject>("display") {};
        OutputTag<JSONObject> actionTag = new OutputTag<JSONObject>("action") {};
        OutputTag<JSONObject> errTag = new OutputTag<JSONObject>("err") {};
        OutputTag<JSONObject> pageTag = new OutputTag<JSONObject>("page") {};
        
        SingleOutputStreamOperator<JSONObject> startStream = validatedStream
            .process(new ProcessFunction<JSONObject, JSONObject>() {
                @Override
                public void processElement(JSONObject obj,
                                           Context ctx,
                                           Collector<JSONObject> out) throws Exception {
                    JSONObject common = obj.getJSONObject("common");
                    JSONObject page = obj.getJSONObject("page");
                    Long ts = obj.getLong("ts");
                    
                    // 1. 先判断时候为启动日志
                    JSONObject start = obj.getJSONObject("start");
                    if (start != null) {
                        // 启动日志
                        out.collect(obj);
                    } else {
                        // 其他日志和启动日志是互斥
                        // 曝光 活动 err page
                        // 2. 曝光
                        JSONArray displays = obj.getJSONArray("displays");
                        if (displays != null) {
                            //一条数据中, 有可能多个曝光, 最好一个曝光一条数据
                            for (int i = 0; i < displays.size(); i++) {
                                JSONObject display = displays.getJSONObject(i);
                                display.putAll(common); // common中的的kv直接添加到了display中
                                display.putAll(page);
                                display.put("ts", ts);
                                ctx.output(displayTag, display);
                            }
                            // 但display的数据用完之后, 删除. 后面用不到
                            obj.remove("displays");
                        }
                        // 3. 活动
                        JSONArray actions = obj.getJSONArray("actions");
                        if (actions != null) {
                            for (int i = 0; i < actions.size(); i++) {
                                JSONObject action = actions.getJSONObject(i);
                                action.putAll(common); // common中的的kv直接添加到了action中
                                action.putAll(page);
                                // action有自己的ts, 所以不需要外面的ts
                                //action.put("ts", ts);
                                ctx.output(actionTag, action);
                            }
                            // 但action的数据用完之后, 删除. 后面用不到
                            obj.remove("actions");
                        }
                        
                        // 3. 错误
                        JSONObject err = obj.getJSONObject("err");
                        if (err != null) {
                            ctx.output(errTag, obj);
                            obj.remove("err");
                        }
                        // 4. page
                        ctx.output(pageTag, obj);
                    }
                    
                    
                }
            });
        DataStream<JSONObject> displayStream = startStream.getSideOutput(displayTag);
        DataStream<JSONObject> actionStream = startStream.getSideOutput(actionTag);
        DataStream<JSONObject> errStream = startStream.getSideOutput(errTag);
        DataStream<JSONObject> pageStream = startStream.getSideOutput(pageTag);
        
        // 返回什么?
        Map<String, DataStream<JSONObject>> streams = new HashMap<String, DataStream<JSONObject>>();
        streams.put(PAGE, pageStream);
        streams.put(ACTION, actionStream);
        streams.put(DISPLAY, displayStream);
        streams.put(ERR, errStream);
        streams.put(START, startStream);
        return streams;
    
    }
    
    private SingleOutputStreamOperator<JSONObject> validateNewOrOld(
        SingleOutputStreamOperator<JSONObject> etledStream) {
        return etledStream
            .keyBy(obj -> obj.getJSONObject("common").getString("mid"))
            .map(new RichMapFunction<JSONObject, JSONObject>() {
                
                private ValueState<String> firstVisitDateState;
                
                @Override
                public void open(Configuration parameters) throws Exception {
                    firstVisitDateState = getRuntimeContext()
                        .getState(new ValueStateDescriptor<String>("firstVisitDate", String.class));
                }
                
                @Override
                public JSONObject map(JSONObject obj) throws Exception {
                    JSONObject common = obj.getJSONObject("common");
                    String isNew = common.getString("is_new");
                    Long ts = obj.getLong("ts");
                    String today = AtguiguUtil.toDate(ts);
                    // 新老客户纠正
                    // 1. 首次访问日期
                    String firstVisitDate = firstVisitDateState.value();
                    if (firstVisitDate == null) {
                        if ("1".equals(isNew)) {
                            // 这个用户没有状态, 并且标记还是行用户, 这这个用户应该是第一天的第一次访问
                            // 状态中就存储今天. 更新hi下状态
                            firstVisitDateState.update(today);
                        } else {
                            // 用户是老用户, 但是没有状态. 为了防止以后有问题, 把状态更新成昨天
                            String yesterday = AtguiguUtil.toDate(ts - 24 * 60 * 60 * 1000);
                            firstVisitDateState.update(yesterday);
                        }
                    } else {
                        if ("1".equals(isNew) && !today.equals(firstVisitDate)) {
                            // 状态不为空, 并且今天还和状态不一样, 需要对is_new纠正
                            common.put("is_new", "0");
                        }
                    }
                    
                    return obj;
                }
            });
    }
    
    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        return stream
            .filter(new FilterFunction<String>() {
                @Override
                public boolean filter(String s) throws Exception {
                    try {
                        JSON.parseObject(s);
                    } catch (Exception e) {
                        System.out.println("你的数据格式不是一个有效的json格式: " + s);
                        // 如果有异常,表示json格式不对, 方法false, 数据不要了
                        return false;
                    }
                    // 去掉空白字符之后, 判断长度应该大于0
                    return s.trim().length() > 0;
                }
            })
            .map(JSON::parseObject);
        
    }
}
/*
所有的日志在一个topic: ods_log

所有的业务在一个topic: ods_db


dwd就是分流:
	日志一个5个日志, 分5个流, 分别写入到不同的topic
		手工的方式
	
	业务数据, 一张表分一个流, 一张表写入到一个topic中
		处理比较复杂, 我们单个拿出来分
		
		对比较简单处理的, 是使用的动态分流.(和维度的分流几乎一样)
		
新老客户标记的纠正:

	由于用户会清缓存, 或者重装, 导致缓存丢失.

	那么新老客户标记就会混乱.

	可能出的问题: 有可能会老客户标记为新客户.

	会不会把新客户误标记为老客户? 不会
	
flink端如何纠正这个标记?
	状态
	
	针对每个用户  状态存储这个用户的首次访问年月日
	
	每来一条数据, 就判断状态中的日期和这条数据的日期的对吧
	
	如果状态= null
		is_new = 1
			确实是行客户, 然后状态存储今天的日期
		is_new = 0
			证明这个用户肯定是老客户, 状态为null的原因flink程序启动晚的原因
			要不要状态做一些处理?
				最好对状态做处理. 把状态存入昨天的日期
	
	如果状态 != null
	    is_new = 1
			判断今天和状态是否相等,
				如果相等就确实是新客户, 不用操作
				如果不等, is_new需要纠正, is_new=0
		
		is_new= 0
			理论上不需要做任何的操作
			
			
 */